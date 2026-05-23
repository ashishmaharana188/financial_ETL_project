import streamlit as st
import pandas as pd
import json
from datetime import datetime
from sqlalchemy import text
from scripts.database import engine


def fetch_single_ticker_ledger(ticker: str, target_date: str) -> pd.DataFrame:
    query = text("""
        SELECT horizon, signal, score, confidence, veto_flag, penalty, reason_json, feature_json
        FROM prediction_ledger
        WHERE ticker = :ticker AND asof_date = :target_date AND engine_name = '1_OLS_Microstructure'
    """)
    with engine.connect() as conn:
        return pd.read_sql(
            query, conn, params={"ticker": ticker, "target_date": target_date}
        )


def fetch_watchlist_ledger(target_date: str) -> pd.DataFrame:
    query = text("""
        SELECT ticker, horizon, signal, score, confidence, veto_flag, penalty
        FROM prediction_ledger
        WHERE asof_date = :target_date AND engine_name = '1_OLS_Microstructure'
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"target_date": target_date})


def fetch_historical_accuracy(ticker: str) -> dict:
    """Queries Phase 3 validation ledger to extract aggregate realized accuracy metrics."""
    query = text("""
        SELECT is_directional_hit, variance_error
        FROM validation_ledger
        WHERE ticker = :ticker AND engine_name = '1_OLS_Microstructure'
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"ticker": ticker})

    if df.empty:
        return {"hit_rate": "N/A", "avg_error": "N/A", "total_audits": 0}

    hits = df["is_directional_hit"].sum()
    total = len(df)
    avg_error = df["variance_error"].mean()

    return {
        "hit_rate": f"{round((hits / total) * 100, 1)}%",
        "avg_error": f"{round(avg_error * 100, 2)}%",
        "total_audits": total,
    }


def fetch_trend_matrix(
    ticker: str, target_date: str, days_lookback: int = 30
) -> pd.DataFrame:
    """Fetches and aligns price, volume, and delivery streams into an unjumbled time-series."""
    query = text("""
        SELECT date, close, volume, delivery_percentage
        FROM unified_market_matrix
        WHERE ticker = :ticker AND date <= :target_date
        ORDER BY date DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "ticker": ticker,
                "target_date": target_date,
                "limit": days_lookback,
            },
        )
    if not df.empty:
        df = df.sort_values(by="date")
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df.set_index("date", inplace=True)
    return df


def render_prediction_trajectory_chart(
    ticker: str, prediction_date: str, horizon_label: str, expected_return: float
):
    """
    Fetches actual price action following a prediction date and overlays
    the model's predicted trajectory line for visual hit/miss validation.
    """
    horizon_days_map = {"2D": 2, "5D": 5, "20D": 20}
    target_days = horizon_days_map.get(horizon_label, 5)

    # Fetch the actual price path from the prediction date forward + a few buffer days to see the result
    query = text("""
        SELECT date, close
        FROM unified_market_matrix
        WHERE ticker = :ticker AND date >= :start_date
        ORDER BY date ASC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        actual_df = pd.read_sql(
            query,
            conn,
            params={
                "ticker": ticker,
                "start_date": prediction_date,
                "limit": target_days + 3,
            },
        )

    if actual_df.empty or len(actual_df) < 2:
        st.caption(f"Waiting for market data to validate {prediction_date} signal...")
        return

    # Extract the starting price (T+0)
    p0 = actual_df.iloc[0]["close"]
    target_price = p0 * (1 + expected_return)

    # Format dates and create the visualization dataframe
    actual_df["date"] = pd.to_datetime(actual_df["date"]).dt.strftime("%Y-%m-%d")
    actual_df.set_index("date", inplace=True)
    actual_df.rename(columns={"close": "Actual Price Path"}, inplace=True)

    # Create the Predicted Trajectory Line (Straight line from P0 to Target Price)
    # We map this across the exact length of the horizon
    predicted_path = [None] * len(actual_df)
    predicted_path[0] = p0  # Start at the exact same point

    # Map the target price to the exact day the horizon matures
    target_index = min(target_days, len(actual_df) - 1)
    predicted_path[target_index] = target_price

    actual_df["Model Predicted Path"] = predicted_path

    # Interpolate the line visually so it draws a clean slope
    actual_df["Model Predicted Path"] = actual_df["Model Predicted Path"].interpolate()

    st.markdown("###### 🎯 Expected vs. Actual Validation Tracker")
    st.line_chart(
        actual_df[["Actual Price Path", "Model Predicted Path"]],
        color=["#FFFFFF", "#FF4B4B"],
    )


def render_ols_engine_ui(selected_ticker: str):
    st.markdown(f"### 📊 Engine Terminal Interface")

    # 1. Scope Initialization
    t_date = st.sidebar.date_input(
        "Analysis Target Date", datetime.strptime("2026-05-22", "%Y-%m-%d")
    )
    target_date = str(t_date)

    # 2. Fetch Data
    ticker_df = fetch_single_ticker_ledger(selected_ticker, target_date)

    # 3. Macro Ribbon
    with st.container():
        if not ticker_df.empty:
            # Type-safe parsing for JSON columns
            raw_reason = ticker_df.iloc[0]["reason_json"]
            reasoning = (
                json.loads(raw_reason) if isinstance(raw_reason, str) else raw_reason
            )

            regime = reasoning.get("systemic_regime_context", "Neutral")
            color = (
                "green"
                if regime == "Risk-On"
                else "red" if regime == "Risk-Off" else "gray"
            )
            st.markdown(
                f"**Market Regime:** :{color}[{regime}] | **Ticker:** {selected_ticker}"
            )

    # 4. Forensic Visualizer
    trend_df = fetch_trend_matrix(selected_ticker, target_date)
    if not trend_df.empty:
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown("#### Price & Institutional Accumulation")
            st.line_chart(trend_df[["close"]], height=300, use_container_width=True)
        with c2:
            st.markdown("#### Delivery Flow")
            st.bar_chart(
                trend_df["delivery_percentage"], height=300, use_container_width=True
            )

    # 5. Decision Matrix
    st.markdown("---")
    st.markdown("#### Decision Matrix & Audit Validation")

    tabs = st.tabs(["2-Day Signal", "5-Day Signal", "20-Day Signal"])

    for tab, h_label in zip(tabs, ["2D", "5D", "20D"]):
        with tab:
            row = (
                ticker_df[ticker_df["horizon"] == h_label].iloc[0]
                if not ticker_df[ticker_df["horizon"] == h_label].empty
                else None
            )

            if row is None:
                st.info("No data for this horizon.")
                continue

            # Robust Type Handling for features/reasoning
            raw_features = row["feature_json"]
            features = (
                json.loads(raw_features)
                if isinstance(raw_features, str)
                else raw_features
            )

            raw_reason = row["reason_json"]
            reasoning = (
                json.loads(raw_reason) if isinstance(raw_reason, str) else raw_reason
            )

            audit_stats = fetch_historical_accuracy(selected_ticker)

            # KPI Grid
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Signal", row["signal"])
            k2.metric("Target Return", f"{round(row['score']*100, 2)}%")
            k3.metric("Conviction", f"{round(row['confidence']*100, 1)}%")
            k4.metric("Audited Hit Rate", audit_stats["hit_rate"])

            # Validation Callout
            if audit_stats["total_audits"] > 0:
                st.caption(
                    f"Audit: Based on {audit_stats['total_audits']} predictions. "
                    f"Drift: {audit_stats['avg_error']}. "
                    f"Status: {'Model Unbiased' if abs(float(audit_stats['avg_error'].replace('%',''))) < 1 else 'Systemic Bias'}"
                )

            # Forensic Metrics
            col1, col2 = st.columns(2)
            with col1:
                st.info(
                    f"**Structural Footprints**\n\n"
                    f"• Delivery: {round(features.get('delivery_percentage', 0), 2)}%\n"
                    f"• PCR Change: {round(features.get('pcr_change', 0), 4)}\n"
                    f"• Futures Basis: {round(features.get('futures_basis', 0), 4)}"
                )
            with col2:
                st.warning(
                    f"**Internal Engine State**\n\n"
                    f"• Volume Consistency: `{features.get('intraday_volume_consistency', 0)}`\n"
                    f"• Macro Context: `{reasoning.get('systemic_regime_context', 'Neutral')}`\n"
                    f"• R-Squared: `{reasoning.get('model_r_squared', 0)}`"
                )
