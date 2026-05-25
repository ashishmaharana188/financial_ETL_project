import pandas as pd
import numpy as np
import statsmodels.api as sm
import json
from datetime import datetime
from sqlalchemy import insert, text
from scripts.database import engine, prediction_ledger
from sqlalchemy.dialects.postgresql import insert


class OLSMicrostructureEngine:
    def __init__(self, engine_name="1_OLS_Microstructure"):
        self.engine_name = engine_name

    def fetch_historical_matrix(self, ticker: str, asof_date: str) -> pd.DataFrame:
        """
        Fetches historical daily matrix containing price, volume, and lagged bulk metrics
        (delivery, open interest, options PCR, futures basis) up to the asof_date.
        """
        query = text("""
            SELECT date, close, volume, delivery_percentage, oi_pcr, futures_basis
            FROM unified_market_matrix
            WHERE ticker = :ticker AND date <= :asof_date
            ORDER BY date ASC
            LIMIT 500
        """)
        with engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"ticker": ticker, "asof_date": asof_date}
            )

        numeric_cols = [
            "close",
            "volume",
            "delivery_percentage",
            "oi_pcr",
            "futures_basis",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
            df["pcr_change"] = df["oi_pcr"].diff()
            df["vol_change"] = df["volume"].pct_change()
        return df

    def fetch_live_intraday_energy(self, ticker: str, asof_date: str) -> float:
        query = text("""
            SELECT "ReportDate", "Volume"
            FROM global_assets_intraday
            WHERE "Ticker" = :ticker AND DATE("ReportDate") = :asof_date
            ORDER BY "ReportDate" ASC
        """)
        with engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"ticker": ticker, "asof_date": asof_date}
            )

        # Holiday/Non-trading day mitigation check
        if df.empty or len(df) < 5:
            return (
                -1.0
            )  # Sentinel value signaling an inactive or unrecorded trading day

        volumes = df["Volume"].dropna().values
        if len(volumes) == 0 or np.std(volumes) == 0:
            return 0.0

        consistency_index = float(np.mean(volumes) / np.std(volumes))
        return round(consistency_index, 4)

    def calculate_macro_regime(self, asof_date: str) -> dict:
        """
        Engine 1.5 Core Logic: Queries the macro daily ledger to calculate rolling
        Z-scores for global volatility index anchors (VIX) and domestic market breadth
        to evaluate systemic macro health boundaries.
        """
        # 1. Fetch rolling historical baseline for Volatility tracking (252 session window)
        vix_query = text("""
            SELECT "ReportDate", "Close_Value"
            FROM macro_daily_ledger
            WHERE "IndicatorName" IN ('US_VIX', 'India_VIX') AND "ReportDate" <= :asof_date
            ORDER BY "ReportDate" DESC
            LIMIT 252
        """)

        # 2. Fetch market breadth components for the current point-in-time snapshot
        breadth_query = text("""
            SELECT "Ticker", "Close", "Open"
            FROM global_assets_daily
            WHERE "ReportDate" = :asof_date
        """)

        with engine.connect() as conn:
            vix_df = pd.read_sql(vix_query, conn, params={"asof_date": asof_date})
            breadth_df = pd.read_sql(
                breadth_query, conn, params={"asof_date": asof_date}
            )

        # Default fallback conditions if staging tables lack depth
        regime = "Neutral"
        vix_z = 0.0
        breadth_ratio = 0.5

        if not vix_df.empty and len(vix_df) > 30:
            latest_vix = vix_df["Close_Value"].iloc[0]
            historical_vix = vix_df["Close_Value"].values
            vix_mean = np.mean(historical_vix)
            vix_std = np.std(historical_vix)
            if vix_std > 0:
                vix_z = float((latest_vix - vix_mean) / vix_std)

        if not breadth_df.empty:
            advancers = len(breadth_df[breadth_df["Close"] > breadth_df["Open"]])
            total_active = len(breadth_df)
            if total_active > 0:
                breadth_ratio = float(advancers / total_active)

        # Operational decision matrix for classification boundaries
        if vix_z > 1.2 or breadth_ratio < 0.35:
            regime = "Risk-Off"
        elif vix_z < -1.0 and breadth_ratio > 0.65:
            regime = "Risk-On"

        return {
            "regime": regime,
            "vix_z_score": round(vix_z, 4),
            "market_breadth_ratio": round(breadth_ratio, 4),
        }

    def fit_and_predict(
        self,
        df: pd.DataFrame,
        intraday_energy: float,
        macro_state: dict,
        horizon_days: int,
    ) -> dict:
        """
        Fits an OLS model using microstructure variables and intraday cash energy,
        then subjects conviction output scores to Engine 1.5 global modifier rules.
        """
        df_clean = df.dropna().copy()
        if len(df_clean) < 30:
            return None

        df_clean[f"fwd_return_{horizon_days}d"] = (
            df_clean["close"].shift(-horizon_days) / df_clean["close"] - 1
        )

        features = ["delivery_percentage", "pcr_change", "vol_change", "futures_basis"]
        train_df = df_clean.dropna(subset=[f"fwd_return_{horizon_days}d"] + features)

        if len(train_df) < 30:
            return None

        X = sm.add_constant(train_df[features])
        y = train_df[f"fwd_return_{horizon_days}d"]

        model = sm.OLS(y, X).fit()

        current_features = df_clean[features].iloc[-1].to_dict()
        current_features["intraday_volume_consistency"] = intraday_energy
        current_features["macro_vix_z"] = macro_state["vix_z_score"]
        current_features["market_breadth"] = macro_state["market_breadth_ratio"]

        X_pred = [1.0] + [df_clean[f].iloc[-1] for f in features]
        expected_return = float(model.predict(X_pred)[0])

        # Interdependency calculations: Scale confidence using microstructure fit, adjust for macro context
        base_confidence = min(max(float(model.rsquared * 2.5), 0.1), 0.95)
        confidence_modifier = 0.0
        veto_flag = False
        penalty = 0.0

        reasoning = {
            "top_positive_driver": str(
                model.tvalues.idxmax() if model.tvalues.max() > 0 else "None"
            ),
            "top_negative_driver": str(
                model.tvalues.idxmin() if model.tvalues.min() < 0 else "None"
            ),
            "model_r_squared": round(float(model.rsquared), 4),
            "intraday_signal_status": (
                "ALGO_ACCUMULATION" if intraday_energy > 1.5 else "RETAIL_NOISE"
            ),
            "systemic_regime_context": macro_state["regime"],
        }

        # Engine 1.5 Interdependency Interconnection Rule Application
        if macro_state["regime"] == "Risk-Off":
            penalty = 0.25
            confidence_modifier = -0.20
            reasoning["macro_modifier_note"] = (
                "Macro Veto Applied: High Global Volatility or Toxic Market Breadth Detected"
            )
            if expected_return > 0.015:
                veto_flag = True  # Flag structural macro contradiction
        elif macro_state["regime"] == "Risk-On":
            confidence_modifier = 0.10
            reasoning["macro_modifier_note"] = (
                "Macro Tailwind Applied: Tailwinds Confirmed via Systemic Risk-On Conditions"
            )

        final_confidence = min(max(base_confidence + confidence_modifier, 0.05), 0.95)

        return {
            "expected_return": expected_return,
            "confidence": final_confidence,
            "penalty": penalty,
            "veto_flag": veto_flag,
            "features": current_features,
            "reasoning": reasoning,
        }

    def generate_signal(
        self,
        expected_return: float,
        confidence: float,
        intraday_energy: float,
        veto_flag: bool,
    ) -> str:
        """Applies final signal classification boundaries, checking macro veto exceptions."""
        if veto_flag:
            return "WATCH"  # Force downgrade away from aggressive BUY triggers during systematic crises

        if expected_return > 0.015 and confidence > 0.35 and intraday_energy > 1.2:
            return "BUY"
        elif expected_return < -0.015 and confidence > 0.35:
            return "SHORT-BIAS"
        elif expected_return > 0.0:
            return "WATCH"
        else:
            return "AVOID"

    def execute_pipeline(self, ticker: str, asof_date: str):
        """
        Main runner executing the intertwined pipeline: Extracts metrics, triggers
        the internal Engine 1.5 tracker modifier context, and writes output rows
        simultaneously to the shared ledger database contract.
        """
        df_hist = self.fetch_historical_matrix(ticker, asof_date)
        if df_hist.empty:
            print(f"[ERROR] Microstructure base history empty for {ticker}")
            return

        intraday_energy = self.fetch_live_intraday_energy(ticker, asof_date)
        macro_state = self.calculate_macro_regime(asof_date)
        horizons = {"2D": 2, "5D": 5, "20D": 20}

        with engine.begin() as conn:
            for h_label, h_days in horizons.items():
                prediction = self.fit_and_predict(
                    df_hist, intraday_energy, macro_state, h_days
                )

                if prediction:
                    signal = self.generate_signal(
                        prediction["expected_return"],
                        prediction["confidence"],
                        intraday_energy,
                        prediction["veto_flag"],
                    )

                    payload = {
                        "engine_name": self.engine_name,
                        "ticker": ticker,
                        "asof_date": datetime.strptime(asof_date, "%Y-%m-%d").date(),
                        "horizon": h_label,
                        "signal": signal,
                        "score": prediction["expected_return"],
                        "confidence": prediction["confidence"],
                        "veto_flag": prediction["veto_flag"],
                        "penalty": prediction["penalty"],
                        "target_metric": f"{h_label} Return Horizon",
                        "reason_json": prediction["reasoning"],
                        "feature_json": prediction["features"],
                        "data_quality_score": 1.0 if len(df_hist) > 100 else 0.6,
                    }

                    stmt = insert(prediction_ledger).values(**payload)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            "engine_name",
                            "ticker",
                            "asof_date",
                            "horizon",
                        ],
                        set_=payload,
                    )
                    conn.execute(stmt)
                    print(
                        f"[SUCCESS] Intertwined Payload Committed: {ticker} | {h_label} | Signal: {signal} | Regime: {macro_state['regime']}"
                    )


def run_mass_historical_backfill(days_depth=60):
    """
    Automated Batch Wrapper: Dynamically sweeps all active market tickers
    across the maximum available historical timeline to seed the prediction ledger.
    """
    from scripts.database import engine
    from sqlalchemy import text

    # 1. Fetch all active stock tickers from metadata
    ticker_query = text('SELECT "Ticker" FROM market_metadata WHERE "IsActive" = true')
    with engine.connect() as conn:
        tickers = [row[0] for row in conn.execute(ticker_query).fetchall()]

    if not tickers:
        print("[ERROR] No active tickers discovered in market_metadata.")
        return

    # 2. Generate date array moving backward from today up to the maximum data depth
    today = datetime.now()
    date_list = [
        (today - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, days_depth + 1)
    ]

    print(
        f"[START] Initiating mass backfill for {len(tickers)} stocks over {days_depth} days ({len(date_list)} execution dates)..."
    )

    pipeline = OLSMicrostructureEngine()

    # 3. Double-loop timeline traversal (Optimized to execute atomically per stock/date junction)
    for target_date in date_list:
        # Skip weekends (NSE/BSE non-trading days)
        day_of_week = datetime.strptime(target_date, "%Y-%m-%d").weekday()
        if day_of_week >= 5:
            continue

        print(f"\n--- Processing Timeline Snapshot: {target_date} ---")
        for ticker in tickers:
            try:
                pipeline.execute_pipeline(ticker=ticker, asof_date=target_date)
            except Exception as e:
                print(f"[SKIP] Pipeline error for {ticker} on {target_date}: {e}")


if __name__ == "__main__":

    run_mass_historical_backfill(days_depth=60)


if __name__ == "__main__":
    engine_instance = OLSMicrostructureEngine()
    engine_instance.execute_pipeline(ticker="RELIANCE.NS", asof_date="2026-05-22")
