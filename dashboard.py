import streamlit as st
import pandas as pd
from sqlalchemy import text
from scripts.database import engine
from scripts.statementScrape import run_etl_pipeline
from scripts.ratioAnalysis import (
    fetch_ccc,
    fetch_cfo_to_pat,
    fetch_operating_margin,
    fetch_debt_to_equity,
    fetch_roic,
    fetch_fcf_yield,
    fetch_dol,
    fetch_gross_margin,
    fetch_interest_coverage,
    fetch_asset_turnover,
)
from scripts.macroScrape import run_macro_pipeline
from scripts.macroAnalysis import Phase2_OLS_Engine
import plotly.graph_objects as go

st.set_page_config(
    page_title="Swarm Intelligence Platform",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.sidebar.title("Swarm Intelligence")
st.sidebar.markdown("Institutional Quantitative Platform")
st.sidebar.divider()

app_mode = st.sidebar.radio(
    "NAVIGATION MENU",
    ["ETL Control Center", "Single Company Deep Dive", "Market Overview"],
    key="main_nav_radio",
)

st.sidebar.divider()
st.sidebar.caption("System Status: Online")
st.sidebar.caption("Database: PostgreSQL Connected")


if app_mode == "ETL Control Center":
    st.title("ETL Control Center")
    st.markdown(
        "Run the mathematically validated pipeline on target companies and monitor forensic outputs."
    )

    st.subheader("Macro Trends")
    st.markdown(
        "Fetch global economic weather: US/IN 10Y Yields, Brent Crude, and DXY."
    )

    if st.button("Run Macro Pipeline", type="secondary", key="macro_start_button"):
        with st.spinner("Executing hybrid spigots... Check terminal for logs."):
            try:
                success, row_count = run_macro_pipeline()
                if success:
                    st.success(
                        f"Macro Pipeline Complete! {row_count} daily records upserted."
                    )
                else:
                    st.warning("Pipeline executed but no data was extracted.")
            except Exception as e:
                st.error(f"Macro pipeline crashed during execution: {e}")

    st.divider()

    st.subheader("Execute ETL Batch")

    col1, col2 = st.columns([3, 1])
    with col1:
        ticker_input = st.text_input(
            "Target Tickers (comma-separated)",
            placeholder="e.g., RELIANCE.NS, TATAPOWER.NS",
            key="etl_ticker_input_widget",
        )
    with col2:
        selected_mode = st.radio(
            "AI Routing Engine:",
            ["Local (Ollama)", "Cloud (Gemini)"],
            horizontal=True,
            key="etl_ai_mode_radio",
        )

    if "pipeline_results" not in st.session_state:
        st.session_state.pipeline_results = None

    if st.button(
        "Start ETL Pipeline",
        type="primary",
        use_container_width=False,
        key="etl_start_button",
    ):
        if not ticker_input.strip():
            st.warning("Please enter at least one ticker to proceed.")
        else:
            target_tickers = [
                t.strip().upper() for t in ticker_input.split(",") if t.strip()
            ]
            mode_param = "local" if "Local" in selected_mode else "cloud"

            with st.spinner(
                f"Executing pipeline for {len(target_tickers)} client(s)... Check terminal for logs."
            ):
                try:
                    st.session_state.pipeline_results = run_etl_pipeline(
                        target_tickers=target_tickers, ai_mode=mode_param
                    )
                    st.success("ETL Batch Processing Complete!")
                except Exception as e:
                    st.error(f"Pipeline crashed during execution: {e}")

    if st.session_state.pipeline_results:
        batch_results = st.session_state.pipeline_results

        summary_df = pd.DataFrame(
            [
                {k: v for k, v in res.items() if k != "DataPayload"}
                for res in batch_results
            ]
        )

        st.dataframe(
            summary_df,
            column_config={
                "Ticker": st.column_config.TextColumn("Client (Ticker)"),
                "Status": st.column_config.TextColumn("Fetch Status"),
                "Direct Validation": st.column_config.TextColumn(
                    "Direct Audit (BS/IS/CF)"
                ),
                "Indirect Validation": st.column_config.TextColumn(
                    "Indirect Audit (OCF/FCF)"
                ),
                "Rows Upserted": st.column_config.NumberColumn("Periods Upserted"),
            },
            hide_index=True,
            use_container_width=True,
        )

        st.divider()
        st.subheader("Deep Dive Data Inspector")
        valid_results = [r for r in batch_results if "DataPayload" in r]

        if valid_results:
            inspector_col1, inspector_col2 = st.columns([1, 2])
            with inspector_col1:
                selected_ticker = st.selectbox(
                    "Select Client:",
                    [r["Ticker"] for r in valid_results],
                    key="etl_client_inspector_selectbox",
                )
            with inspector_col2:
                selected_statement = st.radio(
                    "Select Statement:",
                    [
                        "Income Statement (IS)",
                        "Balance Sheet (BS)",
                        "Cash Flow (CF)",
                        "Indirect Cash Flow (ICF)",
                    ],
                    horizontal=True,
                    key="etl_statement_radio",
                )

            if "Income" in selected_statement:
                stmt_key = "IS"
            elif "Balance" in selected_statement:
                stmt_key = "BS"
            elif "Indirect" in selected_statement:
                stmt_key = "ICF"
            else:
                stmt_key = "CF"

            client_data = next(
                (
                    item["DataPayload"]
                    for item in valid_results
                    if item["Ticker"] == selected_ticker
                ),
                None,
            )

            if client_data:
                raw_df = client_data[stmt_key]["Raw"]
                clean_df = client_data[stmt_key]["Clean"]

                view_col1, view_col2 = st.columns(2)
                with view_col1:
                    st.caption(f"Raw Extracted Data ({selected_ticker} - {stmt_key})")
                    st.dataframe(raw_df, use_container_width=True)
                with view_col2:
                    st.caption(
                        f"Final DB-Formatted Data ({selected_ticker} - {stmt_key})"
                    )
                    st.dataframe(clean_df, use_container_width=True)
###############################################
elif app_mode == "Single Company Deep Dive":
    st.title("Single Company Deep Dive")

    try:
        with engine.connect() as conn:
            tickers_df = pd.read_sql(
                'SELECT DISTINCT "Ticker" FROM yearly_income_statement', conn
            )
            available_db_tickers = tickers_df["Ticker"].tolist()
    except Exception as e:
        available_db_tickers = []
        st.error(f"Failed to connect to database: {e}")

    if not available_db_tickers:
        st.info("No data available in the database. Please run the ETL pipeline first.")
    else:
        selected_db_ticker = st.selectbox(
            "Select Company to Analyze:",
            available_db_tickers,
            key="single_db_ticker_selectbox",
        )

        with st.spinner(f"Running Forensic Scan for {selected_db_ticker}..."):
            # Fetch Phase 1: Structural Anchors
            df_roic = fetch_roic(selected_db_ticker)
            df_fcf = fetch_fcf_yield(selected_db_ticker)
            df_de = fetch_debt_to_equity(selected_db_ticker)
            df_ccc = fetch_ccc(selected_db_ticker)
            df_dol = fetch_dol(selected_db_ticker)

            # Fetch Phase 2: Tactical Responders
            df_op_margin = fetch_operating_margin(selected_db_ticker)
            df_gr_margin = fetch_gross_margin(selected_db_ticker)
            df_int_cov = fetch_interest_coverage(selected_db_ticker)
            df_cfo_pat = fetch_cfo_to_pat(selected_db_ticker)
            df_turnover = fetch_asset_turnover(selected_db_ticker)

            # --- TOP LEVEL: STRUCTURAL HEALTH (PHASE 1) ---
            st.divider()
            st.subheader("Phase 1: Structural Anchors")
            st.caption("Long-term capital efficiency and survival metrics.")

            yc1, yc2, yc3, yc4, yc5 = st.columns(5)
            with yc1:
                latest_roic = (
                    df_roic["roic"].iloc[0] * 100
                    if not df_roic.empty and pd.notnull(df_roic["roic"].iloc[0])
                    else 0
                )
                st.metric("ROIC", f"{latest_roic:.2f}%")
            with yc2:
                latest_fcf = (
                    df_fcf["FCF_Yield"].iloc[0] * 100
                    if not df_fcf.empty
                    and pd.notnull(df_fcf.get("FCF_Yield", pd.Series([None])).iloc[0])
                    else 0
                )
                st.metric("FCF Yield", f"{latest_fcf:.2f}%")
            with yc3:
                latest_de = (
                    df_de["debt_to_equity"].iloc[0]
                    if not df_de.empty and pd.notnull(df_de["debt_to_equity"].iloc[0])
                    else 0
                )
                st.metric("Debt / Equity", f"{latest_de:.2f}")
            with yc4:
                latest_ccc = (
                    df_ccc["cash_conversion_cycle"].iloc[0]
                    if not df_ccc.empty
                    and pd.notnull(df_ccc["cash_conversion_cycle"].iloc[0])
                    else 0
                )
                st.metric("Cash Conv. Cycle", f"{latest_ccc:.0f} Days")
            with yc5:
                latest_dol = (
                    df_dol["degree_of_operating_leverage"].iloc[0]
                    if not df_dol.empty
                    and pd.notnull(df_dol["degree_of_operating_leverage"].iloc[0])
                    else 0
                )
                st.metric("Op. Leverage (DOL)", f"{latest_dol:.2f}x")

            # --- MIDDLE LEVEL: TACTICAL RESPONDERS (PHASE 2) ---
            st.divider()
            st.subheader("Phase 2: Tactical Responders")
            st.caption("Immediate shock absorbers for macro weather impacts.")

            qc1, qc2, qc3, qc4, qc5 = st.columns(5)
            with qc1:
                latest_op_margin = (
                    df_op_margin["operating_margin"].iloc[0] * 100
                    if not df_op_margin.empty
                    and pd.notnull(df_op_margin["operating_margin"].iloc[0])
                    else 0
                )
                st.metric("Operating Margin", f"{latest_op_margin:.2f}%")
            with qc2:
                latest_gr_margin = (
                    df_gr_margin["gross_margin"].iloc[0] * 100
                    if not df_gr_margin.empty
                    and pd.notnull(df_gr_margin["gross_margin"].iloc[0])
                    else 0
                )
                st.metric("Gross Margin", f"{latest_gr_margin:.2f}%")
            with qc3:
                latest_int_cov = (
                    df_int_cov["interest_coverage"].iloc[0]
                    if not df_int_cov.empty
                    and pd.notnull(df_int_cov["interest_coverage"].iloc[0])
                    else 0
                )
                st.metric("Interest Coverage", f"{latest_int_cov:.2f}x")
            with qc4:
                latest_cfo_pat = (
                    df_cfo_pat["cfo_to_pat"].iloc[0]
                    if not df_cfo_pat.empty
                    and pd.notnull(df_cfo_pat["cfo_to_pat"].iloc[0])
                    else 0
                )
                st.metric("CFO / PAT", f"{latest_cfo_pat:.2f}")
            with qc5:
                latest_turnover = (
                    df_turnover["asset_turnover"].iloc[0]
                    if not df_turnover.empty
                    and pd.notnull(df_turnover["asset_turnover"].iloc[0])
                    else 0
                )
                st.metric("Asset Turnover", f"{latest_turnover:.2f}x")

            # --- BOTTOM LEVEL: RAW DATA ROOM ---
            st.divider()
            st.subheader("The Data Room")

            with st.expander("View Raw Structural Matrices (Phase 1)"):
                if not df_roic.empty:
                    p1_merged = df_roic[["ReportDate", "roic"]]
                    if not df_fcf.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_fcf[["ReportDate", "FCF_Yield"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_de.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_de[["ReportDate", "debt_to_equity"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_ccc.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_ccc[["ReportDate", "cash_conversion_cycle"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_dol.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_dol[["ReportDate", "degree_of_operating_leverage"]],
                            on="ReportDate",
                            how="outer",
                        )
                    st.dataframe(
                        p1_merged.sort_values(by="ReportDate", ascending=False),
                        use_container_width=True,
                    )
                else:
                    st.write("No structural data computed.")

            with st.expander("View Raw Tactical Matrices (Phase 2)"):
                if not df_op_margin.empty:
                    p2_merged = df_op_margin[["ReportDate", "operating_margin"]]
                    if not df_gr_margin.empty:
                        p2_merged = pd.merge(
                            p2_merged,
                            df_gr_margin[["ReportDate", "gross_margin"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_int_cov.empty:
                        p2_merged = pd.merge(
                            p2_merged,
                            df_int_cov[["ReportDate", "interest_coverage"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_cfo_pat.empty:
                        p2_merged = pd.merge(
                            p2_merged,
                            df_cfo_pat[["ReportDate", "cfo_to_pat"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_turnover.empty:
                        p2_merged = pd.merge(
                            p2_merged,
                            df_turnover[["ReportDate", "asset_turnover"]],
                            on="ReportDate",
                            how="outer",
                        )
                    st.dataframe(
                        p2_merged.sort_values(by="ReportDate", ascending=False),
                        use_container_width=True,
                    )
                else:
                    st.write("No tactical data computed.")

            st.divider()
            st.header("Phase 2: OLS Macro Bridge & Forensic Triage")

            # 1. Fetch and format the live Macro Spigot data from DB
            try:
                macro_query = text(
                    'SELECT "ReportDate", "IndicatorName", "Value" FROM macro_indicators'
                )
                macro_raw = pd.read_sql(macro_query, engine)
                macro_df = macro_raw.pivot(
                    index="ReportDate", columns="IndicatorName", values="Value"
                )
                macro_df.index = pd.to_datetime(macro_df.index)
            except Exception as e:
                st.error(f"Failed to load Macro Database: {e}")
                macro_df = pd.DataFrame()

            if not macro_df.empty:
                # 2. UI Selector for the Target Pillar (UPDATED VARIABLE NAMES HERE)
                target_options = {
                    "Operating Margin (Primary Bridge)": (
                        "operating_margin",
                        df_op_margin,
                    ),
                    "Gross Margin (Anchor)": ("gross_margin", df_gr_margin),
                    "Interest Coverage (Solvency)": ("interest_coverage", df_int_cov),
                    "Asset Turnover (Productivity)": ("asset_turnover", df_turnover),
                }
                selected_pillar_label = st.selectbox(
                    "Select Micro Pillar to Validate:", list(target_options.keys())
                )
                target_col, target_df = target_options[selected_pillar_label]

                if not target_df.empty:
                    # 3. Instantiate the Black Box Engine
                    ols_engine = Phase2_OLS_Engine(macro_df, target_df)
                    initial_payload = ols_engine.run_static_baseline(target_col)

                    if "error" in initial_payload:
                        st.warning(initial_payload["error"])
                    else:
                        tl = initial_payload["timeline_data"]

                        # 4. Calculate Triage Recommendations
                        n = len(tl["dates"])
                        cooks_threshold = 4 / n if n > 0 else 0
                        recommended_exclusions = [
                            date
                            for date, cook_d in zip(tl["dates"], tl["cooks_distance"])
                            if cook_d > cooks_threshold
                        ]

                        # 5. The Control Room
                        st.subheader("Forensic Execution Board")
                        col_triage1, col_triage2 = st.columns([2, 1])

                        with col_triage1:
                            excluded_dates = st.multiselect(
                                "Exclude Structural Outliers (High Leverage Dots automatically selected):",
                                options=tl["dates"],
                                default=recommended_exclusions,
                                help="Removing these dates instantly recalculates the Alpha Moat and Beta sensitivities.",
                            )

                        # 6. Run Final Clean Baseline
                        final_payload = ols_engine.run_static_baseline(
                            target_col, excluded_dates=excluded_dates
                        )
                        clean_tl = final_payload["timeline_data"]

                        with col_triage2:
                            st.metric(
                                "Bridge Strength (R-Squared)",
                                f"{final_payload['r_squared']*100:.1f}%",
                            )

                        # 7. Rendering Addition 1: The 95% Confidence Bands
                        st.markdown(
                            "### 1. The Bridge: Actual vs. Predicted (95% Confidence Bands)"
                        )
                        chart_df = pd.DataFrame(
                            {
                                "ReportDate": clean_tl["dates"],
                                "Actual Reported": clean_tl["actual_y"],
                                "OLS Predicted (The Line)": clean_tl["predicted_y"],
                                "Upper Tolerance Band": clean_tl["conf_upper"],
                                "Lower Tolerance Band": clean_tl["conf_lower"],
                            }
                        ).set_index("ReportDate")
                        st.line_chart(
                            chart_df, color=["#FF4B4B", "#0068C9", "#808080", "#808080"]
                        )

                        st.markdown("### 2. Management Skill: The Residual Tracker")
                        st.caption(
                            "Green bars indicate 'Alpha' (Beating the macro odds). Red bars indicate 'Rot' (Underperforming the macro environment)."
                        )

                        # Determine colors: Green if positive, Red if negative
                        bar_colors = [
                            "#00C851" if val > 0 else "#FF4444"
                            for val in clean_tl["residuals"]
                        ]

                        fig = go.Figure(
                            data=[
                                go.Bar(
                                    x=clean_tl["dates"],
                                    y=clean_tl["residuals"],
                                    marker_color=bar_colors,
                                    width=0.4,
                                )
                            ]
                        )

                        fig.update_layout(
                            margin=dict(l=0, r=0, t=20, b=0),
                            yaxis_title="Margin Beat/Miss",
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            # Draw a hard, bright zero-line so the user can easily see above/below
                            shapes=[
                                dict(
                                    type="line",
                                    xref="paper",
                                    x0=0,
                                    x1=1,
                                    y0=0,
                                    y1=0,
                                    line=dict(
                                        color="rgba(255, 255, 255, 0.5)", width=2
                                    ),
                                )
                            ],
                        )

                        st.plotly_chart(fig, use_container_width=True)

                        # 9. DNA Output
                        with st.expander("View Mathematical DNA (Alpha Moat & Betas)"):
                            st.markdown(
                                f"**Alpha (Structural Moat):** `{final_payload['alpha_moat']}`"
                            )
                            st.write("**Beta Sensitivities:**")
                            st.json(final_payload["betas"])
                            st.write("**P-Values (Statistical Significance):**")
                            st.json(final_payload["p_values"])
            else:
                st.info(
                    "Macro Data is missing. Please run the ETL Control Center Macro Spigot first."
                )
############################################
elif app_mode == "Market Overview":
    st.title("Market Overview")
    st.markdown("Cross-sectional ranking and quadrant analysis.")

    # Fetch available sectors from the company_profiles table
    try:
        with engine.connect() as conn:
            sectors_df = pd.read_sql(
                'SELECT DISTINCT "Sector" FROM company_profiles WHERE "Sector" IS NOT NULL',
                conn,
            )
            available_sectors = ["All Market"] + sectors_df["Sector"].tolist()
    except Exception:
        available_sectors = ["All Market"]

    selected_sector = st.selectbox("Filter by Sector", available_sectors)

    if st.button("Generate Sector Analysis", type="primary"):
        with st.spinner("Aggregating live cross-sectional data..."):

            # Query the DB for tickers matching the filter
            try:
                with engine.connect() as conn:
                    if selected_sector == "All Market":
                        query = 'SELECT "Ticker", "CompanyName" FROM company_profiles'
                    else:
                        query = f'SELECT "Ticker", "CompanyName" FROM company_profiles WHERE "Sector" = \'{selected_sector}\''
                    target_companies = pd.read_sql(query, conn)
            except Exception as e:
                st.error(f"Failed to query sectors: {e}")
                target_companies = pd.DataFrame()

            if target_companies.empty:
                st.warning(
                    "No companies found in this sector. Run the ETL pipeline on more tickers."
                )
            else:
                # Loop through the target tickers and run the math engine to get their latest values
                market_data = []
                for _, row in target_companies.iterrows():
                    t = row["Ticker"]
                    c_name = row["CompanyName"]

                    df_roic_tmp = fetch_roic(t)
                    df_fcf_tmp = fetch_fcf_yield(t)

                    if not df_roic_tmp.empty and not df_fcf_tmp.empty:
                        latest_roic = (
                            df_roic_tmp["roic"].dropna().iloc[0]
                            if not df_roic_tmp["roic"].dropna().empty
                            else None
                        )
                        latest_fcf = (
                            df_fcf_tmp["FCF_Yield"].dropna().iloc[0]
                            if "FCF_Yield" in df_fcf_tmp.columns
                            and not df_fcf_tmp["FCF_Yield"].dropna().empty
                            else None
                        )

                        if latest_roic is not None and latest_fcf is not None:
                            market_data.append(
                                {
                                    "Ticker": t,
                                    "Company": c_name,
                                    "ROIC (%)": latest_roic * 100,
                                    "FCF Yield (%)": latest_fcf * 100,
                                }
                            )

                market_df = pd.DataFrame(market_data)

                if not market_df.empty:
                    st.divider()
                    st.subheader("The Magic Formula: FCF Yield vs ROIC")
                    st.markdown(
                        "Target the **Top-Right** (High Efficiency + Undervalued)."
                    )

                    # Native Streamlit Scatter Chart (requires Streamlit 1.32+)
                    st.scatter_chart(
                        market_df,
                        x="FCF Yield (%)",
                        y="ROIC (%)",
                        color="Ticker",
                        height=500,
                    )

                    st.subheader("Sector Leaderboard")
                    # Sort leaderboard by ROIC descending
                    leaderboard = market_df.sort_values(
                        by="ROIC (%)", ascending=False
                    ).reset_index(drop=True)
                    st.dataframe(leaderboard, use_container_width=True)
                else:
                    st.warning(
                        "Not enough clean ratio data to generate the quadrant map."
                    )
