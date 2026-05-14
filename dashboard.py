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
st.sidebar.markdown("### Engine Settings")
selected_source = st.sidebar.selectbox(
    "Primary Data Source",
    options=["vantage", "yfinance", "screener"],
    index=0,
    help="Strictly isolates all mathematical models to data provided by this specific spigot.",
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

    # Data Spigot Selector
    st.caption("Data Spigot Configuration")
    selected_spigot = st.radio(
        "Select Primary Data Source (Auto-Rotate highly recommended to bypass API limits):",
        [
            "Auto-Rotate (FMP -> Alpha Vantage)",
            "Financial Modeling Prep (FMP)",
            "Alpha Vantage",
            "Yahoo Finance",
            "Screener.in",
        ],
        horizontal=True,
        key="etl_spigot_radio",
    )

    # Map UI selection to the backend router strings
    spigot_map = {
        "Auto-Rotate (FMP -> Alpha Vantage)": "auto",
        "Financial Modeling Prep (FMP)": "fmp",
        "Alpha Vantage": "vantage",
        "Yahoo Finance": "yfinance",
        "Screener.in": "screener",
    }

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
            backend_source = spigot_map[selected_spigot]

            with st.spinner(
                f"Executing pipeline via {selected_spigot} for {len(target_tickers)} client(s)..."
            ):
                try:
                    #  Passing the requested_source into the ETL engine
                    st.session_state.pipeline_results = run_etl_pipeline(
                        target_tickers=target_tickers,
                        ai_mode=mode_param,
                        requested_source=backend_source,
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
        try:
            with engine.connect() as conn:
                break_date_res = conn.execute(
                    text(
                        'SELECT valid_data_since, "Sector", "Industry" FROM company_profiles WHERE "Ticker" = :ticker'
                    ),
                    {"ticker": selected_db_ticker},
                ).fetchone()

                edgar_break_date = break_date_res[0] if break_date_res else None
                company_sector = (
                    break_date_res[1]
                    if break_date_res and len(break_date_res) > 1
                    else "DEFAULT"
                )
                # Fetch the highly specific industry
                company_industry = (
                    break_date_res[2]
                    if break_date_res and len(break_date_res) > 2
                    else "DEFAULT"
                )
        except Exception:
            edgar_break_date = None
            company_sector = "DEFAULT"
            company_industry = "DEFAULT"

        st.markdown("### Swarm Memory Controls")
        view_mode = st.radio(
            "Select Historical Perspective:",
            [
                "Swarm Predictive (Strictly Post-Edgar)",
                "Historical Audit (Merged Before & After)",
            ],
            horizontal=True,
        )

        with st.spinner(
            f"Running Forensic Scan for {selected_db_ticker} using {selected_source.upper()} data..."
        ):
            # Fetch Phase 1: Structural Anchors (Strictly Isolated)
            df_roic = fetch_roic(selected_db_ticker, selected_source)
            df_fcf = fetch_fcf_yield(selected_db_ticker, selected_source)
            df_de = fetch_debt_to_equity(selected_db_ticker, selected_source)
            df_ccc = fetch_ccc(selected_db_ticker, selected_source)
            df_dol = fetch_dol(selected_db_ticker, selected_source)

            # Fetch Phase 2: Tactical Responders (Strictly Isolated)
            df_op_margin = fetch_operating_margin(selected_db_ticker, selected_source)
            df_gr_margin = fetch_gross_margin(selected_db_ticker, selected_source)
            df_int_cov = fetch_interest_coverage(selected_db_ticker, selected_source)
            df_cfo_pat = fetch_cfo_to_pat(selected_db_ticker, selected_source)
            df_turnover = fetch_asset_turnover(selected_db_ticker, selected_source)

            if "Predictive" in view_mode and edgar_break_date:
                cutoff = pd.to_datetime(edgar_break_date)

                # Apply to Phase 1
                df_roic = df_roic[pd.to_datetime(df_roic["ReportDate"]) > cutoff]
                df_fcf = df_fcf[pd.to_datetime(df_fcf["ReportDate"]) > cutoff]
                df_de = df_de[pd.to_datetime(df_de["ReportDate"]) > cutoff]
                df_ccc = df_ccc[pd.to_datetime(df_ccc["ReportDate"]) > cutoff]
                df_dol = df_dol[pd.to_datetime(df_dol["ReportDate"]) > cutoff]

                # Apply to Phase 2
                df_op_margin = df_op_margin[
                    pd.to_datetime(df_op_margin["ReportDate"]) > cutoff
                ]
                df_gr_margin = df_gr_margin[
                    pd.to_datetime(df_gr_margin["ReportDate"]) > cutoff
                ]
                df_int_cov = df_int_cov[
                    pd.to_datetime(df_int_cov["ReportDate"]) > cutoff
                ]
                df_cfo_pat = df_cfo_pat[
                    pd.to_datetime(df_cfo_pat["ReportDate"]) > cutoff
                ]
                df_turnover = df_turnover[
                    pd.to_datetime(df_turnover["ReportDate"]) > cutoff
                ]

            # --- TOP LEVEL: STRUCTURAL HEALTH (PHASE 1) ---
            st.divider()
            st.subheader("Structural Anchors")
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
            st.subheader("Data Room")

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
            st.header("OLS Macro Bridge & Forensic Triage")

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

                # 1. Fetch the Initial Baseline (Pass the Sector for Smart Defaults)
                initial_payload = ols_engine.run_static_baseline(
                    target_col, sector=company_sector, industry=company_industry
                )

                if "error" in initial_payload:
                    st.warning(
                        f"Could not process {target_col}: {initial_payload['error']}"
                    )
                else:
                    init_tl = initial_payload["timeline_data"]

                    # --- UI NOTIFICATION & MACRO SANDBOX ---
                    n_quarters = len(init_tl["dates"])
                    custom_macro_selection = None

                    if 12 <= n_quarters < 24:
                        active_beams = list(initial_payload["betas"].keys())
                        st.info(
                            f"💡 Limited Data Detected (N={n_quarters}). Engine auto-loaded the **{company_sector}** macro template: `{active_beams}`"
                        )

                        # The full list of 8 beams the user can choose from
                        all_beams = [
                            "Brent_Crude",
                            "USD_INR",
                            "Broad_Commodity",
                            "US_Dollar_Index_3M",
                            "India_CPI_3M",
                            "India_10Y_Yield_6M",
                            "US_10Y_Yield_6M",
                            "Yield_Spread_6M",
                        ]

                        default_beams = [b for b in active_beams if b in all_beams]

                        # The Interactive Multiselect
                        custom_macro_selection = st.multiselect(
                            "Override Macro Template (Select exactly 3):",
                            options=all_beams,
                            default=default_beams,
                            max_selections=3,
                            key=f"macro_override_{target_col}",
                        )

                        # If the user manually changes the beams, instantly recalculate the baseline!
                    if (
                        custom_macro_selection is not None
                        and len(custom_macro_selection) == 3
                        and set(custom_macro_selection) != set(default_beams)
                    ):
                        init_tl = initial_payload["timeline_data"]
                    # -----------------------------------------------

                    # Identify exactly which dates triggered the Cook's alarm
                    system_outliers = [
                        date
                        for date, is_out in zip(init_tl["dates"], init_tl["is_outlier"])
                        if is_out
                    ]

                    # --- OUTLIER CONTROL CENTER ---
                    st.markdown("### Outlier Control Center")

                    if system_outliers:
                        st.warning(
                            f"System detected {len(system_outliers)} mathematical anomalies: {', '.join(system_outliers)}"
                        )
                    else:
                        st.success(" No mathematical anomalies detected.")

                    # The 3-Way Interactive Toggle
                    handling_mode = st.radio(
                        "Select Outlier Handling Strategy:",
                        [
                            "Include All Data",
                            "Exclude System-Detected Outliers",
                            "Custom Manual Selection",
                        ],
                        horizontal=True,
                        key=f"radio_outlier_{target_col}",
                    )

                    # Logic to capture human selection
                    final_exclusions = []
                    if handling_mode == "Exclude System-Detected Outliers":
                        final_exclusions = system_outliers
                    elif handling_mode == "Custom Manual Selection":
                        final_exclusions = st.multiselect(
                            "Select specific quarters to drop from the model:",
                            options=init_tl["dates"],
                            default=system_outliers,
                            key=f"multi_{target_col}",
                        )

                        # 2. Recalculate the Math based on Human Input (Passing custom beams if active)
                    if final_exclusions:
                        clean_payload = ols_engine.run_static_baseline(
                            target_col,
                            sector=company_sector,
                            industry=company_industry,
                            excluded_dates=final_exclusions,
                            custom_beams=custom_macro_selection,
                        )
                        if "error" in clean_payload:
                            clean_payload = initial_payload
                    else:
                        clean_payload = initial_payload

                    clean_tl = clean_payload["timeline_data"]
                    final_payload = clean_payload

                    # --- RESTORED R-SQUARED METRIC ---
                    st.metric(
                        "Bridge Strength (R-Squared)",
                        f"{final_payload['r_squared']*100:.1f}%",
                        help="Measures how much of the company's variance is mathematically explained by the 8 Macro Beams.",
                    )
                    st.divider()
                    st.markdown(f"### The Bridge: {target_col} Actual vs. Predicted")

                    fig_line = go.Figure()

                    # 1. Tolerance Bands
                    fig_line.add_trace(
                        go.Scatter(
                            x=clean_tl["dates"] + clean_tl["dates"][::-1],
                            y=clean_tl["conf_upper"] + clean_tl["conf_lower"][::-1],
                            fill="toself",
                            fillcolor="rgba(128,128,128,0.2)",
                            line=dict(color="rgba(255,255,255,0)"),
                            name="95% Tolerance",
                            hoverinfo="skip",
                        )
                    )

                    # 2. Split Actuals into "Normal" and "Detected Outliers" for coloring
                    normal_x, normal_y = [], []
                    outlier_x, outlier_y = [], []

                    for i in range(len(init_tl["dates"])):
                        if (
                            init_tl["is_outlier"][i]
                            or init_tl["dates"][i] in final_exclusions
                        ):
                            outlier_x.append(init_tl["dates"][i])
                            outlier_y.append(init_tl["actual_y"][i])
                        else:
                            normal_x.append(init_tl["dates"][i])
                            normal_y.append(init_tl["actual_y"][i])

                    # --- RESTORED: THE ACTUAL LINE GRAPH ---
                    fig_line.add_trace(
                        go.Scatter(
                            x=init_tl["dates"],
                            y=init_tl["actual_y"],
                            mode="lines",
                            name="Actual Trendline",
                            line=dict(color="#FF4B4B", width=2),
                            hoverinfo="skip",
                        )
                    )
                    # ---------------------------------------

                    # Plot Normal Data (Red Dots)
                    fig_line.add_trace(
                        go.Scatter(
                            x=normal_x,
                            y=normal_y,
                            mode="markers",
                            name="Normal Quarter",
                            marker=dict(color="#FF4B4B", size=8),
                        )
                    )

                    # Plot Anomalies (Large Yellow Dots)
                    fig_line.add_trace(
                        go.Scatter(
                            x=outlier_x,
                            y=outlier_y,
                            mode="markers",
                            name="Flagged Outlier",
                            marker=dict(
                                color="#FFC107",
                                size=12,
                                line=dict(color="red", width=2),
                            ),
                            hovertemplate="<b>%{x}</b><br>Outlier Value: %{y}<extra></extra>",
                        )
                    )

                    # 3. Add OLS Predicted Line (Uses Cleaned Data)
                    fig_line.add_trace(
                        go.Scatter(
                            x=clean_tl["dates"],
                            y=clean_tl["predicted_y"],
                            mode="lines",
                            name="OLS Predicted",
                            line=dict(color="#0068C9", dash="dash", width=2),
                            connectgaps=True,
                        )
                    )

                    # --- NEW: STEP 4 THE PHANTOM DOT ---
                    if "phantom_dot" in final_payload and final_payload["phantom_dot"]:
                        phantom = final_payload["phantom_dot"]

                        # Connect the last OLS point to the Phantom Dot with a forward-looking dotted line
                        fig_line.add_trace(
                            go.Scatter(
                                x=[clean_tl["dates"][-1], phantom["target_date"]],
                                y=[
                                    clean_tl["predicted_y"][-1],
                                    phantom["predicted_value"],
                                ],
                                mode="lines",
                                name="Forward Trajectory",
                                line=dict(color="#00FFAA", dash="dot", width=2),
                                showlegend=False,
                                hoverinfo="skip",
                            )
                        )

                        # Draw the Ghost Dot
                        fig_line.add_trace(
                            go.Scatter(
                                x=[phantom["target_date"]],
                                y=[phantom["predicted_value"]],
                                mode="markers",
                                name="Phantom Predictor (Next Qtr)",
                                marker=dict(
                                    color="rgba(0,0,0,0)",  # Transparent fill (Ghost dot)
                                    size=14,
                                    line=dict(
                                        color="#00FFAA", width=3
                                    ),  # Bright neon border
                                ),
                                hovertemplate="<b>Projected: %{x}</b><br>Value: %{y}<extra></extra>",
                            )
                        )
                    # -----------------------------------

                    try:
                        if "Audit" in view_mode and edgar_break_date:
                            break_dt = str(edgar_break_date)
                            fig_line.add_vline(
                                x=break_dt, line_dash="dash", line_color="white"
                            )
                            fig_line.add_annotation(
                                x=break_dt,
                                y=1,
                                yref="paper",
                                text=" SEC Corporate Action",
                                showarrow=False,
                                xanchor="left",
                                yanchor="top",
                                font=dict(color="white", size=11),
                            )
                    except NameError:
                        pass

                    fig_line.update_layout(
                        margin=dict(l=0, r=0, t=30, b=0),
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(
                        fig_line,
                        use_container_width=True,
                        key=f"chart_{target_col}",
                    )

                    # --- NEW: PHANTOM PREDICTOR UI CALLOUT ---
                    if "phantom_dot" in final_payload and final_payload["phantom_dot"]:
                        phantom = final_payload["phantom_dot"]
                        st.success(
                            f"Based on live trailing 90-day macro data, the engine projects the target metric to hit **{phantom['predicted_value']:.4f}** for the upcoming unreleased quarter ending **{phantom['target_date']}**."
                        )

                    # ---  ADD THE DATA TABLE VIEWER BACK ---
                    with st.expander("View Underlying Numerical Data"):
                        chart_df = pd.DataFrame(
                            {
                                "ReportDate": clean_tl["dates"],
                                "Actual Reported": [
                                    round(val, 4) for val in clean_tl["actual_y"]
                                ],
                                "OLS Predicted": [
                                    round(val, 4) for val in clean_tl["predicted_y"]
                                ],
                                "Upper Tolerance Band": [
                                    round(val, 4) for val in clean_tl["conf_upper"]
                                ],
                                "Lower Tolerance Band": [
                                    round(val, 4) for val in clean_tl["conf_lower"]
                                ],
                            }
                        ).set_index("ReportDate")

                        st.dataframe(chart_df, use_container_width=True)
                    # -------------------------------------------
                    st.markdown("#### Residual Tracker")

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
                                line=dict(color="rgba(255, 255, 255, 0.5)", width=2),
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

                    # Strictly isolate the market scan to the selected spigot
                    df_roic_tmp = fetch_roic(t, selected_source)
                    df_fcf_tmp = fetch_fcf_yield(t, selected_source)

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
