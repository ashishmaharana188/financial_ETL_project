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
)
from scripts.macroScrape import run_macro_pipeline

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

        with st.spinner(f"Fetching Swarm Metrics for {selected_db_ticker}..."):
            df_ccc = fetch_ccc(selected_db_ticker)
            df_cfo_pat = fetch_cfo_to_pat(selected_db_ticker)
            df_margin = fetch_operating_margin(selected_db_ticker)
            df_leverage = fetch_debt_to_equity(selected_db_ticker)
            df_roic = fetch_roic(selected_db_ticker)
            df_fcf = fetch_fcf_yield(selected_db_ticker)
            df_dol = fetch_dol(selected_db_ticker)

            st.divider()

            # --- TOP SECTION: FOCUSED KPI CARDS ---
            st.subheader("Key Ratios (Latest FY)")
            kpi_c1, kpi_c2, kpi_c3, kpi_c4, kpi_c5 = st.columns(5)

            with kpi_c1:
                latest_margin = (
                    df_margin["operating_margin"].iloc[0] * 100
                    if (
                        not df_margin.empty
                        and pd.notnull(df_margin["operating_margin"].iloc[0])
                    )
                    else 0
                )
                st.metric("Op. Margin", f"{latest_margin:.2f}%")
            with kpi_c2:
                latest_de = (
                    df_leverage["debt_to_equity"].iloc[0]
                    if (
                        not df_leverage.empty
                        and pd.notnull(df_leverage["debt_to_equity"].iloc[0])
                    )
                    else 0
                )
                st.metric("Debt/Equity", f"{latest_de:.2f}")
            with kpi_c3:
                latest_roic = (
                    df_roic["roic"].iloc[0] * 100
                    if (not df_roic.empty and pd.notnull(df_roic["roic"].iloc[0]))
                    else 0
                )
                st.metric("ROIC", f"{latest_roic:.2f}%")
            with kpi_c4:
                latest_fcf = (
                    df_fcf["FCF_Yield"].iloc[0] * 100
                    if (
                        not df_fcf.empty
                        and pd.notnull(
                            df_fcf.get("FCF_Yield", pd.Series([None])).iloc[0]
                        )
                    )
                    else 0
                )
                st.metric("FCF Yield", f"{latest_fcf:.2f}%")
            with kpi_c5:
                valid_dol = df_dol["degree_of_operating_leverage"].dropna()
                latest_dol = valid_dol.iloc[0] if not valid_dol.empty else 0
                st.metric("DOL", f"{latest_dol:.2f}")

            # --- NEW: TREND CHARTING ---
            st.divider()
            st.subheader("Historical Trend Analysis")
            chart_col1, chart_col2 = st.columns(2)

            with chart_col1:
                st.caption("ROIC vs Operating Margin (%)")
                # Combine ROIC and Margin into one chart for correlation
                if not df_roic.empty and not df_margin.empty:
                    chart_df = pd.merge(
                        df_roic[["ReportDate", "roic"]],
                        df_margin[["ReportDate", "operating_margin"]],
                        on="ReportDate",
                    )
                    chart_df["ROIC"] = chart_df["roic"] * 100
                    chart_df["Margin"] = chart_df["operating_margin"] * 100
                    st.line_chart(chart_df.set_index("ReportDate")[["ROIC", "Margin"]])

            with chart_col2:
                st.caption("Debt-to-Equity History")
                if not df_leverage.empty:
                    de_chart = df_leverage[["ReportDate", "debt_to_equity"]].set_index(
                        "ReportDate"
                    )
                    st.line_chart(de_chart)

            # --- NEW: AI MEMO GENERATOR ---
            st.divider()
            st.subheader("Chief Investment Officer (CIO) Summary")
            if st.button("Generate AI Investment Memo", type="primary"):
                with st.spinner("AI is analyzing the financial blueprint..."):
                    # NOTE: Replace this mock output with a call to your AI runtime
                    # Example: summary = runtime.generate(prompt=f"Analyze {selected_db_ticker} with ROIC {latest_roic}...")

                    st.success("Analysis Complete")
                    st.markdown(f"""
                    **Executive Summary for {selected_db_ticker}**
                    * **Efficiency:** The current ROIC of {latest_roic:.2f}% indicates how effectively capital is being deployed.
                    * **Valuation:** An FCF Yield of {latest_fcf:.2f}% acts as the cash-backed floor for current valuation.
                    * **Leverage:** A D/E ratio of {latest_de:.2f} frames the survival risk of the balance sheet.
                    
                    *(To make this dynamic, hook this block up to your `scripts.ai_agent` or `scripts.model_runtime`)*
                    """)

            # --- BOTTOM SECTION: FULL DETAILS ---
            st.divider()
            st.subheader("Detailed Financial Ratios")

            if "operating_margin" in df_margin.columns:
                df_margin.insert(
                    df_margin.columns.get_loc("operating_margin") + 1,
                    "Margin (%)",
                    (df_margin["operating_margin"] * 100).apply(
                        lambda x: f"{x:.2f}%" if pd.notnull(x) else None
                    ),
                )
            if "roic" in df_roic.columns:
                df_roic.insert(
                    df_roic.columns.get_loc("roic") + 1,
                    "ROIC (%)",
                    (df_roic["roic"] * 100).apply(
                        lambda x: f"{x:.2f}%" if pd.notnull(x) else None
                    ),
                )
            if "FCF_Yield" in df_fcf.columns:
                df_fcf.insert(
                    df_fcf.columns.get_loc("FCF_Yield") + 1,
                    "Yield (%)",
                    (df_fcf["FCF_Yield"] * 100).apply(
                        lambda x: f"{x:.2f}%" if pd.notnull(x) else None
                    ),
                )
            if "rev_growth_pct" in df_dol.columns:
                df_dol.insert(
                    df_dol.columns.get_loc("rev_growth_pct") + 1,
                    "Rev Growth (%)",
                    df_dol["rev_growth_pct"].apply(
                        lambda x: f"{x:.2f}%" if pd.notnull(x) else None
                    ),
                )
            if "ebit_growth_pct" in df_dol.columns:
                df_dol.insert(
                    df_dol.columns.get_loc("ebit_growth_pct") + 1,
                    "EBIT Growth (%)",
                    df_dol["ebit_growth_pct"].apply(
                        lambda x: f"{x:.2f}%" if pd.notnull(x) else None
                    ),
                )

            colA, colB = st.columns(2)
            with colA:
                st.markdown("#### 1. Cash Conversion Cycle")
                st.dataframe(df_ccc, use_container_width=True, hide_index=True)
                st.markdown("#### 2. Quality of Earnings (CFO / PAT)")
                st.dataframe(df_cfo_pat, use_container_width=True, hide_index=True)
                st.markdown("#### 3. Operating Margin")
                st.dataframe(df_margin, use_container_width=True, hide_index=True)
                st.markdown("#### 4. Debt-to-Equity")
                st.dataframe(df_leverage, use_container_width=True, hide_index=True)
            with colB:
                st.markdown("#### 5. Return on Invested Capital")
                st.dataframe(df_roic, use_container_width=True, hide_index=True)
                st.markdown("#### 6. Live FCF Yield")
                st.dataframe(df_fcf, use_container_width=True, hide_index=True)
                st.markdown("#### 7. Degree of Operating Leverage")
                st.dataframe(df_dol, use_container_width=True, hide_index=True)


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
