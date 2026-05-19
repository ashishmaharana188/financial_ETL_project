import streamlit as st
import pandas as pd
from sqlalchemy import text
from scripts.database import engine
from scripts.statementScrape import run_etl_pipeline
from scripts.macroScrape import run_macro_pipeline, register_discovered_tickers
import subprocess
import sys

# Import new isolated UI modules
from scripts.engines.companyMetrics import render_company_metrics
from scripts.engines.olsEngine1 import render_ols_engine

# Import fetchers for the Market Overview (since it still calculates cross-sectional data)
from scripts.ratioAnalysis import fetch_roic, fetch_fcf_yield

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
    ["Macro Data", "Company Data", "Engines", "Market Overview"],
    key="main_nav_radio",
)

# --- NEW: Contextual Engine Sidebar Dropdown ---
engine_view = None
if app_mode == "Engines":
    st.sidebar.divider()
    st.sidebar.markdown("### Engine View Options")
    engine_view = st.sidebar.selectbox(
        "Select Active Engine:",
        ["OLS Engine 1", "Canvas Mode (All Engines)"],
        help="Select a specific engine or render all engines simultaneously on the canvas.",
    )

st.sidebar.divider()
st.sidebar.markdown("### Global Settings")
selected_source = st.sidebar.selectbox(
    "Primary Data Source",
    options=["vantage", "yfinance", "screener"],
    index=0,
    help="Strictly isolates all mathematical models to data provided by this specific spigot.",
)

st.sidebar.divider()
st.sidebar.caption("System Status: Online")
st.sidebar.caption("Database: PostgreSQL Connected")


def run_orchestrator(mode, start=None, end=None):
    cmd = [
        sys.executable,
        "-u",
        "-m",
        "scripts.downloadOrchestrator",
        "--mode",
        mode,
    ]
    if start and end:
        cmd.extend(["--start", start, "--end", end])
    subprocess.run(cmd)


# =====================================================================
# 1. MACRO DATA CENTER
# =====================================================================
if app_mode == "Macro Data":
    st.title("Macro Data Center")
    st.markdown(
        "Dedicated space for macro scraping, delta bridge, and data management."
    )

    st.subheader("Database & File Orchestration")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**1. Daily Catch-Up**")
        if st.button("Run Delta Bridge", use_container_width=False):
            with st.spinner("Executing Delta Bridge..."):
                run_orchestrator("delta")
            st.success("Delta Sync complete!")

    with col2:
        st.markdown("**2. Master DB Sync**")
        if st.button("Run Master Parse Sync", use_container_width=False):
            with st.spinner("Executing Master DB Parse..."):
                run_orchestrator("parse_all")
            st.success("Master Parse complete!")

    with col3:
        st.markdown("**3. File Scraper Engine**")
        scrape_dates = st.date_input(
            "Optional Custom Date Range (Leave default for full timeline)",
            value=(),
            key="scrape_dates",
        )

        if st.button("Run Scrapers", use_container_width=False):
            with st.spinner("Executing Scrapers..."):
                if len(scrape_dates) == 2:
                    start_str = scrape_dates[0].strftime("%Y-%m-%d")
                    end_str = scrape_dates[1].strftime("%Y-%m-%d")
                    run_orchestrator("scrape", start=start_str, end=end_str)
                else:
                    run_orchestrator("scrape")
            st.success("Scraping complete! Files saved to cache.")

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

# =====================================================================
# 2. COMPANY DATA CENTER
# =====================================================================
elif app_mode == "Company Data":
    st.title("Company Data Center")
    st.markdown(
        "Run the mathematically validated pipeline on target companies and monitor forensic outputs."
    )

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

    st.caption("Data Spigot Configuration")
    selected_spigot = st.radio(
        "Select Primary Data Source (Auto-Rotate highly recommended to bypass API limits):",
        [
            "Auto-Rotate (FMP -> Alpha Vantage)",
            "Financial Modeling Prep (FMP)",
            "Alpha Vantage (Strictly US)",
            "IndianAPI (Strictly India)",
            "Yahoo Finance",
            "Screener.in",
        ],
        horizontal=True,
        key="etl_spigot_radio",
    )

    spigot_map = {
        "Auto-Rotate (FMP -> Alpha Vantage)": "auto",
        "Financial Modeling Prep (FMP)": "fmp",
        "Alpha Vantage (Strictly US)": "vantage",
        "IndianAPI (Strictly India)": "indianapi",
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
                    register_discovered_tickers(target_tickers)
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

            stmt_key = (
                "IS"
                if "Income" in selected_statement
                else (
                    "BS"
                    if "Balance" in selected_statement
                    else "ICF" if "Indirect" in selected_statement else "CF"
                )
            )

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

# =====================================================================
# 3. QUANTITATIVE ENGINES (MODULAR VIEW)
# =====================================================================
elif app_mode == "Engines":
    st.title("Quantitative Engines & Deep Dive")

    try:
        with engine.connect() as conn:
            tickers_df = pd.read_sql(
                'SELECT "Ticker" FROM market_metadata WHERE "IsActive" = TRUE', conn
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

        # -----------------------------------------------------------------
        # MODULAR DELEGATION: MODULE 1 (ALWAYS RENDERS FIRST)
        # -----------------------------------------------------------------
        engine_data = render_company_metrics(
            selected_db_ticker, selected_source, view_mode, edgar_break_date, engine
        )

        st.divider()

        # -----------------------------------------------------------------
        # MODULAR DELEGATION: ENGINE RENDERING
        # Uses the Dropdown selection from the Sidebar
        # -----------------------------------------------------------------
        if engine_view == "OLS Engine 1":
            render_ols_engine(
                company_sector,
                company_industry,
                engine_data["df_op_margin"],
                engine_data["df_gr_margin"],
                engine_data["df_int_cov"],
                engine_data["df_turnover"],
                engine_data["df_fcf_margin"],
                engine_data["df_rev_growth"],
                engine,
            )

        elif engine_view == "Canvas Mode (All Engines)":
            st.markdown("## 🎨 Canvas Mode: Master Engine View")
            st.caption("Rendering all active quantitative engines simultaneously.")

            st.markdown("---")
            render_ols_engine(
                company_sector,
                company_industry,
                engine_data["df_op_margin"],
                engine_data["df_gr_margin"],
                engine_data["df_int_cov"],
                engine_data["df_turnover"],
                engine_data["df_fcf_margin"],
                engine_data["df_rev_growth"],
                engine,
            )

            # FUTURE ENGINES:
            # st.markdown("---")
            # render_dcf_engine(engine_data...)
            # st.markdown("---")
            # render_options_pricing_engine(engine_data...)

# =====================================================================
# 4. MARKET OVERVIEW
# =====================================================================
elif app_mode == "Market Overview":
    st.title("Market Overview")
    st.markdown("Cross-sectional ranking and quadrant analysis.")

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
                market_data = []
                for _, row in target_companies.iterrows():
                    t = row["Ticker"]
                    c_name = row["CompanyName"]

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

                    st.scatter_chart(
                        market_df,
                        x="FCF Yield (%)",
                        y="ROIC (%)",
                        color="Ticker",
                        height=500,
                    )

                    st.subheader("Sector Leaderboard")
                    leaderboard = market_df.sort_values(
                        by="ROIC (%)", ascending=False
                    ).reset_index(drop=True)
                    st.dataframe(leaderboard, use_container_width=True)
                else:
                    st.warning(
                        "Not enough clean ratio data to generate the quadrant map."
                    )
