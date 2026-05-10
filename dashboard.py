import streamlit as st
import pandas as pd
import json
from sqlalchemy import text
from scripts.database import engine
from scripts.model_runtime import runtime
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

###################################
st.set_page_config(
    page_title="Financial Intelligence Platform", layout="wide", page_icon="⚡"
)

st.title("Financial Intelligence Platform")

# Separate the App into Two distinct Tabs
tab_etl, tab_history = st.tabs(
    ["ETL Control Center", "Swarm Ratio Engine (Database View)"]
)

# ---------------------------------------------------------
# TAB 1: ETL CONTROL CENTER (Existing Pipeline)
# ---------------------------------------------------------
with tab_etl:
    st.markdown(
        "Run the mathematically validated pipeline on target companies and monitor forensic outputs."
    )

    st.divider()
    st.subheader("Execute ETL Batch")

    col1, col2 = st.columns([3, 1])
    with col1:
        ticker_input = st.text_input(
            "Target Tickers (comma-separated)",
            placeholder="e.g., ONGC.NS, ADANIPOWER.NS",
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
        st.subheader("🔍 Deep Dive Data Inspector")
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
                    st.caption(
                        f"**Raw Extracted Data** ({selected_ticker} - {stmt_key})"
                    )
                    st.dataframe(raw_df, use_container_width=True)
                with view_col2:
                    st.caption(
                        f"**Final DB-Formatted Data** ({selected_ticker} - {stmt_key})"
                    )
                    st.dataframe(clean_df, use_container_width=True)


# ---------------------------------------------------------
# TAB 2: RATIO ANALYSIS ENGINE (Database History)
# ---------------------------------------------------------
with tab_history:
    st.markdown(
        "Query the PostgreSQL database to analyze all 7 Swarm-validated financial ratios."
    )

    # Dynamically fetch unique tickers currently stored in the Postgres Database
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
        st.info(
            "No data available in the database. Please run the ETL pipeline in the Control Center first."
        )
    else:
        db_col1, db_col2 = st.columns([1, 3])
        with db_col1:
            selected_db_ticker = st.selectbox(
                "Select Stored Company:",
                available_db_tickers,
                key="history_db_ticker_selectbox",
            )
            run_analysis_btn = st.button(
                "Run Quantitative Analysis",
                type="primary",
                key="history_run_analysis_btn",
            )

        if run_analysis_btn:
            with st.spinner(f"Running Swarm Ratio Engine for {selected_db_ticker}..."):

                # Fetch All 7 Ratios via SQL
                df_ccc = fetch_ccc(selected_db_ticker)
                df_cfo_pat = fetch_cfo_to_pat(selected_db_ticker)
                df_margin = fetch_operating_margin(selected_db_ticker)
                df_leverage = fetch_debt_to_equity(selected_db_ticker)
                df_roic = fetch_roic(selected_db_ticker)
                df_fcf = fetch_fcf_yield(selected_db_ticker)
                df_dol = fetch_dol(selected_db_ticker)

                # ==========================================
                # ADD PERCENTAGE COLUMNS FOR UI DISPLAY
                # ==========================================

                # 3. Operating Margin
                if "operating_margin" in df_margin.columns:
                    df_margin.insert(
                        df_margin.columns.get_loc("operating_margin") + 1,
                        "Margin (%)",
                        (df_margin["operating_margin"] * 100).apply(
                            lambda x: f"{x:.2f}%" if pd.notnull(x) else None
                        ),
                    )

                # 5. ROIC
                if "roic" in df_roic.columns:
                    df_roic.insert(
                        df_roic.columns.get_loc("roic") + 1,
                        "ROIC (%)",
                        (df_roic["roic"] * 100).apply(
                            lambda x: f"{x:.2f}%" if pd.notnull(x) else None
                        ),
                    )

                # 6. FCF Yield
                if "FCF_Yield" in df_fcf.columns:
                    df_fcf.insert(
                        df_fcf.columns.get_loc("FCF_Yield") + 1,
                        "Yield (%)",
                        (df_fcf["FCF_Yield"] * 100).apply(
                            lambda x: f"{x:.2f}%" if pd.notnull(x) else None
                        ),
                    )

                # 7. DOL (Growth rates are already * 100 from SQL, just add the string formatting)
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

                # ==========================================
                # RENDER DASHBOARD (Ordered 1 to 7)
                # ==========================================
                st.divider()
                st.subheader(f"Quantitative Blueprint: {selected_db_ticker}")

                # Render the DataFrames natively in Streamlit
                colA, colB = st.columns(2)

                with colA:
                    st.markdown("#### 1. Cash Conversion Cycle (CCC)")
                    st.dataframe(df_ccc, use_container_width=True, hide_index=True)

                    st.markdown("#### 2. Quality of Earnings (CFO / PAT)")
                    st.dataframe(df_cfo_pat, use_container_width=True, hide_index=True)

                    st.markdown("#### 3. Operating Margin")
                    st.dataframe(df_margin, use_container_width=True, hide_index=True)

                    st.markdown("#### 4. Debt-to-Equity")
                    st.dataframe(df_leverage, use_container_width=True, hide_index=True)

                with colB:
                    st.markdown("#### 5. Return on Invested Capital (ROIC)")
                    st.dataframe(df_roic, use_container_width=True, hide_index=True)

                    st.markdown("#### 6. Live FCF Yield")
                    st.caption("*(Uses Real-Time Live Market Cap via yfinance)*")
                    st.dataframe(df_fcf, use_container_width=True, hide_index=True)

                    st.markdown("#### 7. Degree of Operating Leverage (DOL)")
                    st.dataframe(df_dol, use_container_width=True, hide_index=True)
