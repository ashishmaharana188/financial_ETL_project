import streamlit as st
import pandas as pd
import json
from sqlalchemy import text
from scripts.database import engine
from scripts.model_runtime import runtime
from scripts.statementScrape import run_etl_pipeline

# ==========================================
# STREAMLIT UI - ETL CONTROL CENTER
# ==========================================

st.set_page_config(
    page_title="Financial ETL Control Center", layout="wide", page_icon="⚡"
)

st.title("Financial ETL Control Center")
st.markdown(
    "Run the mathematically validated pipeline on target companies and monitor forensic outputs."
)

st.divider()
st.subheader("Execute ETL Batch")

col1, col2 = st.columns([3, 1])
with col1:
    ticker_input = st.text_input(
        "Target Tickers (comma-separated)", placeholder="e.g., ONGC.NS, ADANIPOWER.NS"
    )
with col2:
    selected_mode = st.radio(
        "AI Routing Engine:", ["Local (Ollama)", "Cloud (Gemini)"], horizontal=True
    )

if "pipeline_results" not in st.session_state:
    st.session_state.pipeline_results = None

if st.button("Start ETL Pipeline", type="primary", use_container_width=False):
    if not ticker_input.strip():
        st.warning("Please enter at least one ticker to proceed.")
    else:
        # Parse the input into a clean list
        target_tickers = [
            t.strip().upper() for t in ticker_input.split(",") if t.strip()
        ]
        mode_param = "local" if "Local" in selected_mode else "cloud"

        with st.spinner(
            f"Executing pipeline for {len(target_tickers)} client(s)... Check terminal for logs."
        ):
            try:
                # Execute pipeline and immediately lock the results into the session state
                st.session_state.pipeline_results = run_etl_pipeline(
                    target_tickers=target_tickers, ai_mode=mode_param
                )
                st.success("ETL Batch Processing Complete!")
            except Exception as e:
                st.error(f"Pipeline crashed during execution: {e}")


if st.session_state.pipeline_results:
    batch_results = st.session_state.pipeline_results

    # 1. MAIN SUMMARY TABLE (Filter out the bulky DataPayload so it renders cleanly)
    summary_df = pd.DataFrame(
        [{k: v for k, v in res.items() if k != "DataPayload"} for res in batch_results]
    )

    st.dataframe(
        summary_df,
        column_config={
            "Ticker": st.column_config.TextColumn("Client (Ticker)"),
            "Status": st.column_config.TextColumn("Fetch Status"),
            "Direct Validation": st.column_config.TextColumn("Direct Audit (BS/IS/CF)"),
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
    st.markdown(
        "Compare the raw extracted PascalCase data directly against the final database-formatted output."
    )

    # Filter only successful fetches that actually have DataPayload attached
    valid_results = [r for r in batch_results if "DataPayload" in r]

    if valid_results:
        # Interactive Selectors
        inspector_col1, inspector_col2 = st.columns([1, 2])
        with inspector_col1:
            selected_ticker = st.selectbox(
                "Select Client:", [r["Ticker"] for r in valid_results]
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
            )

        # Map selection to dictionary keys
        if "Income" in selected_statement:
            stmt_key = "IS"
        elif "Balance" in selected_statement:
            stmt_key = "BS"
        elif "Indirect" in selected_statement:
            stmt_key = "ICF"
        else:
            stmt_key = "CF"

        # Extract the targeted client's DataFrames
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

            # Render side-by-side without lagging the DOM
            view_col1, view_col2 = st.columns(2)
            with view_col1:
                st.caption(f"**Raw Extracted Data** ({selected_ticker} - {stmt_key})")
                st.dataframe(raw_df, use_container_width=True)
            with view_col2:
                st.caption(
                    f"**Final DB-Formatted Data** ({selected_ticker} - {stmt_key})"
                )
                st.dataframe(clean_df, use_container_width=True)
