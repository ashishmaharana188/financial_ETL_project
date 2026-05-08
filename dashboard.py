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

st.title("⚡ Financial ETL Control Center")
st.markdown(
    "Run the mathematically validated pipeline on target companies and monitor forensic outputs."
)

st.divider()
st.subheader("🚀 Execute ETL Batch")

col1, col2 = st.columns([3, 1])
with col1:
    ticker_input = st.text_input(
        "Target Tickers (comma-separated)", placeholder="e.g., ONGC.NS, ADANIPOWER.NS"
    )
with col2:
    selected_mode = st.radio(
        "AI Routing Engine:", ["Local (Ollama)", "Cloud (Gemini)"], horizontal=True
    )

if st.button("Start ETL Pipeline", type="primary", use_container_width=True):
    if not ticker_input.strip():
        st.warning("Please enter at least one ticker to proceed.")
    else:
        # Parse the input into a clean list
        target_tickers = [
            t.strip().upper() for t in ticker_input.split(",") if t.strip()
        ]
        mode_param = "local" if "Local" in selected_mode else "cloud"

        with st.spinner(
            f"Executing pipeline for {len(target_tickers)} client(s)... Check terminal for forensic logs."
        ):
            try:
                # Pass the array to the backend and catch the return payload
                batch_results = run_etl_pipeline(
                    target_tickers=target_tickers, ai_mode=mode_param
                )

                st.success("ETL Batch Processing Complete!")

                if batch_results:
                    # Render the clean validation matrix
                    results_df = pd.DataFrame(batch_results)
                    st.dataframe(
                        results_df,
                        column_config={
                            "Ticker": st.column_config.TextColumn("Client (Ticker)"),
                            "Status": st.column_config.TextColumn("Fetch Status"),
                            "Direct Validation": st.column_config.TextColumn(
                                "Direct Audit (BS/IS/CF)"
                            ),
                            "Indirect Validation": st.column_config.TextColumn(
                                "Indirect Audit (OCF/FCF)"
                            ),
                            "Rows Upserted": st.column_config.NumberColumn(
                                "Periods Upserted"
                            ),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.info("No data returned from the pipeline.")
            except Exception as e:
                st.error(f"Pipeline crashed during execution: {e}")
