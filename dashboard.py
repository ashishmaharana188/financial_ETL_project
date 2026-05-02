import streamlit as st
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import text
from scripts.database import engine

# CONFIGURATION
st.set_page_config(layout="wide", page_title="AI Forensic Dashboard")
BASE_DIR = Path(__file__).resolve().parent
config_file_path = BASE_DIR / "mapping_config.json"


#  HELPER FUNCTIONS
def load_logs():
    """Fetches the latest tickets from PostgreSQL"""
    query = 'SELECT * FROM ai_forensic_logs ORDER BY "Timestamp" DESC'
    return pd.read_sql(query, engine)


def load_dictionary():
    """Loads the current mapping rules"""
    with open(config_file_path, "r") as f:
        return json.load(f)


def update_dictionary(category, new_key):
    """Safely writes a new key to the JSON dictionary"""
    config = load_dictionary()

    # Search all major dictionaries to find the target category
    updated = False
    for dict_name in [
        "ittelson_income_statement_columns",
        "ittelson_balance_sheet_columns",
        "indirect_cash_flow_columns",
    ]:
        if category in config.get(dict_name, {}):
            if new_key not in config[dict_name][category]:
                config[dict_name][category].append(new_key)
                updated = True
            break

    if updated:
        with open(config_file_path, "w") as f:
            json.dump(config, f, indent=4)
        return True
    return False


def resolve_ticket(ticket_id):
    """Marks a ticket as RESOLVED in the database"""
    with engine.begin() as conn:
        conn.execute(
            text(
                'UPDATE ai_forensic_logs SET "Status" = \'RESOLVED\' WHERE "TicketID" = :tid'
            ),
            {"tid": ticket_id},
        )


# UI: THE TICKET DASHBOARD
st.title("🕵️‍♂️ AI Forensic Accounting Dashboard")
st.markdown(
    "Review unmapped accounting lines detected by the ETL pipeline and add them to the global dictionary."
)

logs_df = load_logs()

if logs_df.empty:
    st.success("No leaks detected! The dictionary is perfectly mapped.")
else:
    # Separate pending and resolved
    pending_df = logs_df[logs_df["Status"] == "PENDING"]
    resolved_df = logs_df[logs_df["Status"] == "RESOLVED"]

    st.subheader(f"Pending Audits ({len(pending_df)})")
    st.dataframe(
        pending_df[
            [
                "Timestamp",
                "Ticker",
                "LeakType",
                "LeakAmount",
                "MissingKeyFound",
                "SuggestedCategory",
                "Reasoning",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    # UI: THE DICTIONARY MANAGER
    st.divider()
    st.subheader("Update Global Dictionary")

    if not pending_df.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            # Dropdown populated by the AI's findings
            selected_ticket = st.selectbox(
                "Select a Ticket to Resolve:", pending_df["TicketID"].tolist()
            )

            if selected_ticket:
                ticket_data = pending_df[
                    pending_df["TicketID"] == selected_ticket
                ].iloc[0]
                target_key = ticket_data["MissingKeyFound"]
                suggested_cat = ticket_data["SuggestedCategory"]

        with col2:
            st.text_input("Discovered Line Item (Key)", value=target_key, disabled=True)

        with col3:
            # Allow the user to override the AI's category suggestion if needed
            final_category = st.text_input(
                "Target Dictionary Category", value=suggested_cat
            )

        if st.button("Approve & Update Dictionary", type="primary"):
            if update_dictionary(final_category, target_key):
                resolve_ticket(selected_ticket)
                st.success(f"Added '{target_key}' to '{final_category}'!")
                st.rerun()  # Refresh the UI state
            else:
                st.error(
                    f"Category '{final_category}' not found in mapping_config.json. Please check spelling."
                )

    # RESOLVED LOGS
    with st.expander("View Resolved Tickets"):
        st.dataframe(resolved_df, use_container_width=True)
