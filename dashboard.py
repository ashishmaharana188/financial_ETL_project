import streamlit as st
import pandas as pd
import json
from sqlalchemy import text
from datetime import datetime

# Import your database engine (adjust the import path if your engine is named differently)
from scripts.database import engine


def approve_ai_suggestion(
    ticket_id,
    bucket_name,
    new_keys,
    target_index=None,
    config_path="mapping_config.json",
):
    """
    Injects the AI's mathematically verified keys into the JSON config.
    """
    try:
        # 1. Load the current config
        with open(config_path, "r") as f:
            config = json.load(f)

        # 2. Convert the database string back into a Python list
        keys_to_add = json.loads(new_keys) if isinstance(new_keys, str) else new_keys
        target_map = config["normalized_indirect_cf_synonym_map"]

        if bucket_name in target_map:
            # 3. Inject the keys
            if target_index is not None and target_index != "Create New Group":
                # Append to a specific existing sub-array
                target_map[bucket_name][int(target_index)].extend(keys_to_add)
            else:
                # Create a brand new sub-array
                target_map[bucket_name].append(keys_to_add)

            # 4. Save the updated config back to disk
            with open(config_path, "w") as f:
                json.dump(config, f, indent=4)

            # 5. Close the loop: Mark the database ticket as APPROVED
            with engine.connect() as conn:
                conn.execute(
                    text(
                        'UPDATE ai_forensic_logs SET "Status" = \'APPROVED\' WHERE "TicketID" = :tid'
                    ),
                    {"tid": ticket_id},
                )
                conn.commit()
            return True, f"Successfully updated {bucket_name}"
        else:
            return False, f"Bucket {bucket_name} not found."

    except Exception as e:
        return False, str(e)


# --- STREAMLIT UI ---

st.title("AI Forensic Approval Queue")


# Helper to load config for the dropdowns
def load_config():
    with open("mapping_config.json", "r") as f:
        return json.load(f)


# Fetch pending tickets from your database
pending_logs = pd.read_sql(
    "SELECT * FROM ai_forensic_logs WHERE \"Status\" = 'PENDING'", engine
)
config = load_config()

if not pending_logs.empty:
    for _, row in pending_logs.iterrows():
        with st.expander(f"Ticket: {row['Ticker']} - {row['LeakType']}"):
            st.write(f"**Leak Amount:** {row['LeakAmount']}")
            st.write(f"**Suggested Category:** `{row['SuggestedCategory']}`")
            st.write(f"**Keys to Add:** `{row['MissingKeyFound']}`")
            st.info(f"Reasoning: {row['Reasoning']}")

            # Get the existing sub-arrays for this category to populate the dropdown
            category = row["SuggestedCategory"]
            existing_groups = config["normalized_indirect_cf_synonym_map"].get(
                category, []
            )

            # Create a list of options for the user
            options = ["Create New Group"]
            for i, group in enumerate(existing_groups):
                options.append(f"Append to Group {i}: {group}")

            # The dropdown selection
            selected_option = st.selectbox(
                "Where should this key go?", options, key=f"select_{row['TicketID']}"
            )

            if st.button("Approve & Inject", key=f"btn_{row['TicketID']}"):
                # Figure out the index based on the selection
                target_index = (
                    None
                    if selected_option == "Create New Group"
                    else options.index(selected_option) - 1
                )

                success, msg = approve_ai_suggestion(
                    row["TicketID"], category, row["MissingKeyFound"], target_index
                )
                if success:
                    st.success(msg)
                    st.rerun()  # Refreshes the UI to clear the approved ticket
                else:
                    st.error(msg)
else:
    st.write("No pending AI suggestions. Everything is balanced!")
