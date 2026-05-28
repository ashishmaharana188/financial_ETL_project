import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from scripts.database import engine
import re
import io
import uuid
import logging

CACHE_DIR = "offline_data_cache/master_archives"


# Route to the shared ingestion audit log
logging.basicConfig(
    filename="ingestion_audit.log",
    level=logging.INFO,
    format="%(asctime)s | FILE: %(file_name)-40s | RAW: %(raw)-8s | DEDUP: %(dedup)-8s | PUSH: %(push)-8s | STATUS: %(status)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)


def log_audit(file_name, raw, dedup, push, status):
    logging.info(
        "",
        extra={
            "file_name": os.path.basename(str(file_name)),
            "raw": raw,
            "dedup": dedup,
            "push": push,
            "status": status,
        },
    )


def clean_for_db(df):
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    return df


def parse_trade_events(file_path, event_type):
    df = pd.read_csv(file_path, encoding="latin1")
    df.columns = (
        df.columns.str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    if df.empty:
        return pd.DataFrame()

    df["ReportDate"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")
    df = df.dropna(subset=["ReportDate"])
    df["EventType"] = event_type

    df["Quantity Traded"] = (
        df["Quantity Traded"].astype(str).str.replace(",", "", regex=False)
    )
    df["Quantity Traded"] = pd.to_numeric(df["Quantity Traded"], errors="coerce")

    price_col = "Trade Price / Wght. Avg. Price"
    if price_col not in df.columns:
        price_col = "Trade Price / Wght. Avg. Price "

    if price_col in df.columns:
        df[price_col] = df[price_col].astype(str).str.replace(",", "", regex=False)
        df["TradePrice"] = pd.to_numeric(df[price_col], errors="coerce")
    else:
        df["TradePrice"] = None

    df = df.rename(
        columns={
            "Symbol": "Ticker",
            "Security Name": "SecurityName",
            "Client Name": "ClientName",
            "Buy / Sell": "TransactionType",
            "Quantity Traded": "Quantity",
            "Remarks": "Remarks",
        }
    )
    return df


def push_chunk_to_db(df, file_name):
    raw_count = len(df)

    # Register zero-copy DataFrame in DuckDB
    engine.register("temp_events", df)

    print(
        f"\\n[DB PUSH] Upserting {raw_count} rows from {file_name} into 'trade_events_ledger'..."
    )

    try:
        # Native DuckDB Insert or Ignore
        # Bypasses existing duplicates based on the UNIQUE constraint defined in database.py
        engine.execute("""
            INSERT OR IGNORE INTO trade_events_ledger (
                "ReportDate", "Ticker", "EventType", "SecurityName", 
                "ClientName", "TransactionType", "Quantity", "TradePrice", "Remarks"
            )
            SELECT 
                "ReportDate", "Ticker", "EventType", "SecurityName", 
                "ClientName", "TransactionType", "Quantity", "TradePrice", "Remarks"
            FROM temp_events;
        """)

        # Cleanup
        engine.unregister("temp_events")

        log_audit(file_name, raw_count, raw_count, raw_count, "SUCCESS")
        print(f"  [+] Success.")

    except Exception as e:
        error_msg = str(e).replace("\\n", " ")[:80]
        log_audit(file_name, raw_count, raw_count, 0, f"FAILED: {error_msg}")
        print(f"  [X] Failed: {error_msg}")
        engine.unregister("temp_events")


def execute_events_pipeline(start_date_str="1900-01-01"):

    resume_date = pd.to_datetime(start_date_str).date()

    bulk_files = glob.glob(os.path.join(CACHE_DIR, "nse_bulk_deals_*.csv"))
    block_files = glob.glob(os.path.join(CACHE_DIR, "nse_block_deals_*.csv"))

    print(
        f"[*] Discovered Data: {len(bulk_files)} Bulk files | {len(block_files)} Block files."
    )
    print(f"[*] Resume Checkpoint: {resume_date}")

    def extract_end_date(filepath):
        """Looks for the end date in names like: nse_bulk_deals_09-05-2026_to_20-05-2026.csv"""
        match = re.search(r"_to_(\d{2}-\d{2}-\d{4})\.csv", filepath)
        if match:
            return pd.to_datetime(
                match.group(1), format="%d-%m-%Y", errors="coerce"
            ).date()
        return pd.to_datetime("2099-12-31").date()

    all_dataframes = []

    # Process Bulk Deals
    for idx, file in enumerate(bulk_files, 1):
        if extract_end_date(file) < resume_date:
            continue

        print(
            f"[*] Parsing Bulk Deal [ {idx} / {len(bulk_files)} ] : {os.path.basename(file)}"
        )
        parsed_df = parse_trade_events(file, "Bulk Deal")
        if not parsed_df.empty:
            all_dataframes.append(parsed_df)
            log_audit(file, len(parsed_df), len(parsed_df), 0, "PARSED_IN_MEMORY")
        else:
            log_audit(file, 0, 0, 0, "SKIPPED_EMPTY")

    # Process Block Deals
    for idx, file in enumerate(block_files, 1):
        if extract_end_date(file) < resume_date:
            continue

        print(
            f"[*] Parsing Block Deal [ {idx} / {len(block_files)} ] : {os.path.basename(file)}"
        )
        parsed_df = parse_trade_events(file, "Block Deal")
        if not parsed_df.empty:
            all_dataframes.append(parsed_df)
            log_audit(file, len(parsed_df), len(parsed_df), 0, "PARSED_IN_MEMORY")
        else:
            log_audit(file, 0, 0, 0, "SKIPPED_EMPTY")

    if not all_dataframes:
        print("  No new trade events to process. DB is up to date.")
        return

    master_events_df = pd.concat(all_dataframes, ignore_index=True)
    master_events_df = master_events_df[
        master_events_df["ReportDate"].dt.date >= resume_date
    ]

    if master_events_df.empty:
        print("  All parsed events are older than the resume checkpoint.")
        return

    push_chunk_to_db(master_events_df)
    print("[SUCCESS] Trade Events Ledger Update Complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="1900-01-01")
    args = parser.parse_args()
    execute_events_pipeline(args.start)
