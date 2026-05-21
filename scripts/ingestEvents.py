import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from scripts.database import engine  # Ensure this points to your SQLAlchemy engine
import re

CACHE_DIR = "offline_data_cache/master_archives"


def clean_for_db(df):
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    return df


def parse_trade_events(file_path, event_type):
    # Read CSV (using latin1 encoding as NSE files sometimes contain hidden characters)
    df = pd.read_csv(file_path, encoding="latin1")

    # Aggressively clean header names (strip spaces and invisible characters)
    df.columns = (
        df.columns.str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

    # Ensure dataframe is not empty
    if df.empty:
        return pd.DataFrame()

    df["ReportDate"] = pd.to_datetime(df["Date"], format="%d-%b-%Y")

    df["EventType"] = event_type

    df["Quantity Traded"] = (
        df["Quantity Traded"].astype(str).str.replace(",", "", regex=False)
    )
    df["Quantity Traded"] = pd.to_numeric(df["Quantity Traded"], errors="coerce")

    price_col = "Trade Price / Wght. Avg. Price"
    if price_col not in df.columns:
        price_col = "Trade Price / Wtd. Avg. Price"  # Alternative NSE spelling

    df[price_col] = df[price_col].astype(str).str.replace(",", "", regex=False)
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")

    # 4. Map directly to our Database Schema
    col_mapping = {
        "Symbol": "Ticker",
        "Security Name": "Security_Name",
        "Client Name": "Client_Name",
        "Buy / Sell": "Transaction_Type",
        "Quantity Traded": "Quantity",
        price_col: "Trade_Price",
        "Remarks": "Remarks",
    }

    df = df.rename(columns=col_mapping)

    # Ensure Remarks column exists even if missing in file
    if "Remarks" not in df.columns:
        df["Remarks"] = None
    else:
        # Replace empty dashes '-' with actual Nulls
        df["Remarks"] = df["Remarks"].replace("-", None)

    return df


def execute_events_pipeline(start_date_str="1900-01-01"):
    resume_date = pd.to_datetime(start_date_str).date()

    bulk_files = glob.glob(os.path.join(CACHE_DIR, "nse_bulk_deals_*.csv"))
    block_files = glob.glob(os.path.join(CACHE_DIR, "nse_block_deals_*.csv"))

    total_bulk = len(bulk_files)
    total_block = len(block_files)

    print(f"[*] Discovered Data: {total_bulk} Bulk files | {total_block} Block files.")
    print(f"[*] Resume Checkpoint: {resume_date}")

    def extract_date(filepath):
        match = re.search(r"(\d{8})", filepath)
        if match:
            return pd.to_datetime(match.group(1), format="%d%m%Y").date()
        return pd.to_datetime("1900-01-01").date()

    bulk_files = sorted(bulk_files, key=extract_date)
    block_files = sorted(block_files, key=extract_date)

    all_dataframes = []

    # Process Bulk Deals
    for idx, file in enumerate(bulk_files, 1):
        file_date = extract_date(file)

        # INCREMENTAL SKIP LOGIC
        if file_date < resume_date:
            continue

        print(
            f"[*] Parsing Bulk Deal [ {idx} / {total_bulk} ] : {os.path.basename(file)}"
        )
        parsed_df = parse_trade_events(file, "Bulk Deal")
        if not parsed_df.empty:
            all_dataframes.append(parsed_df)

    # Process Block Deals
    for idx, file in enumerate(block_files, 1):
        file_date = extract_date(file)

        if file_date < resume_date:
            continue

        print(
            f"[*] Parsing Block Deal [ {idx} / {total_block} ] : {os.path.basename(file)}"
        )
        parsed_df = parse_trade_events(file, "Block Deal")
        if not parsed_df.empty:
            all_dataframes.append(parsed_df)

    # Combine into the Ledger
    if not all_dataframes:
        print("[-] No trade events found to process (or all skipped). Aborting.")
        return

    master_events_df = pd.concat(all_dataframes, ignore_index=True)

    final_columns = [
        "ReportDate",
        "Ticker",
        "EventType",
        "Security_Name",
        "Client_Name",
        "Transaction_Type",
        "Quantity",
        "Trade_Price",
        "Remarks",
    ]

    master_events_df = master_events_df[final_columns]
    master_events_df = clean_for_db(master_events_df)

    print(
        f"\n[DB PUSH] Loading {len(master_events_df)} rows into 'trade_events_ledger'..."
    )
    master_events_df.to_sql(
        "trade_events_ledger",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=10000,
        method="multi",
    )
    print("[SUCCESS] Trade Events Ledger Update Complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="1900-01-01")
    args = parser.parse_args()

    execute_events_pipeline(args.start)
