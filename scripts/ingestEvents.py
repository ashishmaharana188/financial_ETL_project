import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from scripts.database import engine
import re
import io
import uuid
from sqlalchemy.dialects.postgresql import insert

CACHE_DIR = "offline_data_cache/master_archives"


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


def push_chunk_to_db(df):
    if df.empty:
        return

    # 1. Exact Match to your SQLAlchemy Model (Case-Sensitive)
    final_columns = [
        "ReportDate",
        "Ticker",
        "EventType",
        "SecurityName",
        "ClientName",
        "TransactionType",
        "Quantity",
        "TradePrice",
        "Remarks",
    ]

    for col in final_columns:
        if col not in df.columns:
            df[col] = None

    df = df[final_columns]
    df = clean_for_db(df)

    # 2. In-memory deduplication
    pk_cols = [
        "ReportDate",
        "Ticker",
        "ClientName",
        "TransactionType",
        "Quantity",
        "TradePrice",
    ]
    df = df.drop_duplicates(subset=pk_cols, keep="last")

    print(f"    -> [BULK PUSH] Upserting {len(df)} rows into 'trade_events_ledger'...")

    # Notice: NO lowercasing hack here. We leave the DataFrame columns as CamelCase.

    # 3. Define the Native PostgreSQL Upsert Logic
    def postgres_upsert(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_stmt = insert(table.table).values(data)

        upsert_stmt = insert_stmt.on_conflict_do_nothing(
            constraint="unique_trade_event"
        )
        conn.execute(upsert_stmt)

    # 4. Execute using native Pandas integration
    try:
        df.to_sql(
            "trade_events_ledger",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=5000,
            method=postgres_upsert,
        )
    except Exception as e:
        print(f"    [-] CRITICAL Bulk Push Error: {e}")


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
