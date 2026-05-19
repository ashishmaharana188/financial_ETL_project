import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
import argparse
from scripts.database import engine

# Adjust to wherever your block/bulk files are saved (root cache or master_archives)
CACHE_DIR = "offline_data_cache/master_archives"


def clean_trade_data(df, event_type):
    """Formats NSE Bulk/Block headers to match our database schema."""
    if df.empty:
        return pd.DataFrame()

    # 1. Aggressively strip hidden characters and trailing spaces from headers
    df.columns = (
        df.columns.str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .str.upper()
    )

    # 2. Map the cleaned NSE headers to our Database Schema
    col_mapping = {
        "DATE": "ReportDate",
        "SYMBOL": "Ticker",
        "CLIENT NAME": "ClientName",
        "BUY / SELL": "TransactionType",
        "QUANTITY TRADED": "Quantity",
        "TRADE PRICE / WGHT. AVG. PRICE": "AveragePrice",
        "TRADE PRICE / WTD. AVG. PRICE": "AveragePrice",
    }

    df = df.rename(columns=col_mapping)

    required_cols = [
        "ReportDate",
        "Ticker",
        "ClientName",
        "TransactionType",
        "Quantity",
        "AveragePrice",
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(
            f"    [!] Warning: Missing columns {missing_cols} in {event_type} file. Skipping."
        )
        return pd.DataFrame()

    # 3. Invincible Date Parsing (Enforcing Day-First to prevent American format inversion)
    df["ReportDate"] = pd.to_datetime(df["ReportDate"], format="mixed", dayfirst=True)

    df["EventType"] = event_type

    # 4. Standardize Transaction Types
    df["TransactionType"] = df["TransactionType"].astype(str).str.strip().str.upper()
    df.loc[df["TransactionType"] == "B", "TransactionType"] = "BUY"
    df.loc[df["TransactionType"] == "S", "TransactionType"] = "SELL"

    # 5. Clean numbers (destroy commas)
    df["Quantity"] = pd.to_numeric(
        df["Quantity"].astype(str).str.replace(",", ""), errors="coerce"
    )
    df["AveragePrice"] = pd.to_numeric(
        df["AveragePrice"].astype(str).str.replace(",", ""), errors="coerce"
    )

    # Drop strictly corrupted rows
    df = df.dropna(subset=["ReportDate", "Ticker", "Quantity", "AveragePrice"])

    return df[required_cols + ["EventType"]]


def push_to_ledger(df):
    """Pushes the cleaned dataframe to PostgreSQL using an idempotent Clear-and-Replace strategy."""
    if df.empty:
        return

    # Format dates to string for the DB push
    df["ReportDate"] = df["ReportDate"].dt.strftime("%Y-%m-%d 00:00:00")
    unique_dates = df["ReportDate"].unique().tolist()

    print(
        f"    [DB PUSH] Upserting {len(df)} trade events across {len(unique_dates)} distinct dates..."
    )

    # Bypassing Pandas NaN for psycopg2 safety
    import math

    records = df.to_dict(orient="records")
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and math.isnan(value):
                record[key] = None

    # Idempotent "Clear and Replace" strategy
    delete_query = text("""
        DELETE FROM trade_events_ledger 
        WHERE "ReportDate" IN :dates 
          AND "EventType" IN :event_types
    """)

    insert_query = text("""
        INSERT INTO trade_events_ledger (
            "ReportDate", "Ticker", "EventType", 
            "ClientName", "TransactionType", "Quantity", "AveragePrice"
        ) VALUES (
            :ReportDate, :Ticker, :EventType, 
            :ClientName, :TransactionType, :Quantity, :AveragePrice
        )
    """)

    try:
        with engine.begin() as conn:
            event_types = df["EventType"].unique().tolist()
            # 1. Wipe existing data for these exact dates/types to prevent duplicates
            conn.execute(
                delete_query,
                {"dates": tuple(unique_dates), "event_types": tuple(event_types)},
            )

            # 2. Insert fresh data
            for record in records:
                conn.execute(insert_query, record)

        print("    [✔] SUCCESS: Trade Events Ledger safely updated.")
    except Exception as e:
        print(f"    [!] DATABASE ERROR:\n{e}")


def run_trades_etl(start_date=None, end_date=None):
    """Scans cache for Bulk and Block deals, optionally filtering by date range for Delta syncs."""
    print("=" * 50)
    print("INITIATING TRADE EVENTS PARSER (BULK & BLOCK DEALS)")
    print("=" * 50)

    # Hunt for the specific file name structures
    bulk_files = glob.glob(os.path.join(CACHE_DIR, "nse_bulk_deals_*.csv"))
    block_files = glob.glob(os.path.join(CACHE_DIR, "nse_block_deals_*.csv"))

    all_data = []
    print(f"[*] Found {len(bulk_files)} Bulk files and {len(block_files)} Block files.")

    # Process Bulk
    for file in bulk_files:
        df = pd.read_csv(file, encoding="latin1")
        cleaned = clean_trade_data(df, "Bulk Deal")
        all_data.append(cleaned)

    # Process Block
    for file in block_files:
        df = pd.read_csv(file, encoding="latin1")
        cleaned = clean_trade_data(df, "Block Deal")
        all_data.append(cleaned)

    if all_data:
        master_df = pd.concat(all_data, ignore_index=True)

        # If running in Delta Mode, filter the dataframe before pushing
        if start_date and end_date:
            s_dt = pd.to_datetime(start_date)
            e_dt = pd.to_datetime(end_date)
            master_df = master_df[
                (master_df["ReportDate"] >= s_dt) & (master_df["ReportDate"] <= e_dt)
            ]
            print(
                f"[*] Delta Mode: Filtered to {len(master_df)} events between {start_date} and {end_date}."
            )

        push_to_ledger(master_df)
    else:
        print("[-] No valid trade events data found to parse.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    run_trades_etl(args.start, args.end)
