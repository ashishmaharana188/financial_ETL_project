import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
import argparse
from scripts.databasedatabase import engine
import math
import re

# Adjust to wherever your macro files are saved (root cache or master_archives)
CACHE_DIR = "offline_data_cache/master_archives"

# ==========================================
# 1. PARSING LOGIC
# ==========================================


def parse_fiidii_cash(file_path):
    """Parses the FII/DII Cash Master File."""
    try:
        df = pd.read_csv(file_path, encoding="latin1")
        df.columns = df.columns.str.strip().str.lower()

        # Ensure we have the basic expected columns
        if "date" not in df.columns or "fii_net" not in df.columns:
            print(f"    [-] Skipping {file_path} - Missing expected columns.")
            return pd.DataFrame()

        # Rename to match database schema exactly
        col_mapping = {
            "date": "ReportDate",
            "fii_buy": "FII_Buy_Value",
            "fii_sell": "FII_Sell_Value",
            "fii_net": "FII_Net_Value",
            "dii_buy": "DII_Buy_Value",
            "dii_sell": "DII_Sell_Value",
            "dii_net": "DII_Net_Value",
            "nifty_close": "Nifty_Close",
        }

        df = df.rename(columns=col_mapping)

        # Format Date safely
        df["ReportDate"] = pd.to_datetime(
            df["ReportDate"], format="mixed", dayfirst=True
        ).dt.strftime("%Y-%m-%d 00:00:00")

        # Clean numeric columns
        numeric_cols = [
            "FII_Buy_Value",
            "FII_Sell_Value",
            "FII_Net_Value",
            "DII_Buy_Value",
            "DII_Sell_Value",
            "DII_Net_Value",
            "Nifty_Close",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""), errors="coerce"
                )

        return df
    except Exception as e:
        print(f"[-] Error parsing FII Cash {file_path}: {e}")
        return pd.DataFrame()


def parse_participant_oi(file_path):
    """Parses the NSE Participant Wise OI File."""
    try:
        # NSE puts a description text in row 0, so we MUST skip row 0 to hit the real headers
        df = pd.read_csv(file_path, encoding="latin1", skiprows=1)

        # Aggressively clean headers (remove invisible characters and standardize to lower case)
        df.columns = (
            df.columns.str.replace("ï»¿", "", regex=False)
            .str.replace("\t", "", regex=False)
            .str.strip()
            .str.lower()
        )

        # The NSE sometimes changes case, so mapping via lower case is safer
        if "client type" not in df.columns:
            print(f"    [-] Skipping {file_path} - Missing 'Client Type' column.")
            return pd.DataFrame()

        # Extract date from filename (e.g., nse_part_oi_28122023.csv)
        import re

        match = re.search(r"(\d{8})", file_path)
        if match:
            date_str = pd.to_datetime(match.group(1), format="%d%m%Y").strftime(
                "%Y-%m-%d 00:00:00"
            )
        else:
            print(
                f"    [-] Skipping {file_path} - Could not extract date from filename."
            )
            return pd.DataFrame()

        # Add the date column
        df["ReportDate"] = date_str

        # Rename columns to match DB schema exactly
        col_mapping = {
            "client type": "ClientType",
            "future index long": "FutureIndexLong",
            "future index short": "FutureIndexShort",
            "future stock long": "FutureStockLong",
            "future stock short": "FutureStockShort",
            "option index call long": "OptionIndexCallLong",
            "option index put long": "OptionIndexPutLong",
            "option index call short": "OptionIndexCallShort",
            "option index put short": "OptionIndexPutShort",
            "option stock call long": "OptionStockCallLong",
            "option stock put long": "OptionStockPutLong",
            "option stock call short": "OptionStockCallShort",
            "option stock put short": "OptionStockPutShort",
            "total long contracts": "TotalLongContracts",
            "total short contracts": "TotalShortContracts",
        }

        df = df.rename(columns=col_mapping)

        # Keep only valid ClientTypes (Client, DII, FII, Pro)
        valid_clients = ["Client", "DII", "FII", "Pro"]
        df["ClientType"] = df["ClientType"].astype(str).str.strip().str.capitalize()
        df["ClientType"] = df["ClientType"].replace({"Dii": "DII", "Fii": "FII"})
        df = df[df["ClientType"].isin(valid_clients)]

        # Explicitly drop any garbage columns like 'Unnamed: 15'
        expected_cols = list(col_mapping.values()) + ["ReportDate"]
        df = df[[col for col in expected_cols if col in df.columns]]

        # THE FIX: Destroy commas and decimals, convert to pure numbers for Postgres BigInt
        numeric_cols = [
            col for col in df.columns if col not in ["ClientType", "ReportDate"]
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ""), errors="coerce"
            )

        return df
    except Exception as e:
        print(f"[-] Error parsing Participant OI {file_path}: {e}")
        return pd.DataFrame()


# ==========================================
# 2. DATABASE PUSH LOGIC (Idempotent)
# ==========================================


def push_to_db(df, table_name, conflict_cols, update_cols):
    """Generic function to push data safely bypassing Postgres NaN issues."""
    if df.empty:
        return

    print(f"    [DB PUSH] Upserting {len(df)} rows into {table_name}...")

    # Bypassing Pandas NaN for psycopg2 safety
    records = df.to_dict(orient="records")
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and math.isnan(value):
                record[key] = None

    # Dynamically build the ON CONFLICT DO UPDATE query
    conflict_str = ", ".join([f'"{c}"' for c in conflict_cols])
    set_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
    cols_str = ", ".join([f'"{c}"' for c in df.columns])
    vals_str = ", ".join([f":{c}" for c in df.columns])

    upsert_query = text(f"""
        INSERT INTO {table_name} ({cols_str})
        VALUES ({vals_str})
        ON CONFLICT ({conflict_str}) 
        DO UPDATE SET {set_str};
    """)

    try:
        with engine.begin() as conn:
            for record in records:
                conn.execute(upsert_query, record)
        print(f"    [✔] SUCCESS: {table_name} successfully updated.")
    except Exception as e:
        print(f"    [!] DATABASE ERROR in {table_name}:\n{e}")


# ==========================================
# 3. MASTER ETL EXECUTION
# ==========================================


def run_macro_etl(start_date=None, end_date=None):
    print("=" * 50)
    print("INITIATING MACRO INDICATORS PARSER (FII/DII & OI)")
    print("=" * 50)

    # 1. Process FII/DII Cash
    fiidii_files = glob.glob(os.path.join(CACHE_DIR, "*fiidii_master*.csv"))
    fiidii_data = []
    for file in fiidii_files:
        fiidii_data.append(parse_fiidii_cash(file))

    if fiidii_data:
        master_cash = pd.concat(fiidii_data, ignore_index=True)
        # Apply Delta filter if arguments passed
        if start_date and end_date:
            s_dt = pd.to_datetime(start_date).strftime("%Y-%m-%d 00:00:00")
            e_dt = pd.to_datetime(end_date).strftime("%Y-%m-%d 00:00:00")
            master_cash = master_cash[
                (master_cash["ReportDate"] >= s_dt)
                & (master_cash["ReportDate"] <= e_dt)
            ]

        if not master_cash.empty:
            update_cols = [c for c in master_cash.columns if c != "ReportDate"]
            push_to_db(master_cash, "macro_fiidii_cash", ["ReportDate"], update_cols)
    else:
        print("[-] No FII/DII Cash master files found.")

    # 2. Process Participant OI
    oi_files = glob.glob(os.path.join(CACHE_DIR, "nse_part_oi_*.csv"))
    oi_data = []
    print(f"[*] Found {len(oi_files)} Participant OI files.")
    for file in oi_files:
        oi_data.append(parse_participant_oi(file))

    if oi_data:
        master_oi = pd.concat(oi_data, ignore_index=True)
        # Apply Delta filter if arguments passed
        if start_date and end_date:
            s_dt = pd.to_datetime(start_date).strftime("%Y-%m-%d 00:00:00")
            e_dt = pd.to_datetime(end_date).strftime("%Y-%m-%d 00:00:00")
            master_oi = master_oi[
                (master_oi["ReportDate"] >= s_dt) & (master_oi["ReportDate"] <= e_dt)
            ]

        if not master_oi.empty:
            update_cols = [
                c for c in master_oi.columns if c not in ["ReportDate", "ClientType"]
            ]
            push_to_db(
                master_oi,
                "macro_participant_oi",
                ["ReportDate", "ClientType"],
                update_cols,
            )
    else:
        print("[-] No Participant OI files found.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()

    run_macro_etl(args.start, args.end)
