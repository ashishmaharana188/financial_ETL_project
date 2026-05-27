import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from scripts.database import engine  # Ensure this points to your SQLAlchemy engine
import re
from sqlalchemy.dialects.postgresql import insert
import logging

CACHE_DIR = "offline_data_cache/master_archives"

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


def parse_fiidii_cash(file_path):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.lower()
    df["ReportDate"] = pd.to_datetime(df["date"])

    fii_df = pd.DataFrame(
        {
            "ReportDate": df["ReportDate"],
            "ClientType": "FII",
            "Cash_Buy_Value": df["fii_buy"],
            "Cash_Sell_Value": df["fii_sell"],
            "Cash_Net_Value": df["fii_net"],
            "Nifty_Close": df["nifty_close"],
        }
    )

    dii_df = pd.DataFrame(
        {
            "ReportDate": df["ReportDate"],
            "ClientType": "DII",
            "Cash_Buy_Value": df["dii_buy"],
            "Cash_Sell_Value": df["dii_sell"],
            "Cash_Net_Value": df["dii_net"],
            "Nifty_Close": df["nifty_close"],
        }
    )

    return pd.concat([fii_df, dii_df], ignore_index=True)


def parse_participant_oi(file_path):
    df = pd.read_csv(file_path, header=1)
    df.columns = df.columns.str.strip().str.replace("\t", "")

    filename = os.path.basename(file_path)
    match = re.search(r"(\d{8})", filename)
    if match:
        date_str = match.group(1)
        report_date = pd.to_datetime(date_str, format="%d%m%Y")
    else:
        return pd.DataFrame()

    df["ReportDate"] = report_date

    df = df.rename(
        columns={
            "Client Type": "ClientType",
            "Future Index Long": "Future_Index_Long",
            "Future Index Short": "Future_Index_Short",
            "Future Stock Long": "Future_Stock_Long",
            "Future Stock Short": "Future_Stock_Short",
            "Option Index Call Long": "Option_Index_Call_Long",
            "Option Index Put Long": "Option_Index_Put_Long",
            "Option Index Call Short": "Option_Index_Call_Short",
            "Option Index Put Short": "Option_Index_Put_Short",
            "Option Stock Call Long": "Option_Stock_Call_Long",
            "Option Stock Put Long": "Option_Stock_Put_Long",
            "Option Stock Call Short": "Option_Stock_Call_Short",
            "Option Stock Put Short": "Option_Stock_Put_Short",
            "Total Long Contracts": "Total_Long_Contracts",
            "Total Short Contracts": "Total_Short_Contracts",
        }
    )

    df = df.dropna(subset=["ClientType"])

    numeric_cols = [
        col for col in df.columns if col not in ["ClientType", "ReportDate"]
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False), errors="coerce"
        )
    return df


def execute_macro_pipeline(start_date_str="1900-01-01"):
    resume_date = pd.to_datetime(start_date_str).date()

    # 1. Load Cash File
    cash_file = os.path.join(CACHE_DIR, "niftytrader_fiidii_master.csv")
    if os.path.exists(cash_file):
        print(f"[*] Parsing Macro Cash Flow (Resuming from {resume_date})...")
        df_cash = parse_fiidii_cash(cash_file)
        # INCREMENTAL SKIP LOGIC for Single Dataframe
        log_audit(cash_file, len(df_cash), len(df_cash), 0, "PARSED_IN_MEMORY")

        df_cash = df_cash[df_cash["ReportDate"].dt.date >= resume_date]
    else:
        df_cash = pd.DataFrame()
        print("[-] NiftyTrader FII/DII master file not found.")

    # 2. Load all Participant OI Files
    oi_files = glob.glob(os.path.join(CACHE_DIR, "nse_part_oi_*.csv"))
    total_oi = len(oi_files)

    print(f"[*] Discovered Data: {total_oi} Participant OI files.")
    print(f"[*] Resume Checkpoint: {resume_date}")

    # --- THE CHRONOLOGICAL FORCER ---
    def extract_date(filepath):
        match = re.search(r"(\d{8})", filepath)
        if match:
            return pd.to_datetime(match.group(1), format="%d%m%Y").date()
        return pd.to_datetime("1900-01-01").date()

    oi_files = sorted(oi_files, key=extract_date)
    oi_dataframes = []

    for idx, file in enumerate(oi_files, 1):
        file_date = extract_date(file)

        # INCREMENTAL SKIP LOGIC
        if file_date < resume_date:
            continue

        print(
            f"[*] Parsing Participant OI [ {idx} / {total_oi} ] : {os.path.basename(file)}"
        )
        parsed_df = parse_participant_oi(file)
        if not parsed_df.empty:
            oi_dataframes.append(parsed_df)
            log_audit(file, len(parsed_df), len(parsed_df), 0, "PARSED_IN_MEMORY")
        else:
            log_audit(file, 0, 0, 0, "SKIPPED_EMPTY")

    if oi_dataframes:
        df_oi = pd.concat(oi_dataframes, ignore_index=True)
    else:
        df_oi = pd.DataFrame()

    # 3. Merge the two Universes together
    if not df_cash.empty and not df_oi.empty:
        master_macro_df = pd.merge(
            df_cash, df_oi, on=["ReportDate", "ClientType"], how="outer"
        )
    elif not df_cash.empty:
        master_macro_df = df_cash
    elif not df_oi.empty:
        master_macro_df = df_oi
    else:
        print("[-] No macro data to process (or all skipped). Aborting.")
        return

    # 4. Enforce strict Database Column alignment
    final_columns = [
        "ReportDate",
        "ClientType",
        "Cash_Buy_Value",
        "Cash_Sell_Value",
        "Cash_Net_Value",
        "Nifty_Close",
        "Future_Index_Long",
        "Future_Index_Short",
        "Future_Stock_Long",
        "Future_Stock_Short",
        "Option_Index_Call_Long",
        "Option_Index_Put_Long",
        "Option_Index_Call_Short",
        "Option_Index_Put_Short",
        "Option_Stock_Call_Long",
        "Option_Stock_Put_Long",
        "Option_Stock_Call_Short",
        "Option_Stock_Put_Short",
        "Total_Long_Contracts",
        "Total_Short_Contracts",
    ]

    for col in final_columns:
        if col not in master_macro_df.columns:
            master_macro_df[col] = None

    master_macro_df = master_macro_df[final_columns]
    master_macro_df = clean_for_db(master_macro_df)

    pk_cols = ["ReportDate", "ClientType"]
    raw_count = len(master_macro_df)
    master_macro_df = master_macro_df.drop_duplicates(subset=pk_cols, keep="last")
    dedup_count = len(master_macro_df)

    print(
        f"\n[DB PUSH] Upserting {len(master_macro_df)} rows into 'institutional_ledger'..."
    )

    def postgres_upsert(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_stmt = insert(table.table).values(data)
        update_dict = {c.name: c for c in insert_stmt.excluded if c.name not in pk_cols}
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=pk_cols, set_=update_dict
        )
        conn.execute(upsert_stmt)

    try:
        master_macro_df.to_sql(
            "institutional_ledger",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=5000,
            method=postgres_upsert,
        )
        log_audit("Batch_Macro_Concat", raw_count, dedup_count, dedup_count, "SUCCESS")
        print("[SUCCESS] Institutional Ledger Update Complete.")
    except Exception as e:
        error_msg = str(e).replace("\n", " ")[:80]
        log_audit(
            "Batch_Macro_Concat", raw_count, dedup_count, 0, f"FAILED: {error_msg}"
        )
        print(f"    [-] CRITICAL Bulk Push Error: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="1900-01-01")
    args = parser.parse_args()

    execute_macro_pipeline(args.start)
