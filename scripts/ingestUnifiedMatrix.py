import os
import glob
import json
import zipfile
import io
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from scripts.database import engine  # Ensure this points to your SQLAlchemy engine
from sqlalchemy.dialects.postgresql import insert
import re
import io
import uuid
import logging

CACHE_DIR = "offline_data_cache/master_archives"

# High-Speed Compact Logger
logging.basicConfig(
    filename="ingestion_audit.log",
    level=logging.INFO,
    format="%(asctime)s | FILE: %(file_name)-40s | RAW: %(raw)-8s | DEDUP: %(dedup)-8s | PUSH: %(push)-8s | STATUS: %(status)s",
    datefmt="%Y-%m-%d %H:%M:%S",
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

    # 1. Hunt down and destroy literal text dashes
    df = df.replace(to_replace=r"^\s*-\s*$", value=np.nan, regex=True)

    # 2. Split numeric columns by their absolute Database type
    float_cols = [
        "StrikePrice",
        "Open",
        "High",
        "Low",
        "Close",
        "Turnover",
        "Delivery_Percentage",
        "Settlement_Price",
        "Underlying_Price",
    ]

    int_cols = [
        "Volume",
        "No_Of_Trades",
        "Delivery_Qty",
        "Short_Volume",
        "Open_Interest",
        "Change_In_OI",
    ]

    # 3. Process Floats (Decimals allowed)
    for col in float_cols:
        if col in df.columns:
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(
                df[col]
            ):
                df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4. Process Integers (Force exact whole numbers, stripping .0)
    for col in int_cols:
        if col in df.columns:
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(
                df[col]
            ):
                df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 5. Final NaN/Infinity Cleanup
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)

    # 6. CRITICAL DB FIX: Primary Keys cannot be null. Futures have no StrikePrice.
    if "StrikePrice" in df.columns:
        df["StrikePrice"] = df["StrikePrice"].fillna(0.0)

    return df


def parse_cash_and_shorts(cash_file, preloaded_short_df=None):
    df_cash = pd.read_csv(cash_file)
    df_cash.rename(columns=lambda x: x.strip().upper(), inplace=True)

    df_cash["DATE1"] = df_cash["DATE1"].astype(str).str.strip()
    df_cash["ReportDate"] = pd.to_datetime(
        df_cash["DATE1"], format="mixed", dayfirst=True, errors="coerce"
    )
    df_cash = df_cash.dropna(subset=["ReportDate"])

    df_cash = df_cash.rename(
        columns={
            "SYMBOL": "Ticker",
            "SERIES": "Exchange_Series",
            "OPEN_PRICE": "Open",
            "HIGH_PRICE": "High",
            "LOW_PRICE": "Low",
            "CLOSE_PRICE": "Close",
            "TTL_TRD_QNTY": "Volume",
            "TURNOVER_LACS": "Turnover",
            "NO_OF_TRADES": "No_Of_Trades",
            "DELIV_QTY": "Delivery_Qty",
            "DELIV_PER": "Delivery_Percentage",
        }
    )

    df_cash["InstrumentType"] = "CASH"
    df_cash["ExpiryDate"] = pd.to_datetime("2099-12-31").date()
    df_cash["StrikePrice"] = 0.0

    # Instantly merge using the preloaded RAM object
    if preloaded_short_df is not None and not preloaded_short_df.empty:
        df_cash = pd.merge(
            df_cash,
            preloaded_short_df[["ReportDate", "Ticker", "Short_Volume"]],
            on=["ReportDate", "Ticker"],
            how="left",
        )
    else:
        df_cash["Short_Volume"] = None

    return df_cash


def parse_modern_fo_df(df):
    df.rename(columns=lambda x: x.strip(), inplace=True)

    df["TradDt"] = df["TradDt"].astype(str).str.strip()
    df["XpryDt"] = df["XpryDt"].astype(str).str.strip()

    df["ReportDate"] = pd.to_datetime(df["TradDt"])
    df["ExpiryDate"] = pd.to_datetime(df["XpryDt"]).dt.date

    # Use FinInstrmTp (Type) instead of FinInstrmNm (Name/Trading Symbol)
    col_mapping = {
        "TckrSymb": "Ticker",
        "FinInstrmTp": "InstrumentType",
        "StrkPric": "StrikePrice",
        "OptnTp": "OptionType",
        "OpnPric": "Open",
        "HghPric": "High",
        "LwPric": "Low",
        "ClsPric": "Close",
        "TtlTradgVol": "Volume",
        "TtlTrfVal": "Turnover",
        "TtlNbOfTxsExctd": "No_Of_Trades",
        "OpnIntrst": "Open_Interest",
        "ChngInOpnIntrst": "Change_In_OI",
        "SttlmPric": "Settlement_Price",
        "UndrlygPric": "Underlying_Price",
    }

    # Only rename columns that actually exist to avoid KeyErrors
    df = df.rename(columns={k: v for k, v in col_mapping.items() if k in df.columns})

    # Fallback just in case NSE mangled the headers on a specific day
    if "InstrumentType" not in df.columns and "FinInstrmNm" in df.columns:
        df["InstrumentType"] = df["FinInstrmNm"].astype(str).str[:20]

    return df


def parse_legacy_fo_df(df):
    df.rename(columns=lambda x: x.strip().upper(), inplace=True)

    # FIX: Strip hidden spaces
    df["TIMESTAMP"] = df["TIMESTAMP"].astype(str).str.strip()
    df["EXPIRY_DT"] = df["EXPIRY_DT"].astype(str).str.strip()

    df["ReportDate"] = pd.to_datetime(df["TIMESTAMP"], format="%d-%b-%Y")
    df["ExpiryDate"] = pd.to_datetime(df["EXPIRY_DT"], format="%d-%b-%Y").dt.date

    df = df.rename(
        columns={
            "SYMBOL": "Ticker",
            "INSTRUMENT": "InstrumentType",
            "STRIKE_PR": "StrikePrice",
            "OPTION_TYP": "OptionType",
            "OPEN": "Open",
            "HIGH": "High",
            "LOW": "Low",
            "CLOSE": "Close",
            "CONTRACTS": "Volume",
            "VAL_INLAKH": "Turnover",
            "OPEN_INT": "Open_Interest",
            "CHG_IN_OI": "Change_In_OI",
            "SETTLE_PR": "Settlement_Price",
        }
    )

    df["OptionType"] = df["OptionType"].replace("XX", None)
    return df


def parse_mcx(file_path):
    with open(file_path, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return pd.DataFrame()  # File is corrupt or empty

    # Handle if JSON is wrapped in a dict
    if isinstance(data, dict):
        if "d" in data and isinstance(data["d"], dict) and "Data" in data["d"]:
            data = data["d"]["Data"]
        elif "data" in data:
            data = data["data"]
        elif "Data" in data:
            data = data["Data"]
        else:
            data = [data]

    df = pd.DataFrame(data)

    # Safely abort if the file generated zero rows/columns
    if df.empty:
        return pd.DataFrame()

    df.columns = df.columns.str.strip()

    # Safely abort if it's missing the expected Date key
    if "Date" not in df.columns:
        if "date" in df.columns:
            df.rename(columns={"date": "Date"}, inplace=True)
        else:
            return pd.DataFrame()

    df["ReportDate"] = pd.to_datetime(df["Date"])
    df["ExpiryDate"] = pd.to_datetime(
        df["ExpiryDate"], format="mixed", errors="coerce"
    ).dt.date

    df = df.rename(
        columns={
            "Symbol": "Ticker",
            "InstrumentName": "InstrumentType",
            "StrikePrice": "StrikePrice",
            "OptionType": "OptionType",
            "Open": "Open",
            "High": "High",
            "Low": "Low",
            "Close": "Close",
            "Volume": "Volume",
            "Value": "Turnover",
            "OpenInterest": "Open_Interest",
        }
    )

    if "StrikePrice" in df.columns:
        df["StrikePrice"] = df["StrikePrice"].fillna(0.0)

    return df


def push_chunk_to_db(df, file_name="Unknown_File"):
    """Bypasses SQLAlchemy text-parsing and uses psycopg2 COPY for 100x throughput."""
    if df.empty:
        log_audit(file_name, 0, 0, 0, "SKIPPED_EMPTY")
        return

    raw_count = len(df)  # Track incoming rows

    final_columns = [
        "Ticker",
        "ReportDate",
        "InstrumentType",
        "ExpiryDate",
        "StrikePrice",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Exchange_Series",
        "Turnover",
        "No_Of_Trades",
        "Delivery_Qty",
        "Delivery_Percentage",
        "Short_Volume",
        "OptionType",
        "Open_Interest",
        "Change_In_OI",
        "Settlement_Price",
        "Underlying_Price",
    ]

    for col in final_columns:
        if col not in df.columns:
            df[col] = None

    df = df[final_columns]
    df = clean_for_db(df)

    # Clean duplicates in Python first
    pk_cols = [
        "Ticker",
        "ReportDate",
        "InstrumentType",
        "ExpiryDate",
        "StrikePrice",
        "OptionType",
        "Exchange_Series",
    ]

    df = df.dropna(subset=["ReportDate"])

    # 2. Strip hidden spaces to prevent logical string mismatches
    str_pk_cols = ["Ticker", "InstrumentType", "OptionType", "Exchange_Series"]
    for col in str_pk_cols:
        if col in df.columns:
            # Re-convert to NaN if it was a genuine null string like "nan" or "None"
            df[col] = (
                df[col].astype(str).str.strip().replace({"nan": np.nan, "None": np.nan})
            )

    df["Ticker"] = df["Ticker"].fillna("UNKNOWN")
    df["InstrumentType"] = df["InstrumentType"].fillna("XX")
    df["OptionType"] = df["OptionType"].fillna("XX")
    df["Exchange_Series"] = df["Exchange_Series"].fillna("XX")
    df["StrikePrice"] = df["StrikePrice"].fillna(0.0)
    df["ExpiryDate"] = df["ExpiryDate"].fillna(pd.to_datetime("2099-12-31").date())

    pk_cols = [
        "Ticker",
        "ReportDate",
        "InstrumentType",
        "ExpiryDate",
        "StrikePrice",
        "OptionType",
        "Exchange_Series",
    ]

    df = df.drop_duplicates(subset=pk_cols, keep="last")
    dedup_count = len(df)

    print(f"    -> [BULK PUSH] Streaming {len(df)} rows to DB via COPY protocol...")

    # 1. Dump DataFrame to an in-memory CSV buffer (Tab separated handles commas in text safely)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=True, sep="\t", na_rep="\\N")
    csv_buffer.seek(0)

    # Unique temp table name for thread safety
    temp_table = f"temp_master_{uuid.uuid4().hex[:8]}"

    # 2. Get the raw psycopg2 connection from SQLAlchemy
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            # Create a temporary table that perfectly mimics our master table.
            # ON COMMIT DROP ensures it deletes itself immediately after the transaction.
            cur.execute(
                f"CREATE TEMP TABLE {temp_table} (LIKE unified_market_master INCLUDING ALL) ON COMMIT DROP;"
            )

            # 3. Stream data from Python RAM directly into Postgres Engine
            cur.copy_expert(
                f"COPY {temp_table} FROM STDIN WITH CSV HEADER DELIMITER '\t' NULL '\\N'",
                csv_buffer,
            )

            # 4. Construct internal Upsert query
            columns = ", ".join([f'"{col}"' for col in df.columns])
            set_clause = ", ".join(
                [
                    f'"{col}" = EXCLUDED."{col}"'
                    for col in df.columns
                    if col not in pk_cols
                ]
            )

            upsert_query = f"""
                INSERT INTO unified_market_master ({columns})
                SELECT {columns} FROM {temp_table}
                ON CONFLICT ("Ticker", "ReportDate", "InstrumentType", "ExpiryDate", "StrikePrice", "OptionType", "Exchange_Series")
                DO UPDATE SET {set_clause};
            """
            cur.execute(upsert_query)

        raw_conn.commit()
        log_audit(
            file_name, raw_count, dedup_count, dedup_count, "SUCCESS"
        )  # UPDATE 4: Log Success
    except Exception as e:
        raw_conn.rollback()
        error_msg = str(e).replace("\n", " ")[:80]
        log_audit(
            file_name, raw_count, dedup_count, 0, f"FAILED: {error_msg}"
        )  # UPDATE 5: Log Failure
        print(f"    [-] CRITICAL Bulk Push Error: {e}")
    finally:
        raw_conn.close()


def execute_pipeline(start_date_str="1900-01-01"):
    resume_date = pd.to_datetime(start_date_str).date()

    cash_files = glob.glob(os.path.join(CACHE_DIR, "nse_cash_*.csv"))
    short_files = glob.glob(os.path.join(CACHE_DIR, "nse_short_selling_*.csv"))
    fo_zips = glob.glob(os.path.join(CACHE_DIR, "nse_fo_bhav_*.zip"))
    mcx_files = glob.glob(os.path.join(CACHE_DIR, "mcx_bhav_*.json"))

    total_cash = len(cash_files)
    total_fo = len(fo_zips)
    total_mcx = len(mcx_files)

    print(
        f"[*] Discovered Data: {total_cash} Cash CSVs | {total_fo} F&O Zips | {total_mcx} MCX JSONs."
    )
    print(f"[*] Resume Checkpoint: {resume_date}")

    def extract_date(filepath):
        match = re.search(r"(\d{8})", filepath)
        if match:
            date_str = match.group(1)

            # 1. Try DDMMYYYY (NSE Standard)
            dt = pd.to_datetime(date_str, format="%d%m%Y", errors="coerce")

            # 2. If it fails (returns NaT), fallback to YYYYMMDD (MCX Standard)
            if pd.isnull(dt):
                dt = pd.to_datetime(date_str, format="%Y%m%d", errors="coerce")

            # 3. If a valid date was found, return it
            if pd.notnull(dt):
                return dt.date()

        return pd.to_datetime("1900-01-01").date()

    cash_files = sorted(cash_files, key=extract_date)
    fo_zips = sorted(fo_zips, key=extract_date)
    mcx_files = sorted(mcx_files, key=extract_date)

    # --- PRE-LOAD SHORT DATA INTO RAM ---
    global_short_df = None
    if short_files:
        print("[*] Pre-loading Master Short Selling Inventory into RAM...")
        df_short = pd.read_csv(short_files[0])
        df_short.rename(columns=lambda x: x.strip(), inplace=True)
        df_short["Date"] = df_short["Date"].astype(str).str.strip()
        df_short["ReportDate"] = pd.to_datetime(
            df_short["Date"], format="mixed", dayfirst=True, errors="coerce"
        )
        df_short = df_short.dropna(subset=["ReportDate"])
        global_short_df = df_short.rename(
            columns={"Symbol": "Ticker", "Quantity": "Short_Volume"}
        )

    # 1. PARSE CASH & SHORT SELLING
    for idx, cash_file in enumerate(cash_files, 1):
        file_date = extract_date(cash_file)

        # INCREMENTAL SKIP LOGIC
        if file_date < resume_date:
            continue

        print(
            f"[*] Parsing Cash [ {idx} / {total_cash} ] : {os.path.basename(cash_file)}"
        )
        # Pass the pre-loaded RAM object here instead of a filepath
        df = parse_cash_and_shorts(cash_file, global_short_df)
        push_chunk_to_db(df, cash_file)

    # 2. PARSE F&O
    for idx, z_path in enumerate(fo_zips, 1):
        file_date = extract_date(z_path)

        if file_date < resume_date:
            continue

        print(
            f"[*] Parsing F&O Zip [ {idx} / {total_fo} ] : {os.path.basename(z_path)}"
        )
        try:
            with zipfile.ZipFile(z_path, "r") as z:
                for file_name in z.namelist():
                    if file_name.startswith("BhavCopy_NSE_FO") and file_name.endswith(
                        ".csv"
                    ):
                        with z.open(file_name) as f:
                            df = pd.read_csv(f)
                            push_chunk_to_db(parse_modern_fo_df(df), file_name)

                    elif file_name.startswith("fo") and file_name.endswith("bhav.csv"):
                        with z.open(file_name) as f:
                            df = pd.read_csv(f)
                            push_chunk_to_db(parse_legacy_fo_df(df), file_name)
        except zipfile.BadZipFile:
            print(f"    [-] Corrupted Zip File skipped: {z_path}")

    # 3. PARSE MCX
    for idx, mcx_file in enumerate(mcx_files, 1):
        file_date = extract_date(mcx_file)

        if file_date < resume_date:
            continue

        print(f"[*] Parsing MCX [ {idx} / {total_mcx} ] : {os.path.basename(mcx_file)}")
        push_chunk_to_db(parse_mcx(mcx_file), mcx_file)

    print("[SUCCESS] Unified Matrix Update Complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="1900-01-01")
    args = parser.parse_args()

    execute_pipeline(args.start)
