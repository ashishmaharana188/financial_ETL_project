import os
import glob
import polars as pl
from datetime import datetime
from scripts.database import engine
import re
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


def parse_fiidii_cash(file_path: str) -> pl.DataFrame:
    try:
        # Load cleanly with polars
        df = pl.read_csv(file_path, encoding="utf8-lossy", ignore_errors=True)
        # Standardize column names
        df = df.rename({c: c.strip().lower() for c in df.columns})

        # Parse Dates safely
        df = df.with_columns(
            pl.col("date")
            .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
            .alias("ReportDate")
        )

        # FII Chunk
        fii_df = df.select(
            [
                pl.col("ReportDate"),
                pl.lit("FII").alias("ClientType"),
                pl.col("fii_buy").cast(pl.Float64).alias("Cash_Buy_Value"),
                pl.col("fii_sell").cast(pl.Float64).alias("Cash_Sell_Value"),
                pl.col("fii_net").cast(pl.Float64).alias("Cash_Net_Value"),
                pl.col("nifty_close").cast(pl.Float64).alias("Nifty_Close"),
            ]
        )

        # DII Chunk
        dii_df = df.select(
            [
                pl.col("ReportDate"),
                pl.lit("DII").alias("ClientType"),
                pl.col("dii_buy").cast(pl.Float64).alias("Cash_Buy_Value"),
                pl.col("dii_sell").cast(pl.Float64).alias("Cash_Sell_Value"),
                pl.col("dii_net").cast(pl.Float64).alias("Cash_Net_Value"),
                pl.col("nifty_close").cast(pl.Float64).alias("Nifty_Close"),
            ]
        )

        return pl.concat([fii_df, dii_df], how="vertical")
    except Exception as e:
        print(f"Error parsing cash file: {e}")
        return pl.DataFrame()


def parse_participant_oi(file_path: str) -> pl.DataFrame:
    try:
        # Skip the first row header
        df = pl.read_csv(
            file_path,
            skip_rows=1,
            encoding="utf8-lossy",
            ignore_errors=True,
            infer_schema_length=0,
        )
        df = df.rename({c: c.strip().replace("\t", "") for c in df.columns})

        filename = os.path.basename(file_path)
        match = re.search(r"(\d{8})", filename)
        if match:
            date_str = match.group(1)
            report_date = datetime.strptime(date_str, "%d%m%Y").date()
        else:
            return pl.DataFrame()

        # Rename Map
        rename_map = {
            "Client Type": "ClientType",
            "ClientType": "ClientType",
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

        # Apply Rename safely
        df = df.select(
            [
                pl.col(old).alias(new)
                for old, new in rename_map.items()
                if old in df.columns
            ]
        )

        numeric_cols = [c for c in df.columns if c not in ["ClientType"]]

        # Clean strings, strip commas, cast to Int
        df = df.with_columns(
            [
                pl.lit(report_date).alias("ReportDate"),
                *(
                    pl.col(c).str.replace_all(",", "").cast(pl.Int64, strict=False)
                    for c in numeric_cols
                ),
            ]
        )

        # Drop rows where ClientType is missing
        return df.filter(pl.col("ClientType").is_not_null())
    except Exception as e:
        print(f"Error reading OI file {file_path}: {e}")
        return pl.DataFrame()


def execute_macro_pipeline(start_date_str="1900-01-01"):
    resume_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()

    # 1. Load Cash File
    cash_file = os.path.join(CACHE_DIR, "niftytrader_fiidii_master.csv")
    df_cash = pl.DataFrame()
    if os.path.exists(cash_file):
        print(f"[*] Parsing Macro Cash Flow (Resuming from {resume_date})...")
        df_cash = parse_fiidii_cash(cash_file)
        if not df_cash.is_empty():
            df_cash = df_cash.filter(pl.col("ReportDate") >= resume_date)
            log_audit(cash_file, df_cash.height, df_cash.height, 0, "PARSED_IN_MEMORY")
    else:
        print("[-] NiftyTrader FII/DII master file not found.")

    # 2. Load all Participant OI Files
    oi_files = glob.glob(os.path.join(CACHE_DIR, "nse_part_oi_*.csv"))
    total_oi = len(oi_files)

    print(f"[*] Discovered Data: {total_oi} Participant OI files.")
    print(f"[*] Resume Checkpoint: {resume_date}")

    def extract_date(filepath):
        match = re.search(r"(\d{8})", filepath)
        if match:
            return datetime.strptime(match.group(1), "%d%m%Y").date()
        return datetime.strptime("1900-01-01", "%Y-%m-%d").date()

    oi_files = sorted(oi_files, key=extract_date)
    oi_dataframes = []

    for idx, file in enumerate(oi_files, 1):
        file_date = extract_date(file)

        if file_date < resume_date:
            continue

        print(
            f"[*] Parsing Participant OI [ {idx} / {total_oi} ] : {os.path.basename(file)}"
        )
        parsed_df = parse_participant_oi(file)
        if not parsed_df.is_empty():
            oi_dataframes.append(parsed_df)
            log_audit(file, parsed_df.height, parsed_df.height, 0, "PARSED_IN_MEMORY")
        else:
            log_audit(file, 0, 0, 0, "SKIPPED_EMPTY")

    df_oi = (
        pl.concat(oi_dataframes, how="vertical") if oi_dataframes else pl.DataFrame()
    )

    # 3. Outer Join the Universes Natively in Polars
    if not df_cash.is_empty() and not df_oi.is_empty():
        master_macro_df = df_cash.join(
            df_oi, on=["ReportDate", "ClientType"], how="full", coalesce=True
        )
    elif not df_cash.is_empty():
        master_macro_df = df_cash
    elif not df_oi.is_empty():
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

    # Add missing columns natively as nulls
    for col in final_columns:
        if col not in master_macro_df.columns:
            master_macro_df = master_macro_df.with_columns(pl.lit(None).alias(col))

    # Select explicit layout and dedup
    master_macro_df = master_macro_df.select(final_columns)

    raw_count = master_macro_df.height
    master_macro_df = master_macro_df.unique(
        subset=["ReportDate", "ClientType"], keep="last"
    )
    dedup_count = master_macro_df.height

    print(f"\n[DB PUSH] Upserting {dedup_count} rows into 'institutional_ledger'...")

    try:
        # Zero-Copy PyArrow Push to DuckDB
        engine.register("temp_inst", master_macro_df.to_arrow())

        conflict_keys = ["ReportDate", "ClientType"]
        update_cols = [
            col for col in master_macro_df.columns if col not in conflict_keys
        ]
        columns_str = ", ".join([f'"{col}"' for col in final_columns])
        set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

        # Native DuckDB Upsert
        engine.execute(f"""
            INSERT INTO institutional_ledger ({columns_str})
            SELECT {columns_str} FROM temp_inst
            ON CONFLICT ("ReportDate", "ClientType") 
            DO UPDATE SET {set_clause};
        """)

        engine.unregister("temp_inst")
        log_audit("Batch_Macro_Concat", raw_count, dedup_count, dedup_count, "SUCCESS")
        print("[SUCCESS] Institutional Ledger Update Complete.")

    except Exception as e:
        error_msg = str(e).replace("\n", " ")[:80]
        log_audit(
            "Batch_Macro_Concat", raw_count, dedup_count, 0, f"FAILED: {error_msg}"
        )
        print(f"  [X] Failed: {error_msg}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="1900-01-01")
    args = parser.parse_args()
    execute_macro_pipeline(args.start)
