import os
import glob
import polars as pl
from datetime import datetime
from scripts.database import engine
import re
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


def parse_trade_events(file_path: str, event_type: str) -> pl.DataFrame:
    try:
        # Multi-threaded CSV parsing, explicitly ignoring bad lines natively
        # Using infer_schema_length=0 to prevent type-cast crashes on dirty rows
        df = pl.read_csv(
            file_path, encoding="utf8-lossy", ignore_errors=True, infer_schema_length=0
        )
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pl.DataFrame()

    if df.is_empty():
        return pl.DataFrame()

    # Map the dirty column names to our clean schema
    rename_map = {
        "Date": "ReportDate",
        "Symbol": "Ticker",
        "Security Name": "SecurityName",
        "Client Name": "ClientName",
        "Buy / Sell": "TransactionType",
        "Quantity Traded": "Quantity",
    }

    # Identify price column variant
    price_col = (
        "Trade Price / Wght. Avg. Price"
        if "Trade Price / Wght. Avg. Price" in df.columns
        else "Trade Price"
    )
    rename_map[price_col] = "TradePrice"

    # Filter columns to only what we need, rename them
    df = df.select(
        [pl.col(old).alias(new) for old, new in rename_map.items() if old in df.columns]
    )

    # Vectorized Casts & Date Parsing
    df = df.with_columns(
        [
            pl.col("ReportDate").str.strptime(pl.Date, format="%d-%b-%Y", strict=False),
            pl.col("Quantity").str.replace_all(",", "").cast(pl.Int64, strict=False),
            pl.col("TradePrice")
            .str.replace_all(",", "")
            .cast(pl.Float64, strict=False),
            pl.lit(event_type).alias("EventType"),
            pl.lit(None).cast(pl.Utf8).alias("Remarks"),
        ]
    )

    # Drop Invalid rows
    df = df.filter(
        pl.col("ReportDate").is_not_null()
        & pl.col("Ticker").is_not_null()
        & pl.col("Quantity").is_not_null()
    )

    return df


def execute_events_pipeline(start_date_str="1900-01-01"):
    resume_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()

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
            try:
                return datetime.strptime(match.group(1), "%d-%m-%Y").date()
            except ValueError:
                pass
        return datetime.strptime("2099-12-31", "%Y-%m-%d").date()

    all_dataframes = []

    # Process Bulk Deals
    for idx, file in enumerate(bulk_files, 1):
        if extract_end_date(file) < resume_date:
            continue

        print(
            f"[*] Parsing Bulk Deal [ {idx} / {len(bulk_files)} ] : {os.path.basename(file)}"
        )
        parsed_df = parse_trade_events(file, "Bulk Deal")
        if not parsed_df.is_empty():
            all_dataframes.append(parsed_df)
            log_audit(file, parsed_df.height, parsed_df.height, 0, "PARSED_IN_MEMORY")
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
        if not parsed_df.is_empty():
            all_dataframes.append(parsed_df)
            log_audit(file, parsed_df.height, parsed_df.height, 0, "PARSED_IN_MEMORY")
        else:
            log_audit(file, 0, 0, 0, "SKIPPED_EMPTY")

    if not all_dataframes:
        print("  No new trade events to process. DB is up to date.")
        return

    # Zero-copy Vertical Concat in Polars
    master_events_df = pl.concat(all_dataframes, how="vertical")

    # Filter by date
    master_events_df = master_events_df.filter(pl.col("ReportDate") >= resume_date)

    if master_events_df.is_empty():
        print("  All parsed events are older than the resume checkpoint.")
        return

    print(f"[*] Deduping {master_events_df.height} Trade Events in memory...")
    raw_count = master_events_df.height

    # Polars Native Unique
    master_events_df = master_events_df.unique(
        subset=[
            "ReportDate",
            "Ticker",
            "ClientName",
            "TransactionType",
            "Quantity",
            "TradePrice",
        ]
    )
    dedup_count = master_events_df.height

    # Push to DuckDB via Zero-Copy Arrow
    print(f"[*] Pushing {dedup_count} deduped trade events to DuckDB...")
    try:
        arrow_table = master_events_df.to_arrow()
        engine.register("temp_events", arrow_table)

        engine.execute("""
            INSERT INTO trade_events_ledger ("ReportDate", "Ticker", "EventType", "SecurityName", "ClientName", "TransactionType", "Quantity", "TradePrice", "Remarks")
            SELECT "ReportDate", "Ticker", "EventType", "SecurityName", "ClientName", "TransactionType", "Quantity", "TradePrice", "Remarks"
            FROM temp_events
            ON CONFLICT ("ReportDate", "Ticker", "ClientName", "TransactionType", "Quantity", "TradePrice") 
            DO NOTHING;
        """)
        engine.unregister("temp_events")
        log_audit("Batch_Events_Concat", raw_count, dedup_count, dedup_count, "SUCCESS")
        print("[SUCCESS] Trade Events Ledger Update Complete.")
    except Exception as e:
        error_msg = str(e).replace("\n", " ")[:80]
        log_audit(
            "Batch_Events_Concat", raw_count, dedup_count, 0, f"FAILED: {error_msg}"
        )
        print(f"  [X] Failed: {error_msg}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="1900-01-01")
    args = parser.parse_args()
    execute_events_pipeline(args.start)
