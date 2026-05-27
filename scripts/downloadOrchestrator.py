import subprocess
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
import time

# Import your database engine
from scripts.database import engine

LOG_FILE = "pipeline_execution.log"


def write_log(message):
    """Writes to both the console and a persistent log file."""
    print(message, end="")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(message)
    except Exception as e:
        print(f"\n[!] Failed to write to log file: {e}")


def init_logger(mode):
    """Clears the old log and starts a new session."""
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== SWARM PIPELINE LOG INITIALIZED: {mode.upper()} ===\n")
        f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")


def run_isolated_script(script_name, extra_args=None):
    """
    Runs a python script synchronously. If it fails, it returns False.
    All stdout/stderr is tee'd to a log file for post-crash debugging.
    """
    header = f"\n{'='*50}\nEXECUTING: {script_name}\n{'='*50}\n"
    write_log(header)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    script_path = os.path.join(current_dir, script_name)

    if not os.path.exists(script_path):
        write_log(f"[!] Critical Error: Missing {script_path}\n")
        return False

    # Standardize forward and backward slashes into Python dot-notation module syntax
    normalized_script = script_name.replace("/", ".").replace("\\", ".")
    module_name = f"scripts.{normalized_script.replace('.py', '')}"

    cmd = [sys.executable, "-u", "-m", module_name]

    if extra_args:
        cmd.extend(extra_args)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Combines stderr into stdout so we catch tracebacks
        text=True,
        bufsize=1,
        universal_newlines=True,
        cwd=project_root,
    )

    # Stream output to both terminal and log file
    for line in iter(process.stdout.readline, ""):
        write_log(line)

    process.wait()

    if process.returncode != 0:
        error_msg = f"\n[!] SCRAPE/PARSE FAILED: {script_name} (Exit Code: {process.returncode})\n"
        error_msg += f"[*] Check '{LOG_FILE}' for the complete traceback.\n"
        write_log(error_msg)
        return False

    time.sleep(2)
    return True


def get_domain_watermark(table_name, friendly_name):
    try:
        with engine.connect() as conn:
            if table_name == "unified_market_master":
                # THE WEAKEST LINK STRATEGY:
                # Group by instrument type to find the MAX date for EACH instrument.
                # Then, return the MINIMUM of those MAX dates so the slowest instrument dictates the backfill.
                query = """
                SELECT MIN(max_date) FROM (
                    SELECT "InstrumentType", MAX("ReportDate") as max_date 
                    FROM unified_market_master 
                    WHERE "InstrumentType" IN ('CASH', 'STF', 'STO', 'IDF', 'IDO', 'FUTCOM', 'OPTFUT')
                    GROUP BY "InstrumentType"
                ) subquery;
                """
                res = conn.execute(text(query)).scalar()
            else:
                # Standard check for other tables (Institutional Ledger, etc.)
                res = conn.execute(
                    text(f'SELECT MAX("ReportDate") FROM {table_name}')
                ).scalar()

            if res:
                date_val = pd.to_datetime(res).date()
                write_log(f"[*] {friendly_name} Weakest-Link Watermark: {date_val}")
                return date_val

    except Exception as e:
        write_log(f"[-] DB Query Failed for {table_name}: {e}")

    default_date = pd.to_datetime("2015-01-01").date()
    write_log(
        f"[*] {friendly_name} Watermark: Not Found (Defaulting to {default_date})"
    )
    return default_date


def get_events_highest_watermark():
    """Finds the absolute highest available date across Bulk, Block, and Short Selling."""
    watermarks = []

    try:
        with engine.connect() as conn:
            # 1. Check Trade Events (Bulk & Block)
            res_events = conn.execute(
                text('SELECT MAX("ReportDate") FROM trade_events_ledger')
            ).scalar()
            if res_events:
                watermarks.append(pd.to_datetime(res_events).date())

            # 2. Check Short Selling (From Unified Master)
            res_short = conn.execute(
                text(
                    'SELECT MAX("ReportDate") FROM unified_market_master WHERE "Short_Volume" IS NOT NULL'
                )
            ).scalar()
            if res_short:
                watermarks.append(pd.to_datetime(res_short).date())

    except Exception as e:
        write_log(f"[-] DB Query Failed for Trade Events: {e}")

    # Return the HIGHEST date found
    if watermarks:
        highest_date = max(watermarks)
        write_log(f"[*] Trade Events (Strongest Link) Watermark: {highest_date}")
        return highest_date

    default_date = pd.to_datetime("2015-01-01").date()
    write_log(f"[*] Trade Events Watermark: Not Found (Defaulting to {default_date})")
    return default_date


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=[
            "delta",
            "bulk_historic",
            "scrape_only",
            "macro_refresh",
            "alpha_refresh",
        ],
        required=True,
    )
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()

    init_logger(args.mode)

    if args.mode == "delta":
        write_log("=== INITIATING DOMAIN-ISOLATED DELTA BRIDGE ===\n")

        today_str = pd.Timestamp.today().strftime("%Y-%m-%d")

        write_log("--- DOMAIN 1: MARKET DATA ---")
        market_date = get_domain_watermark("unified_market_master", "Market Data")
        market_start = (market_date - pd.Timedelta(days=2)).strftime("%Y-%m-%d")

        write_log(f"    -> Fetching & Parsing: {market_start} to {today_str}\n")
        if not run_isolated_script(
            "nseArchiveLooper.py", ["--start", market_start, "--end", today_str]
        ):
            sys.exit(1)
        if not run_isolated_script("ingestUnifiedMatrix.py", ["--start", market_start]):
            sys.exit(1)

        events_date = get_events_highest_watermark()
        events_start = (events_date - pd.Timedelta(days=2)).strftime("%Y-%m-%d")

        write_log(f"    -> Fetching & Parsing: {events_start} to {today_str}\n")
        if not run_isolated_script(
            "nseScrape.py", ["--start", events_start, "--end", today_str]
        ):
            sys.exit(1)
        if not run_isolated_script("ingestEvents.py", ["--start", events_start]):
            sys.exit(1)

        write_log("\n--- DOMAIN 3: INSTITUTIONAL FLOWS ---")
        fii_date = get_domain_watermark("institutional_ledger", "Institutional Flows")
        fii_start = (fii_date - pd.Timedelta(days=2)).strftime("%Y-%m-%d")

        write_log(f"    -> Fetching & Parsing: {fii_start} to {today_str}\n")
        if not run_isolated_script(
            "fiiDiiBackfill.py", ["--start", fii_start, "--end", today_str]
        ):
            sys.exit(1)
        if not run_isolated_script("ingestInstitutional.py", ["--start", fii_start]):
            sys.exit(1)

        write_log("\n  ALL DOMAINS SYNCED SUCCESSFULLY.\n")

        ## AUTOMATION LINK: SEQUENTIAL INFERENCE AND AUDIT PHASE
        # write_log("\n--- DOMAIN 4: QUANTITATIVE MATHEMATICAL CORES ---\n")
        # write_log("[*] Executing OLS Engine 1 (Incremental Prediction Catch-up)...")
        # if not run_isolated_script("engines/olsEngine1.py"):
        #    sys.exit(1)

        # write_log(
        #    "[*] Executing Systemic Auditor Engine (Realized Performance Catch-up)..."
        # )
        # if not run_isolated_script("engines/auditorEngine.py"):
        #    sys.exit(1)

        # write_log(
        #    "\n  QUANTITATIVE MODEL CACHE AND PERFORMANCE LEDGERS FULLY DEPLOYED.\n"
        # )

    elif args.mode == "bulk_historic":
        write_log(f"[*] Triggering ISOLATED Master Ingestion (Bypassing Scrapers)\n")

        write_log("\n--- PHASE 2: SEQUENTIAL INGESTION (DUMB LOADERS) ---\n")
        ui_args = ["--start", args.start]
        if not run_isolated_script("ingestUnifiedMatrix.py", ui_args):
            sys.exit(1)
        if not run_isolated_script("ingestInstitutional.py", ui_args):
            sys.exit(1)
        if not run_isolated_script("ingestEvents.py", ui_args):
            sys.exit(1)

        write_log("\n  BULK INGESTION COMPLETE.\n")

    elif args.mode == "scrape_only":
        write_log(
            f"[*] Triggering ISOLATED Extraction from {args.start} to {args.end}\n"
        )
        date_args = ["--start", args.start, "--end", args.end]

        write_log("\n--- PHASE 1: SEQUENTIAL EXTRACTION ---\n")
        if not run_isolated_script("nseScrape.py", date_args):
            sys.exit(1)
        if not run_isolated_script("nseArchiveLooper.py", date_args):
            sys.exit(1)
        if not run_isolated_script("fiiDiiBackfill.py", date_args):
            sys.exit(1)

        write_log("\n  ISOLATED SCRAPE COMPLETE.\n")

    elif args.mode == "macro_refresh":
        write_log(f"[*] Triggering ISOLATED Macro & Global Asset Pipeline\n")

        write_log("\n--- PHASE 1: MACRO SCRAPE & DB UPSERT ---\n")
        if not run_isolated_script("macroScrape.py"):
            sys.exit(1)

        write_log("\n  MACRO PIPELINE COMPLETE.\n")

    elif args.mode == "alpha_refresh":
        write_log(f"\n[*] Triggering ISOLATED Alpha Factory Refresh\n")

        write_log("\n--- REBUILDING MATERIALIZED VIEWS ---\n")
        if not run_isolated_script("materializedViewEngine.py", ["--build"]):
            sys.exit(1)

        # write_log("\n--- RECOMPUTING DEEP ANOMALY LEDGERS ---\n")
        # write_log("[*] Rebuilding complete multi-horizon prediction database cache...")
        # if not run_isolated_script("engines/olsEngine1.py"):
        #    sys.exit(1)

        # write_log("[*] Rebuilding complete historical hit-rate validation matrix...")
        # if not run_isolated_script("engines/auditorEngine.py"):
        #    sys.exit(1)

        write_log("\nALPHA FACTORY REFRESH COMPLETE.\n")
