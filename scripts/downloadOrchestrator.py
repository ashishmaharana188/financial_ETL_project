import subprocess
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
import time

# Import your database engine
from scripts.database import engine

# =====================================================================
# PERSISTENT LOGGER SETUP
# =====================================================================
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


# =====================================================================
# CORE SUBPROCESS EXECUTION ENGINE (WITH STRICT HALT)
# =====================================================================
def run_isolated_script(script_name, extra_args=None):
    """
    Runs a python script synchronously. If it fails, it returns False.
    All stdout/stderr is tee'd to a log file for post-crash debugging.
    """
    header = f"\n{'='*50}\n🚀 EXECUTING: {script_name}\n{'='*50}\n"
    write_log(header)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    script_path = os.path.join(current_dir, script_name)

    if not os.path.exists(script_path):
        write_log(f"[!] Critical Error: Missing {script_path}\n")
        return False

    module_name = f"scripts.{script_name.replace('.py', '')}"
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


# =====================================================================
# DELTA BRIDGE LOGIC (2-Day Lag Safety Net)
# =====================================================================
def get_latest_db_date():
    dates = []
    try:
        with engine.connect() as conn:
            # 1. Check Unified Master components
            try:
                query_unified = text(
                    'SELECT "InstrumentType", MAX("ReportDate") FROM unified_market_master GROUP BY "InstrumentType";'
                )
                res_unified = conn.execute(query_unified).fetchall()

                cash_dates = [
                    pd.to_datetime(r[1]) for r in res_unified if r[0] == "CASH"
                ]
                deriv_dates = [
                    pd.to_datetime(r[1]) for r in res_unified if r[0] != "CASH"
                ]

                if cash_dates:
                    msg = f"[*] High-Water Mark (CASH): {max(cash_dates).strftime('%Y-%m-%d')}\n"
                    write_log(msg)
                    dates.append(max(cash_dates))
                if deriv_dates:
                    msg = f"[*] High-Water Mark (DERIVATIVES): {max(deriv_dates).strftime('%Y-%m-%d')}\n"
                    write_log(msg)
                    dates.append(max(deriv_dates))
            except Exception as e:
                write_log(f"[-] Could not read unified_market_master: {e}\n")

            # 2. Check Institutional Ledger
            try:
                query_inst = text('SELECT MAX("ReportDate") FROM institutional_ledger;')
                res_inst = conn.execute(query_inst).scalar()
                if res_inst:
                    dt = pd.to_datetime(res_inst)
                    dates.append(dt)
                    write_log(
                        f"[*] High-Water Mark (INSTITUTIONAL): {dt.strftime('%Y-%m-%d')}\n"
                    )
            except Exception:
                pass

            # 3. Find the Weakest Link
            if dates:
                weakest_link_date = min(dates)
                write_log(
                    f"[*] Weakest Link Detected: Restarting bridge from {weakest_link_date.strftime('%Y-%m-%d')} to ensure no missing files.\n"
                )
                return weakest_link_date

    except Exception as e:
        write_log(f"[-] Database error during high-water mark check: {e}\n")

    return None


# =====================================================================
# EXECUTION ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=["delta", "bulk_historic", "scrape_only"], required=True
    )
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()

    init_logger(args.mode)

    if args.mode == "delta":
        latest_date = get_latest_db_date()
        end_dt_str = datetime.now().strftime("%Y-%m-%d")

        if not latest_date:
            write_log(
                "[*] Database appears empty. Defaulting Delta Bridge to genesis date (2015-01-01).\n"
            )
            start_dt_str = "2015-01-01"
        else:
            lag_date = latest_date - timedelta(days=2)
            start_dt_str = lag_date.strftime("%Y-%m-%d")
            write_log(
                f"[*] Applying 2-Day Overlap Lag. Delta Bridge starting from: {start_dt_str}\n"
            )

        date_args = ["--start", start_dt_str, "--end", end_dt_str]

        write_log("\n--- PHASE 1: SEQUENTIAL EXTRACTION ---\n")
        if not run_isolated_script("nseScrape.py", date_args):
            sys.exit(1)
        if not run_isolated_script("nseArchiveLooper.py", date_args):
            sys.exit(1)
        if not run_isolated_script("fiiDiiBackfill.py", date_args):
            sys.exit(1)

        write_log("\n--- PHASE 2: SEQUENTIAL INGESTION (DUMB LOADERS) ---\n")
        loader_args = ["--start", start_dt_str]
        if not run_isolated_script("ingestUnifiedMatrix.py", loader_args):
            sys.exit(1)
        if not run_isolated_script("ingestInstitutional.py", loader_args):
            sys.exit(1)
        if not run_isolated_script("ingestEvents.py", loader_args):
            sys.exit(1)

        write_log("\n--- PHASE 3: ALPHA FACTORY REFRESH ---\n")
        if not run_isolated_script("materializedViewEngine.py", ["--refresh"]):
            sys.exit(1)

        write_log("\n[✔] DELTA BRIDGE COMPLETE.\n")

    elif args.mode == "bulk_historic":
        write_log(f"[*] Triggering ISOLATED Master Ingestion (Bypassing Scrapers)\n")

        write_log("\n--- PHASE 2: SEQUENTIAL INGESTION (DUMB LOADERS) ---\n")
        # The crucial fix: Notice the sys.exit(1) halts the pipeline if the parser crashes
        ui_args = ["--start", args.start]
        if not run_isolated_script("ingestUnifiedMatrix.py", ui_args):
            sys.exit(1)
        if not run_isolated_script("ingestInstitutional.py", ui_args):
            sys.exit(1)
        if not run_isolated_script("ingestEvents.py", ui_args):
            sys.exit(1)

        write_log("\n[✔] BULK INGESTION COMPLETE.\n")

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

        write_log("\n[✔] ISOLATED SCRAPE COMPLETE.\n")
