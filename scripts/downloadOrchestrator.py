import subprocess
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

# Import your database engine
from scripts.database import engine


# =====================================================================
# CORE SUBPROCESS EXECUTION ENGINES
# =====================================================================
def run_isolated_script(script_name, extra_args=None):
    """
    Runs a python script in a completely isolated sub-process.
    """
    print(f"\n{'='*50}")
    print(f"LAUNCHING ISOLATED PIPELINE: {script_name}")
    print(f"{'='*50}\n")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    script_path = os.path.join(current_dir, script_name)

    if not os.path.exists(script_path):
        print(f"[!] Error: Could not find {script_path}")
        return False

    module_name = f"scripts.{script_name.replace('.py', '')}"
    cmd = [sys.executable, "-u", "-m", module_name]

    if extra_args:
        cmd.extend(extra_args)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        cwd=project_root,
    )

    for line in iter(process.stdout.readline, ""):
        print(line, end="")
    process.stdout.close()
    return_code = process.wait()

    if return_code != 0:
        print(f"\n[!] Pipeline {script_name} failed with exit code {return_code}")
        return False
    return True


def run_targeted_script(script_name, start_date, end_date):
    """Runs a script with specific --start and --end boundary arguments."""
    args = ["--start", start_date, "--end", end_date]
    return run_isolated_script(script_name, args)


def get_max_date_from_db(table_name, date_column="ReportDate", overlap_days=2):
    """
    Queries the database for the most recent date and applies a safety overlap.
    By default, it rolls back 2 days to heal any data fragmentation caused by terminal crashes.
    """
    query = text(f'SELECT MAX("{date_column}") FROM {table_name}')
    try:
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
            if result:
                # IDEMPOTENT HEALING: Go back 2 days instead of jumping forward
                safe_start_date = pd.to_datetime(result) - timedelta(days=overlap_days)
                return safe_start_date.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[-] Could not fetch max date from {table_name}: {e}")

    # Default fallback if table is completely empty
    return (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


# =====================================================================
# SYNC LOGIC: 3 DISTINCT MODES
# =====================================================================


def trigger_delta_sync():
    """MODE 1: Daily Catch-Up (Scrape missing, then parse missing)"""
    print("=" * 50)
    print("INITIATING DELTA BRIDGE (DAILY CATCH-UP)")
    print("=" * 50)

    today = datetime.now().strftime("%Y-%m-%d")

    print("\n[1] Checking Database for missing dates...")
    bhavcopy_start = get_max_date_from_db("market_bhavcopy_metrics")
    derivatives_start = get_max_date_from_db("derivatives_matrix")

    print("\n[2] Executing targeted downloads...")
    if bhavcopy_start <= today:
        run_targeted_script("nseScrape.py", bhavcopy_start, today)
        run_targeted_script("fiiDiiBackfill.py", bhavcopy_start, today)

    if derivatives_start <= today:
        run_targeted_script("nseArchiveLooper.py", derivatives_start, today)

    print("\n[3] Triggering Targeted Parsers...")
    run_isolated_script("bhavcopyParser.py")  # Internally defaults to daily bridge
    run_targeted_script("derivativesParser.py", derivatives_start, today)

    print("\n[✔] DELTA SYNC COMPLETE.")


def trigger_parse_all():
    """MODE 2: Master Data Sync (Parses local cache to DB, NO SCRAPING)"""
    print("=" * 50)
    print("INITIATING MASTER PARSE SYNC (LOCAL CACHE -> DB)")
    print("=" * 50)

    print("\n[1] Executing Bulk Parsing for Equities & Commodities...")
    run_isolated_script("bhavcopyParser.py", ["bulk"])

    print("\n[2] Executing Bulk Parsing for F&O Derivatives...")
    run_isolated_script("derivativesParser.py")  # Runs standalone cache scan

    print("\n[✔] MASTER PARSE SYNC COMPLETE.")


def trigger_scrape(start_dt=None, end_dt=None):
    """MODE 3: Scraper Only (Downloads files based on default or custom dates)"""
    print("=" * 50)
    print("INITIATING SCRAPER PIPELINE")
    print("=" * 50)

    if start_dt and end_dt:
        print(f"\n[*] Custom Date Range Detected: {start_dt} to {end_dt}")
        run_targeted_script("nseScrape.py", start_dt, end_dt)
        run_targeted_script("nseArchiveLooper.py", start_dt, end_dt)
        run_targeted_script("fiiDiiBackfill.py", start_dt, end_dt)
    else:
        print("\n[*] No custom dates provided. Running default historical scrapers...")
        run_isolated_script("nseScrape.py")
        run_isolated_script("nseArchiveLooper.py")
        run_isolated_script("fiiDiiBackfill.py")

    print("\n[✔] SCRAPE COMPLETE. Files are resting in the local cache.")


# =====================================================================
# EXECUTION ENTRY POINT
# =====================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=["delta", "parse_all", "scrape"], required=True
    )
    parser.add_argument(
        "--start", type=str, help="Start date for custom scrape (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=str, help="End date for custom scrape (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    if args.mode == "delta":
        trigger_delta_sync()
    elif args.mode == "parse_all":
        trigger_parse_all()
    elif args.mode == "scrape":
        trigger_scrape(args.start, args.end)
