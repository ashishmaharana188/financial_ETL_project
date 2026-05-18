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
def run_isolated_script(script_name):
    """
    Runs a python script in a completely isolated sub-process.
    This guarantees Cloudflare sees the exact same memory footprint
    as when you run it manually from your terminal.
    """
    print(f"\n{'='*50}")
    print(f"LAUNCHING ISOLATED PIPELINE: {script_name}")
    print(f"{'='*50}\n")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)  # Step out to the main project folder
    script_path = os.path.join(current_dir, script_name)

    if not os.path.exists(script_path):
        print(f"[!] Error: Could not find {script_path}")
        return False

    # Build the module name (e.g., 'scripts.bhavcopyParser')
    module_name = f"scripts.{script_name.replace('.py', '')}"

    # Run it EXACTLY like the terminal: python -m scripts.filename
    process = subprocess.Popen(
        [sys.executable, "-m", module_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        cwd=project_root,  # Force execution from the root directory
    )

    for line in process.stdout:
        print(line, end="")

    process.wait()

    if process.returncode != 0:
        print(f"\n[!] ERROR: {script_name} crashed (Exit Code {process.returncode}).")
        return False
    else:
        print(f"\n[+] SUCCESS: {script_name} completed.")
        return True


def run_targeted_script(script_name, start_date, end_date):
    """
    Fires the isolated subprocess with targeted CLI dates for Delta Catchup.
    """
    print(f"\n{'='*50}")
    print(f"LAUNCHING TARGETED SYNC: {script_name}")
    print(f"Target Window: {start_date} to {end_date}")
    print(f"{'='*50}\n")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)  # Step out to the main project folder
    script_path = os.path.join(current_dir, script_name)

    if not os.path.exists(script_path):
        print(f"[!] Error: Could not find {script_path}")
        return False

    # Build the module name (e.g., 'scripts.nseScrape')
    module_name = f"scripts.{script_name.replace('.py', '')}"

    process = subprocess.Popen(
        [sys.executable, "-m", module_name, "--start", start_date, "--end", end_date],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        cwd=project_root,  # Force execution from the root directory
    )

    for line in process.stdout:
        print(line, end="")

    process.wait()
    return process.returncode == 0


# =====================================================================
# DELTA BRIDGE HELPER FUNCTIONS
# =====================================================================
def get_max_date_from_db(table_name, date_column="ReportDate", default_days_back=10):
    """
    Safely asks the database for the maximum date for a specific metric.
    If the table is empty or fails, defaults to a safe fallback.
    """
    try:
        query = text(f'SELECT MAX("{date_column}") FROM {table_name}')
        with engine.connect() as conn:
            result = conn.execute(query).scalar()

            if result:
                # Return the day AFTER the last recorded date to avoid downloading duplicates
                next_date = pd.to_datetime(result) + timedelta(days=1)
                return next_date.strftime("%Y-%m-%d")
            else:
                return (datetime.now() - timedelta(days=default_days_back)).strftime(
                    "%Y-%m-%d"
                )
    except Exception as e:
        print(
            f"[!] Warning: Could not read {table_name}. Returning default start date. ({e})"
        )
        return (datetime.now() - timedelta(days=default_days_back)).strftime("%Y-%m-%d")


# =====================================================================
# DASHBOARD ENTRY POINTS
# =====================================================================
def trigger_full_sync():
    """
    Called by the Dashboard's 'Full Sync' button.
    Runs the three independent scrapers sequentially using default horizons.
    """
    print("=" * 50)
    print("INITIATING MASTER DASHBOARD SYNC (FULL OVERRIDE)")
    print("=" * 50)

    pipeline_scripts = ["nseScrape.py", "nseArchiveLooper.py", "fiiDiiBackfill.py"]

    for script in pipeline_scripts:
        success = run_isolated_script(script)
        if not success:
            print(f"\n[!] Pipeline halted due to failure in {script}.")
            return

    print("\n[✔] ALL PIPELINES EXECUTED AND DATA SYNCED SUCCESSFULLY.")


def trigger_delta_sync():
    """
    Called by the Dashboard's 'Daily Update' button.
    Checks the DB and only downloads the specific missing dates.
    """
    print("=" * 50)
    print("INITIATING DELTA BRIDGE (DAILY CATCH-UP)")
    print("=" * 50)

    today = datetime.now().strftime("%Y-%m-%d")

    print("\n[1] Checking Database for missing dates...")
    bhavcopy_start = get_max_date_from_db("market_bhavcopy_metrics", "ReportDate")
    smart_money_start = (
        bhavcopy_start  # Assuming you want smart money synced to the same max date
    )
    fiidii_start = bhavcopy_start  # Assuming FII/DII is synced to the same max date

    print("\n[2] Executing targeted downloads...")

    if bhavcopy_start <= today:
        run_targeted_script("nseArchiveLooper.py", bhavcopy_start, today)

    if smart_money_start <= today:
        run_targeted_script("nseScrape.py", smart_money_start, today)

    if fiidii_start <= today:
        run_targeted_script("fiiDiiBackfill.py", fiidii_start, today)

    print("\nDELTA DOWNLOADS COMPLETE.")

    # 3. Immediately trigger the parser to process the new data
    print("\n[3] Triggering Parser...")
    run_isolated_script("bhavcopyParser.py")
    run_targeted_script("derivativesParser.py", bhavcopy_start, today)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        type=str,
        choices=["full", "delta"],
        default="delta",
        help="Choose sync mode",
    )
    args = parser.parse_args()

    if args.mode == "full":
        trigger_full_sync()
    else:
        trigger_delta_sync()
