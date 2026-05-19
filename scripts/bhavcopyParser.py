import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from scripts.database import engine, get_missing_dates
import glob
import zipfile

# Define your cache directory where your raw files live
CACHE_DIR = "offline_data_cache/master_archives"


def push_to_bhavcopy_metrics(df):

    if df.empty:
        return

    print(f"    [DB PUSH] Upserting {len(df)} records to market_bhavcopy_metrics...")

    # 1. Protect against math operations that generated Infinity (e.g. Coc)
    import numpy as np
    import math

    df = df.replace([np.inf, -np.inf], np.nan)

    # 2. Convert dataframe rows to raw Python dictionaries
    records = df.to_dict(orient="records")

    # 3. BULLETPROOF NULLIFICATION (Bypassing Pandas)
    # Postgres BIGINT hates Python 'NaN'. We must safely force it to Python 'None' so it becomes SQL 'NULL'
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and math.isnan(value):
                record[key] = None

    upsert_query = text("""
        INSERT INTO market_bhavcopy_metrics (
            "IndicatorName", "ReportDate", "Open", "High", "Low", 
            "Close_Value", "Volume", "Delivery_Percentage", 
            "Short_Volume", "Cost_Of_Carry", "Open_Interest", "AssetClass"
        )
        VALUES (
            :IndicatorName, :ReportDate, :Open, :High, :Low, 
            :Close_Value, :Volume, :Delivery_Percentage, 
            :Short_Volume, :Cost_Of_Carry, :Open_Interest, :AssetClass
        )
        ON CONFLICT ("IndicatorName", "ReportDate") 
        DO UPDATE SET 
            "Open" = EXCLUDED."Open",
            "High" = EXCLUDED."High",
            "Low" = EXCLUDED."Low",
            "Close_Value" = EXCLUDED."Close_Value",
            "Volume" = EXCLUDED."Volume",
            "Delivery_Percentage" = EXCLUDED."Delivery_Percentage",
            "Short_Volume" = EXCLUDED."Short_Volume",
            "Cost_Of_Carry" = EXCLUDED."Cost_Of_Carry",
            "Open_Interest" = EXCLUDED."Open_Interest",
            "AssetClass" = EXCLUDED."AssetClass";
    """)

    try:
        with engine.begin() as conn:
            for record in records:
                conn.execute(upsert_query, record)
        print("    [✔] SUCCESS: Metrics successfully updated.")
    except Exception as e:
        print(f" DATABASE ERROR:\n{e}")


def parse_mcx_bhavcopy(mcx_file_path, target_date):
    """Parses MCX JSON for Commodities and extracts OHLCV + Open Interest."""
    if not os.path.exists(mcx_file_path):
        print(f"[-] Missing MCX File: {mcx_file_path}")
        return pd.DataFrame()

    print(" -> Parsing MCX Commodities...")
    with open(mcx_file_path, "r") as f:
        data = json.load(f)

    records = data.get("d", {}).get("Data", [])
    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["Symbol"] = df["Symbol"].str.strip()

    # Standardize column names to map to DB
    mapped_df = pd.DataFrame(
        {
            "IndicatorName": df["Symbol"],
            "ReportDate": pd.to_datetime(target_date).strftime("%Y-%m-%d %H:%M:%S"),
            "Open": df["Open"],
            "High": df["High"],
            "Low": df["Low"],
            "Close_Value": df["Close"],
            "Volume": df["Volume"],
            "Open_Interest": df["OpenInterest"],
            "Delivery_Percentage": None,
            "Short_Volume": None,
            "Cost_Of_Carry": None,
            "AssetClass": "Commodity",
        }
    )

    # Take the nearest expiry (highest volume usually, or drop duplicates)
    mapped_df = mapped_df.sort_values("Volume", ascending=False).drop_duplicates(
        subset=["IndicatorName"], keep="first"
    )
    return mapped_df


def parse_nse_bhavcopies(cash_path, fo_path, master_short_df, target_date):
    """Merges Cash OHLCV, Delivery, FO Cost of Carry, and RAM-cached Short Volume."""
    if not os.path.exists(cash_path):
        return pd.DataFrame()

    t_date_str = pd.to_datetime(target_date).strftime("%Y-%m-%d")
    print(" -> Parsing NSE Cash, Delivery, and SGBs/ETFs...")

    # 1. READ CASH FILE
    try:
        # utf-8-sig automatically destroys Excel's invisible BOM marker
        cash_df = pd.read_csv(cash_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        # Fallback for NSE's dirty characters (like the original 08-08-2022 file)
        cash_df = pd.read_csv(cash_path, encoding="latin1")

    # Strip spaces and any remaining invisible characters
    cash_df.columns = (
        cash_df.columns.str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

    # === BULLETPROOF SAFETY CHECK ===
    if "SYMBOL" not in cash_df.columns:
        print(
            f"    [!] WARNING: Corrupted or invalid Cash file detected for {t_date_str}. Skipping date."
        )
        return pd.DataFrame()

    cash_df["SYMBOL"] = cash_df["SYMBOL"].str.strip()
    cash_df["SERIES"] = cash_df["SERIES"].str.strip()

    # Clean Delivery % (remove '-' and convert to float)
    cash_df["DELIV_PER"] = pd.to_numeric(
        cash_df["DELIV_PER"].replace("-", np.nan), errors="coerce"
    )

    # Identify Asset Classes
    cash_df["AssetClass"] = "Equity"
    cash_df.loc[cash_df["SERIES"] == "GB", "AssetClass"] = "SGB"
    base_df = cash_df[cash_df["SERIES"].isin(["EQ", "BE", "GB", "SM", "ST"])].copy()

    # 2. SHORT VOLUME MERGE (From RAM, NO Hard Drive Checks)
    short_vol_df = pd.DataFrame(columns=["SYMBOL", "Short_Volume"])
    if not master_short_df.empty:
        # Lightning-fast memory filter instead of a hard-drive read
        short_vol_df = master_short_df[master_short_df["Date"] == t_date_str][
            ["SYMBOL", "Short_Volume"]
        ]

    base_df = pd.merge(base_df, short_vol_df, on="SYMBOL", how="left")

    # 3. DERIVATIVES MERGE (COST OF CARRY & TOTAL OPEN INTEREST)
    coc_df = pd.DataFrame(columns=["SYMBOL", "Cost_Of_Carry"])
    oi_df = pd.DataFrame(columns=["SYMBOL", "Open_Interest"])

    if os.path.exists(fo_path):
        print(" -> Extracting F&O Cost of Carry & Open Interest...")
        try:
            target_dt = pd.to_datetime(target_date)

            with zipfile.ZipFile(fo_path, "r") as z:
                # Find CSV dynamically (supports ISO casing changes)
                file_list = z.namelist()
                csv_filename = next(
                    (
                        name
                        for name in file_list
                        if name.lower().endswith(".csv") and "bhav" in name.lower()
                    ),
                    None,
                )
                if not csv_filename:
                    csv_filename = next(
                        (name for name in file_list if name.lower().endswith(".csv")),
                        None,
                    )

                with z.open(csv_filename) as f:
                    fo_df = pd.read_csv(f, encoding="latin1")

            # Smart Formatting against invisible BOMs
            fo_df.columns = (
                fo_df.columns.str.replace("ï»¿", "", regex=False)
                .str.replace("\ufeff", "", regex=False)
                .str.strip()
                .str.upper()
            )

            if "TCKRSYMB" in fo_df.columns:
                # --- NEW ISO FORMAT ---
                ticker_col = fo_df["TCKRSYMB"].str.strip()
                expiry_col = pd.to_datetime(fo_df["XPRYDT"], format="mixed")
                close_col = pd.to_numeric(fo_df["CLSPRIC"], errors="coerce")
                oi_col_raw = pd.to_numeric(fo_df["OPNINTRST"], errors="coerce")
                inst_type_col = fo_df.apply(
                    lambda x: (
                        "FUT"
                        if str(x.get("FININSTRMTP", "")).endswith("F")
                        else str(x.get("OPTNTP", "")).strip()
                    ),
                    axis=1,
                )
            else:
                # --- OLD FORMAT ---
                ticker_col = fo_df["SYMBOL"].str.strip()
                expiry_col = pd.to_datetime(fo_df["EXPIRY_DT"], format="mixed")
                close_col = pd.to_numeric(fo_df["CLOSE"], errors="coerce")
                oi_col_raw = pd.to_numeric(fo_df["OPEN_INT"], errors="coerce")
                inst_type_col = fo_df.apply(
                    lambda x: (
                        "FUT"
                        if "FUT" in str(x.get("INSTRUMENT", "")).upper()
                        else str(x.get("OPTION_TYP", "")).strip()
                    ),
                    axis=1,
                )

            clean_fo = pd.DataFrame(
                {
                    "SYMBOL": ticker_col,
                    "EXPIRY_DT": expiry_col,
                    "INSTRUMENT": inst_type_col,
                    "CLOSE": close_col,
                    "OPEN_INT": oi_col_raw,
                }
            )

            # Math A: Total Open Interest for the asset (Sum all Futures and Options contracts for the stock)
            oi_df = clean_fo.groupby("SYMBOL")["OPEN_INT"].sum().reset_index()
            oi_df.rename(columns={"OPEN_INT": "Open_Interest"}, inplace=True)

            # Math B: Nearest Expiry Futures Cost of Carry
            fut_df = clean_fo[clean_fo["INSTRUMENT"] == "FUT"].copy()
            if not fut_df.empty:
                fut_df = fut_df.sort_values("EXPIRY_DT")
                nearest_fut = fut_df.drop_duplicates(
                    subset=["SYMBOL"], keep="first"
                ).copy()

                nearest_fut["DTE"] = (nearest_fut["EXPIRY_DT"] - target_dt).dt.days
                nearest_fut["DTE"] = nearest_fut["DTE"].replace(
                    0, 1
                )  # Prevent Div by Zero

                # Merge with Spot to calculate
                calc_df = pd.merge(
                    base_df[["SYMBOL", "CLOSE_PRICE"]],
                    nearest_fut[["SYMBOL", "CLOSE", "DTE"]],
                    on="SYMBOL",
                )
                calc_df["Cost_Of_Carry"] = (
                    (
                        (calc_df["CLOSE"] - calc_df["CLOSE_PRICE"])
                        / calc_df["CLOSE_PRICE"]
                    )
                    * (365 / calc_df["DTE"])
                    * 100
                )
                coc_df = calc_df[["SYMBOL", "Cost_Of_Carry"]]

        except Exception as e:
            print(f"    [!] Error processing FO zip for CoC & OI: {e}")

    # Merge CoC and Open Interest into Base
    base_df = pd.merge(base_df, coc_df, on="SYMBOL", how="left")
    base_df = pd.merge(base_df, oi_df, on="SYMBOL", how="left")

    # 4. MAP TO FINAL SCHEMA
    mapped_df = pd.DataFrame(
        {
            "IndicatorName": base_df["SYMBOL"],
            "ReportDate": t_date_str + " 00:00:00",
            "Open": base_df["OPEN_PRICE"],
            "High": base_df["HIGH_PRICE"],
            "Low": base_df["LOW_PRICE"],
            "Close_Value": base_df["CLOSE_PRICE"],
            "Volume": base_df["TTL_TRD_QNTY"],
            "Delivery_Percentage": base_df["DELIV_PER"],
            "Short_Volume": base_df["Short_Volume"],
            "Cost_Of_Carry": base_df["Cost_Of_Carry"],
            "Open_Interest": base_df["Open_Interest"],
            "AssetClass": base_df["AssetClass"],
        }
    )

    return mapped_df


def load_master_short_data():

    print("[*] Building Master Short-Volume Memory Bank...")

    # Search for short selling files in BOTH the root directory and the cache fold
    search_paths = [
        "nse_short_selling_*.csv",
        os.path.join("offline_data_cache", "nse_short_selling_*.csv"),
        os.path.join(CACHE_DIR, "nse_short_selling_*.csv"),
    ]

    short_files = []
    for path in search_paths:
        short_files.extend(glob.glob(path))

    # Remove any accidental duplicates just in case
    short_files = list(set(short_files))

    if not short_files:
        print("    [-] No short selling files found. Short Volume will be NULL.")
        return pd.DataFrame()

    df_list = []
    for file in short_files:
        df = pd.read_csv(file, encoding="latin1")
        df.columns = df.columns.str.strip()
        df_list.append(df)

    # Stitch them all together
    master_sh = pd.concat(df_list, ignore_index=True)

    # Clean the data globally
    master_sh["Quantity"] = (
        master_sh["Quantity"].astype(str).str.replace(",", "").astype(float)
    )
    # format='mixed' handles the 16-JUN-2023 format effortlessly
    master_sh["Date"] = pd.to_datetime(master_sh["Date"], format="mixed").dt.strftime(
        "%Y-%m-%d"
    )

    # Group by Date and Symbol to ensure no duplicates if chunks overlap
    master_sh = master_sh.groupby(["Date", "Symbol"])["Quantity"].sum().reset_index()
    master_sh.rename(
        columns={"Symbol": "SYMBOL", "Quantity": "Short_Volume"}, inplace=True
    )

    print(
        f"    [+] Memory Bank Loaded: {len(master_sh)} historical short records available."
    )
    return master_sh


def run_bulk_bhavcopy_etl():
    """
    Scans the local cache for all available trading days, filters out dates
    before 2015, loads the master memory banks, and executes the ETL sequentially.
    """
    print("\n" + "=" * 50)
    print("INITIATING DYNAMIC BHAVCOPY BULK ETL")
    print("=" * 50)

    # 1. Discover all valid trading days by looking for Cash Bhavcopies
    cash_files = glob.glob(os.path.join(CACHE_DIR, "nse_cash_*.csv"))
    valid_dates = []

    for file in cash_files:
        # Extract the DDMMYYYY from 'nse_cash_DDMMYYYY.csv'
        filename = os.path.basename(file)
        date_str = filename.replace("nse_cash_", "").replace(".csv", "")
        try:
            parsed_date = datetime.strptime(date_str, "%d%m%Y")
            # Enforce the 2015 Horizon Check
            if parsed_date.year >= 2015:
                valid_dates.append(parsed_date)
        except ValueError:
            continue

    # Sort chronologically (oldest to newest)
    valid_dates.sort()

    if not valid_dates:
        print("[-] No valid trading days found post-2015. Aborting.")
        return

    print(f"[*] Discovered {len(valid_dates)} valid trading days to process.")

    # 2. Load the Master Short Volume memory bank ONCE
    master_short_df = load_master_short_data()

    # 3. Execute the Loop
    for target_date in valid_dates:
        print(f"\n--- Processing Date: {target_date.strftime('%Y-%m-%d')} ---")

        ddmmyyyy = target_date.strftime("%d%m%Y")
        mcx_date = target_date.strftime("%Y%m%d")

        mcx_path = os.path.join(CACHE_DIR, f"mcx_bhav_{mcx_date}.json")
        cash_path = os.path.join(CACHE_DIR, f"nse_cash_{ddmmyyyy}.csv")
        # Change it to this in BOTH orchestrator loops:
        fo_path = os.path.join(CACHE_DIR, f"nse_fo_bhav_{ddmmyyyy}.zip")

        mcx_df = parse_mcx_bhavcopy(mcx_path, target_date)
        nse_df = parse_nse_bhavcopies(cash_path, fo_path, master_short_df, target_date)

        master_df = pd.concat([mcx_df, nse_df], ignore_index=True)

        if not master_df.empty:
            push_to_bhavcopy_metrics(master_df)
        else:
            print("    [-] Skipped: No valid data extracted for this date.")


def run_daily_bridge():
    """
    The Delta Bridge: Checks the DB for missing dates, triggers downloads (if missing),
    and parses exactly what is needed to catch up to today.
    """
    print("\n" + "=" * 50)
    print("INITIATING DYNAMIC DELTA BRIDGE (DAILY UPDATE)")
    print("=" * 50)

    # 1. Ask the database what we are missing
    missing_dates = get_missing_dates("market_bhavcopy_metrics")

    if not missing_dates:
        print("[+] No updates needed. Pipeline resting.")
        return

    # 2. Load the Master Short Volume memory bank ONCE for the bridge
    master_short_df = load_master_short_data()

    # 3. Process the exact gap
    for target_date in missing_dates:
        target_dt = datetime.combine(target_date, datetime.min.time())
        date_str_display = target_dt.strftime("%Y-%m-%d")

        print(f"\n--- Bridging Gap: {date_str_display} ---")

        ddmmyyyy = target_dt.strftime("%d%m%Y")
        mcx_date = target_dt.strftime("%Y%m%d")

        mcx_path = os.path.join(CACHE_DIR, f"mcx_bhav_{mcx_date}.json")
        cash_path = os.path.join(CACHE_DIR, f"nse_cash_{ddmmyyyy}.csv")
        # Change it to this in BOTH orchestrator loops:
        fo_path = os.path.join(CACHE_DIR, f"nse_fo_bhav_{ddmmyyyy}.zip")

        if os.path.exists(cash_path):
            mcx_df = parse_mcx_bhavcopy(mcx_path, target_dt)
            nse_df = parse_nse_bhavcopies(
                cash_path, fo_path, master_short_df, target_dt
            )

            master_df = pd.concat([mcx_df, nse_df], ignore_index=True)

            if not master_df.empty:
                push_to_bhavcopy_metrics(master_df)
            else:
                print(
                    f"    [-] Skipped: No valid data extracted for {date_str_display}."
                )
        else:
            print(
                f"    [-] Skipped: No Cash file found for {date_str_display} (Likely a Market Holiday)."
            )


if __name__ == "__main__":
    import sys

    # Simple CLI argument to control the flow
    if len(sys.argv) > 1 and sys.argv[1] == "bulk":
        run_bulk_bhavcopy_etl()  # Run this ONCE to build the 2015-today foundation
    else:
        run_daily_bridge()  # Run this DAILY to catch up
