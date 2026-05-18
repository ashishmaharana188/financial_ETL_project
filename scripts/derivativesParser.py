import os
import zipfile
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from database import engine

CACHE_DIR = "offline_data_cache/master_archives"


def get_yesterday_pcr(target_date):
    """
    Fetches the OI_PCR from the most recent trading day prior to the target_date
    so we can calculate the exact Change_In_OI_PCR.
    """
    query = text("""
        WITH LastDate AS (
            SELECT MAX("ReportDate") as max_date 
            FROM derivatives_matrix 
            WHERE "ReportDate" < :t_date
        )
        SELECT "Ticker", "ExpiryDate", "OI_PCR"
        FROM derivatives_matrix
        WHERE "ReportDate" = (SELECT max_date FROM LastDate)
          AND "InstrumentType" = 'AGGREGATE'
    """)

    try:
        with engine.connect() as conn:
            df_yest = pd.read_sql(query, conn, params={"t_date": target_date})
            if not df_yest.empty:
                # Convert ExpiryDate to datetime for safe merging
                df_yest["ExpiryDate"] = pd.to_datetime(df_yest["ExpiryDate"])
                return df_yest
    except Exception as e:
        print(f"    [!] Warning: Could not fetch yesterday's PCR: {e}")

    return pd.DataFrame(columns=["Ticker", "ExpiryDate", "OI_PCR"])


def push_to_derivatives_matrix(df):
    if df.empty:
        return

    print(f"    [DB PUSH] Upserting {len(df)} records to derivatives_matrix...")
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")

    upsert_query = text("""
        INSERT INTO derivatives_matrix (
            "Ticker", "ReportDate", "ExpiryDate", "InstrumentType", "StrikePrice",
            "Close_Price", "Open_Interest", "Change_In_OI", "Volume",
            "OI_PCR", "Change_In_OI_PCR", "Volume_PCR", "Rollover_Percentage"
        )
        VALUES (
            :Ticker, :ReportDate, :ExpiryDate, :InstrumentType, :StrikePrice,
            :Close_Price, :Open_Interest, :Change_In_OI, :Volume,
            :OI_PCR, :Change_In_OI_PCR, :Volume_PCR, :Rollover_Percentage
        )
        ON CONFLICT ("Ticker", "ReportDate", "ExpiryDate", "InstrumentType", "StrikePrice") 
        DO UPDATE SET 
            "Close_Price" = EXCLUDED."Close_Price",
            "Open_Interest" = EXCLUDED."Open_Interest",
            "Change_In_OI" = EXCLUDED."Change_In_OI",
            "Volume" = EXCLUDED."Volume",
            "OI_PCR" = EXCLUDED."OI_PCR",
            "Change_In_OI_PCR" = EXCLUDED."Change_In_OI_PCR",
            "Volume_PCR" = EXCLUDED."Volume_PCR",
            "Rollover_Percentage" = EXCLUDED."Rollover_Percentage";
    """)

    try:
        with engine.begin() as conn:
            for record in records:
                conn.execute(upsert_query, record)
        print(" SUCCESS: Derivatives Matrix successfully updated.")
    except Exception as e:
        print(f"DATABASE ERROR:\n{e}")


def parse_derivatives_zip(target_date):
    """Extracts the zip, calculates Greeks/Ratios, and builds the matrix."""
    target_dt = pd.to_datetime(target_date)
    ddmmyyyy = target_dt.strftime("%d%m%Y")
    fo_path = os.path.join(CACHE_DIR, f"nse_fo_bhav_{ddmmyyyy}.zip")

    if not os.path.exists(fo_path):
        print(f"No F&O Zip found for {target_dt.strftime('%Y-%m-%d')}")
        return

    print(f"\n--- Matrixing Derivatives for {target_dt.strftime('%Y-%m-%d')} ---")

    try:
        with zipfile.ZipFile(fo_path, "r") as z:
            # Look inside the zip and find the exact name of the CSV file dynamically
            file_list = z.namelist()

            # Find the file that looks like a bhavcopy CSV (ignores case sensitivity)
            csv_filename = next(
                (
                    name
                    for name in file_list
                    if name.lower().endswith(".csv") and "bhav" in name.lower()
                ),
                None,
            )

            # Fallback: if 'bhav' isn't in the name, just grab whatever CSV is in there
            if not csv_filename:
                csv_filename = next(
                    (name for name in file_list if name.lower().endswith(".csv")), None
                )

            if not csv_filename:
                print(f"    [!] Error: No valid CSV found inside {fo_path}")
                return

            with z.open(csv_filename) as f:
                fo_df = pd.read_csv(f, encoding="latin1")

    except Exception as e:
        print(f"    [!] Error reading F&O zip: {e}")
        return

    fo_df.columns = (
        fo_df.columns.str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .str.upper()
    )

    # BACKWARD COMPATIBILITY: Map old historical headers to the new NSE format
    column_mapping = {
        "SYMBOL": "TCKRSYMB",
        "EXPIRY_DT": "XPRYDT",
        "STRIKE_PR": "STRKPRIC",
        "CLOSE": "CLSPRIC",
        "OPEN_INT": "OPNINTRST",
        "CHG_IN_OI": "CHNGINOPNINTRST",
        "CONTRACTS": "TTLTRADGVOL",
        "OPTION_TYP": "OPTNTP",
    }
    fo_df.rename(columns=column_mapping, inplace=True)

    # 1. Base Formatting
    fo_df["TCKRSYMB"] = fo_df["TCKRSYMB"].str.strip()
    fo_df["XPRYDT"] = pd.to_datetime(fo_df["XPRYDT"])

    # Create the core raw matrix
    raw_df = pd.DataFrame(
        {
            "Ticker": fo_df["TCKRSYMB"],
            "ReportDate": target_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "ExpiryDate": fo_df["XPRYDT"],
            "InstrumentType": fo_df.apply(
                lambda x: (
                    str(x["OPTNTP"]).strip()
                    if pd.notna(x["OPTNTP"])
                    and str(x["OPTNTP"]).strip() in ["CE", "PE"]
                    else "FUT"
                ),
                axis=1,
            ),
            "StrikePrice": pd.to_numeric(fo_df["STRKPRIC"], errors="coerce").fillna(
                0.0
            ),
            "Close_Price": fo_df["CLSPRIC"],
            "Open_Interest": fo_df["OPNINTRST"],
            "Change_In_OI": fo_df["CHNGINOPNINTRST"],
            "Volume": fo_df["TTLTRADGVOL"],
        }
    )

    # Ensure FUT strikes are mathematically exactly 0.0
    raw_df.loc[raw_df["InstrumentType"] == "FUT", "StrikePrice"] = 0.0

    # 2. Calculating PCR (Grouped by Ticker & Expiry)
    print("    -> Calculating Option PCRs...")
    options_df = raw_df[raw_df["InstrumentType"].isin(["CE", "PE"])]

    # Pivot to get CE and PE totals side-by-side
    pcr_pivot = (
        options_df.groupby(["Ticker", "ExpiryDate", "InstrumentType"])[
            ["Open_Interest", "Volume"]
        ]
        .sum()
        .unstack()
    )

    agg_df = pd.DataFrame(index=pcr_pivot.index)
    agg_df["OI_PCR"] = pcr_pivot[("Open_Interest", "PE")] / pcr_pivot[
        ("Open_Interest", "CE")
    ].replace(0, np.nan)
    agg_df["Volume_PCR"] = pcr_pivot[("Volume", "PE")] / pcr_pivot[
        ("Volume", "CE")
    ].replace(0, np.nan)
    agg_df = agg_df.reset_index()

    # 3. Calculating Delta PCR (Change from yesterday)
    yest_pcr_df = get_yesterday_pcr(target_dt.strftime("%Y-%m-%d"))

    if not yest_pcr_df.empty:
        agg_df = pd.merge(
            agg_df,
            yest_pcr_df,
            on=["Ticker", "ExpiryDate"],
            how="left",
            suffixes=("", "_yest"),
        )
        agg_df["Change_In_OI_PCR"] = agg_df["OI_PCR"] - agg_df["OI_PCR_yest"]
        agg_df.drop(columns=["OI_PCR_yest"], inplace=True)
    else:
        agg_df["Change_In_OI_PCR"] = None

    # 4. Calculating Futures Rollover %
    print("    -> Calculating Futures Rollover...")
    fut_df = raw_df[raw_df["InstrumentType"] == "FUT"].copy()

    # Get total OI for all futures of a given ticker
    total_fut_oi = (
        fut_df.groupby("Ticker")["Open_Interest"].sum().reset_index(name="Total_FUT_OI")
    )

    # Find the nearest expiry for each ticker
    nearest_expiry = fut_df.loc[fut_df.groupby("Ticker")["ExpiryDate"].idxmin()]

    # Merge them to calculate rollover: (Total OI - Current Expiry OI) / Total OI * 100
    rollover_calc = pd.merge(
        nearest_expiry[["Ticker", "ExpiryDate", "Open_Interest"]],
        total_fut_oi,
        on="Ticker",
    )
    rollover_calc["Rollover_Percentage"] = (
        (rollover_calc["Total_FUT_OI"] - rollover_calc["Open_Interest"])
        / rollover_calc["Total_FUT_OI"].replace(0, np.nan)
    ) * 100

    # 5. Merge Rollover into the Aggregate DataFrame
    agg_df = pd.merge(
        agg_df,
        rollover_calc[["Ticker", "ExpiryDate", "Rollover_Percentage"]],
        on=["Ticker", "ExpiryDate"],
        how="left",
    )

    # 6. Format Aggregate Rows to match DB Schema
    agg_df["ReportDate"] = target_dt.strftime("%Y-%m-%d %H:%M:%S")
    agg_df["InstrumentType"] = "AGGREGATE"
    agg_df["StrikePrice"] = 0.0
    agg_df["Close_Price"] = None
    agg_df["Open_Interest"] = None
    agg_df["Change_In_OI"] = None
    agg_df["Volume"] = None

    # 7. Combine Raw Strike Rows with the newly calculated Aggregate Rows
    final_matrix_df = pd.concat([raw_df, agg_df], ignore_index=True)

    push_to_derivatives_matrix(final_matrix_df)


if __name__ == "__main__":
    import argparse
    import re

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()

    # Scenario A: Frontend Dashboard Trigger via Orchestrator (Delta Bridge)
    if args.start and args.end:
        start_dt = pd.to_datetime(args.start)
        end_dt = pd.to_datetime(args.end)
        current_date = start_dt
        while current_date <= end_dt:
            if current_date.weekday() < 5:
                parse_derivatives_zip(current_date)
            current_date += pd.Timedelta(days=1)

    # Scenario B: Manual Direct Execution - Rebuild/Parse ALL files in local cache
    else:
        print(
            "[*] Manual Override: Scanning local cache for ALL downloaded F&O zips..."
        )

        if not os.path.exists(CACHE_DIR):
            print(f"Cache directory {CACHE_DIR} does not exist.")
        else:
            # Look for filenames matching nse_fo_bhav_DDMMYYYY.zip
            zip_files = [
                f
                for f in os.listdir(CACHE_DIR)
                if f.startswith("nse_fo_bhav_") and f.endswith(".zip")
            ]

            if not zip_files:
                print("No downloaded F&O zip files found in the offline cache.")
            else:
                # Extract dates from the filenames to parse them in chronological order
                found_dates = []
                for file_name in zip_files:
                    match = re.search(
                        r"nse_fo_bhav_(\d{2})(\d{2})(\d{4})\.zip", file_name
                    )
                    if match:
                        day, month, year = match.groups()
                        dt = datetime(int(year), int(month), int(day))
                        found_dates.append(dt)

                print(
                    f"[+] Found {len(found_dates)} historical zips locally. Rebuilding derivatives matrix..."
                )

                # Sort chronologically so yesterday's PCR values align for Change_In_OI_PCR calculations
                for current_date in sorted(found_dates):
                    parse_derivatives_zip(current_date)

                print("\nHISTORICAL MATRIX PARSING COMPLETE.")
