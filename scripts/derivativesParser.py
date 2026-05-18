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
        print("    [✔] SUCCESS: Derivatives Matrix successfully updated.")
    except Exception as e:
        print(f"    [!] DATABASE ERROR:\n{e}")


def parse_derivatives_zip(target_date):
    """Extracts the zip, calculates Greeks/Ratios, and builds the matrix."""
    target_dt = pd.to_datetime(target_date)
    ddmmyyyy = target_dt.strftime("%d%m%Y")
    fo_path = os.path.join(CACHE_DIR, f"nse_fo_bhav_{ddmmyyyy}.zip")

    if not os.path.exists(fo_path):
        print(f"[-] No F&O Zip found for {target_dt.strftime('%Y-%m-%d')}")
        return

    print(f"\n--- Matrixing Derivatives for {target_dt.strftime('%Y-%m-%d')} ---")

    try:
        inner_csv_name = f"fo{target_dt.strftime('%d%b%Y').upper()}bhav.csv"
        with zipfile.ZipFile(fo_path, "r") as z:
            with z.open(inner_csv_name) as f:
                fo_df = pd.read_csv(f, encoding="latin1")
    except Exception as e:
        print(f"    [!] Error reading F&O zip: {e}")
        return

    fo_df.columns = fo_df.columns.str.strip()

    # 1. Base Formatting
    fo_df["SYMBOL"] = fo_df["SYMBOL"].str.strip()
    fo_df["EXPIRY_DT"] = pd.to_datetime(fo_df["EXPIRY_DT"])

    # Create the core raw matrix
    raw_df = pd.DataFrame(
        {
            "Ticker": fo_df["SYMBOL"],
            "ReportDate": target_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "ExpiryDate": fo_df["EXPIRY_DT"],
            "InstrumentType": fo_df.apply(
                lambda x: (
                    "FUT" if "FUT" in x["INSTRUMENT"] else x["OPTION_TYP"].strip()
                ),
                axis=1,
            ),
            "StrikePrice": fo_df["STRIKE_PR"].astype(float),
            "Close_Price": fo_df["CLOSE"],
            "Open_Interest": fo_df["OPEN_INT"],
            "Change_In_OI": fo_df["CHG_IN_OI"],
            "Volume": fo_df["CONTRACTS"],
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

    agg_df = pd.DataFrame(index=pcr_pivot.index.droplevel(-1).drop_duplicates())
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

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()

    if args.start and args.end:
        start_dt = pd.to_datetime(args.start)
        end_dt = pd.to_datetime(args.end)
        current_date = start_dt
        while current_date <= end_dt:
            if current_date.weekday() < 5:
                parse_derivatives_zip(current_date)
            current_date += pd.Timedelta(days=1)
    else:
        # Test mode: run for yesterday if no args provided
        test_date = pd.to_datetime("today") - pd.Timedelta(days=1)
        if test_date.weekday() >= 5:  # If weekend, push to Friday
            test_date -= pd.Timedelta(days=test_date.weekday() - 4)
        parse_derivatives_zip(test_date)
