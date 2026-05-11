import yfinance as yf
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from sqlalchemy import text
from scripts.database import engine


def fetch_hybrid_macro_data(period_days=1000):

    print("Initializing Hybrid Macro Spigots...")
    raw_data_frames = []

    # --- SPIGOT 1: YAHOO FINANCE ---
    yf_tickers = {
        "US_10Y_Yield": "^TNX",
        "Brent_Crude": "BZ=F",
        "USD_INR": "INR=X",
        "US_Dollar_Index": "DX-Y.NYB",
        "Broad_Commodity": "DBC",  # NEW: Beam 7 (Sector-Specific Input Costs)
    }

    for name, ticker in yf_tickers.items():
        try:
            print(f" -> Hitting Yahoo API for {name}...")
            tick = yf.Ticker(ticker)
            hist = tick.history(period="5y")
            if not hist.empty:
                df = hist[["Close"]].copy()
                df.rename(columns={"Close": name}, inplace=True)
                df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
                raw_data_frames.append(df)
        except Exception as e:
            print(f"    [ERROR] Yahoo API failed for {ticker}: {e}")

    # --- SPIGOT 2: TRADINGVIEW ---
    try:
        print(" -> Hitting TradingView API for India_10Y_Yield and India_CPI...")
        tv = TvDatafeed()

        # 1. Fetch India 10Y Yield
        tv_data_10y = tv.get_hist(
            symbol="IN10Y",
            exchange="TVC",
            interval=Interval.in_daily,
            n_bars=period_days,
        )

        if tv_data_10y is not None and not tv_data_10y.empty:
            df_in10y = tv_data_10y[["close"]].copy()
            df_in10y.rename(columns={"close": "India_10Y_Yield"}, inplace=True)
            df_in10y.index = pd.to_datetime(df_in10y.index).normalize()
            raw_data_frames.append(df_in10y)

        # 2. NEW: Fetch India CPI (Beam 8: Inflation)
        tv_data_cpi = tv.get_hist(
            symbol="INCPI",
            exchange="ECONOMICS",
            interval=Interval.in_daily,
            n_bars=period_days,
        )

        if tv_data_cpi is not None and not tv_data_cpi.empty:
            df_cpi = tv_data_cpi[["close"]].copy()
            df_cpi.rename(columns={"close": "India_CPI"}, inplace=True)
            df_cpi.index = pd.to_datetime(df_cpi.index).normalize()
            raw_data_frames.append(df_cpi)

    except Exception as e:
        print(f"    [ERROR] TradingView API failed: {e}")

    # --- MERGE & SPREAD CALCULATION ---
    if raw_data_frames:
        print(
            "\nMerging datasets, applying ffill for monthly CPI, and calculating Yield Spread..."
        )
        macro_df = pd.concat(raw_data_frames, axis=1)

        # Forward fill ensures monthly CPI data stretches across daily rows
        macro_df.ffill(inplace=True)
        macro_df.dropna(inplace=True)

        macro_df["Yield_Spread"] = (
            macro_df["India_10Y_Yield"] - macro_df["US_10Y_Yield"]
        )
        return macro_df

    return pd.DataFrame()


def push_to_database(df):

    print("\nReshaping data for database ingestion...")

    # Reshape from "Wide" (Dates as rows, Indicators as columns)
    # to "Long" (IndicatorName, ReportDate, Value)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "ReportDate"}, inplace=True)

    melted_df = df.melt(
        id_vars=["ReportDate"], var_name="IndicatorName", value_name="Value"
    )

    # Format the Date as a string for the DB
    melted_df["ReportDate"] = melted_df["ReportDate"].dt.strftime("%Y-%m-%d")
    melted_df.dropna(subset=["Value"], inplace=True)

    records = melted_df.to_dict(orient="records")
    print(f"Pushing {len(records)} daily records to the macro_indicators table...")

    upsert_query = text("""
        INSERT INTO macro_indicators ("IndicatorName", "ReportDate", "Value")
        VALUES (:IndicatorName, :ReportDate, :Value)
        ON CONFLICT ("IndicatorName", "ReportDate") 
        DO UPDATE SET "Value" = EXCLUDED."Value";
    """)

    try:
        with engine.begin() as conn:
            for record in records:
                conn.execute(upsert_query, record)
        print("SUCCESS: MACRO DATA UPSERTED TO DATABASE")
    except Exception as e:
        print(f"DATABASE ERROR\n{e}")


def run_macro_pipeline(period_days=1500):
    print(f"\nStarting Macro Pipeline for {period_days} days...")
    final_df = fetch_hybrid_macro_data(period_days=period_days)

    if not final_df.empty:
        push_to_database(final_df)
        return True, len(final_df)
    else:
        print("Pipeline aborted: No data extracted.")
        return False, 0


if __name__ == "__main__":
    success, rows = run_macro_pipeline()
