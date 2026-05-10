import yfinance as yf
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from sqlalchemy import text


def fetch_hybrid_macro_data(period_days=1000):

    print("Initializing Hybrid Macro Spigots...")
    raw_data_frames = []

    # --- SPIGOT 1: YAHOO FINANCE ---
    yf_tickers = {
        "US_10Y_Yield": "^TNX",
        "Brent_Crude": "BZ=F",
        "USD_INR": "INR=X",
        "US_Dollar_Index": "DX-Y.NYB",
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
        print(" Hitting TradingView API for India_10Y_Yield...")
        tv = TvDatafeed()
        tv_data = tv.get_hist(
            symbol="IN10Y",
            exchange="TVC",
            interval=Interval.in_daily,
            n_bars=period_days,
        )

        if tv_data is not None and not tv_data.empty:
            df_in10y = tv_data[["close"]].copy()
            df_in10y.rename(columns={"close": "India_10Y_Yield"}, inplace=True)
            df_in10y.index = pd.to_datetime(df_in10y.index).normalize()
            raw_data_frames.append(df_in10y)
    except Exception as e:
        print(f"    [ERROR] TradingView API failed: {e}")

    # --- MERGE & SPREAD CALCULATION ---
    if raw_data_frames:
        print("\nMerging datasets and calculating Yield Spread...")
        macro_df = pd.concat(raw_data_frames, axis=1)
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


if __name__ == "__main__":
    final_df = fetch_hybrid_macro_data(
        period_days=1500
    )  # Roughly 5 years of trading days
    if not final_df.empty:
        push_to_database(final_df)
    else:
        print("Pipeline aborted: No data extracted.")
