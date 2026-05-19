import yfinance as yf
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from sqlalchemy import text
from scripts.database import engine
import time


def register_discovered_tickers(tickers, data_source="auto"):

    if not tickers:
        return

    print(f"\nRegistering discovered client tickers from {data_source.upper()}...")
    records = []

    for ticker in tickers:
        if data_source in ["screener", "indianapi"]:
            exchange = "NSE"
        elif ticker.endswith(".NS"):
            exchange = "NSE"
        elif ticker.endswith(".BO"):
            exchange = "BSE"
        else:
            exchange = "US"

        records.append(
            {
                "Ticker": ticker,
                "IndicatorName": ticker,
                "TargetTable": "market_pricing_daily",  # Explicitly routing to the new table
                "AssetClass": "Equity",
                "Exchange": exchange,
                "IsActive": True,
                "Description": f"Auto-discovered via {data_source.upper()} ETL",
            }
        )

    upsert_query = text("""
        INSERT INTO market_metadata ("Ticker", "IndicatorName", "TargetTable", "AssetClass", "Exchange", "IsActive", "Description")
        VALUES (:Ticker, :IndicatorName, :TargetTable, :AssetClass, :Exchange, :IsActive, :Description)
        ON CONFLICT ("Ticker") 
        DO UPDATE SET
            "TargetTable" = EXCLUDED."TargetTable",
            "Exchange" = EXCLUDED."Exchange",
            "Description" = EXCLUDED."Description"; 
    """)

    try:
        with engine.begin() as conn:
            for record in records:
                conn.execute(upsert_query, record)
        print(f"SUCCESS: {len(records)} client tickers verified in market_metadata.")
    except Exception as e:
        print(f"    [ERROR] Failed to register tickers: {e}")


def get_active_equities():
    """Fetches dynamically discovered equities and their target tables from the DB."""
    query = """
        SELECT "Ticker", "IndicatorName", "TargetTable"
        FROM market_metadata 
        WHERE "IsActive" = true AND "AssetClass" = 'Equity';
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        print(f"[ERROR] Failed to fetch equities from DB: {e}")
        return pd.DataFrame()


def get_yf_period(interval):
    """Returns the maximum allowed lookback period for yfinance intraday data."""
    if interval == "1m":
        return "7d"
    elif interval in ["5m", "15m", "30m"]:
        return "60d"
    elif interval == "1h":
        return "730d"
    return "max"  # For 1d


def get_tv_interval(interval_str):
    """Maps string intervals to tvDatafeed Interval enums."""
    mapping = {
        "1d": Interval.in_daily,
        "1h": Interval.in_1_hour,
        "30m": Interval.in_30_minute,
        "5m": Interval.in_5_minute,
        "1m": Interval.in_1_minute,
    }
    return mapping.get(interval_str, Interval.in_daily)


def fetch_hybrid_macro_data(
    intervals=["1d", "1h", "30m", "5m", "1m"], period_days=1000
):
    print("Initializing Hybrid Multi-Timeframe Spigots...")
    raw_data_frames = []
    req_cols = ["Open", "High", "Low", "Close_Value", "Volume", "TargetTable"]

    # --- 1. DEFINE TARGETS WITH THEIR DESTINATION TABLES ---
    yf_targets = {
        # Dictionary format: Name: (Ticker, TargetTable)
        "US_10Y_Yield": ("^TNX", "macro_indicators"),
        "Brent_Crude": ("BZ=F", "macro_indicators"),
        "USD_INR": ("INR=X", "macro_indicators"),
        "US_Dollar_Index": ("DX-Y.NYB", "macro_indicators"),
        "Broad_Commodity": ("DBC", "macro_indicators"),
        "US_VIX": ("^VIX", "macro_indicators"),
        "Nifty_50": ("^NSEI", "macro_indicators"),
    }

    equities_df = get_active_equities()
    for _, row in equities_df.iterrows():
        yf_targets[row["IndicatorName"]] = (row["Ticker"], row["TargetTable"])

    tv_targets = {
        # Dictionary format: Name: (Symbol, Exchange, TargetTable)
        "India_10Y_Yield": ("IN10Y", "TVC", "macro_indicators"),
        "India_CPI": ("INCPI", "ECONOMICS", "macro_indicators"),
        "India_VIX": ("INDIAVIX", "NSE", "macro_indicators"),
    }

    # --- 2. FETCH YAHOO FINANCE ---
    for name, (ticker, target_table) in yf_targets.items():
        for interval in intervals:
            try:
                print(f" -> YF Fetch: {name} | {interval}...")
                tick = yf.Ticker(ticker)
                period = get_yf_period(interval)

                hist = tick.history(period=period, interval=interval)

                if not hist.empty:
                    df = hist.copy()
                    df.index = pd.to_datetime(df.index).tz_localize(None)
                    df.index.name = "ReportDate"
                    df.reset_index(inplace=True)

                    df.rename(columns={"Close": "Close_Value"}, inplace=True)

                    final_name = name if interval == "1d" else f"{name}_{interval}"
                    df["IndicatorName"] = final_name
                    df["TargetTable"] = target_table  # Tag the destination

                    for col in req_cols:
                        if col not in df.columns:
                            df[col] = None

                    raw_data_frames.append(
                        df[["IndicatorName", "ReportDate"] + req_cols]
                    )
            except Exception as e:
                print(f"    [ERROR] YF failed for {ticker} ({interval}): {e}")
            time.sleep(0.5)

    # --- 3. FETCH TRADINGVIEW ---
    try:
        tv = TvDatafeed()

        for name, (symbol, exchange, target_table) in tv_targets.items():
            for interval in intervals:
                if "CPI" in name and interval != "1d":
                    continue

                try:
                    print(f" -> TV Fetch: {name} | {interval}...")
                    tv_interval = get_tv_interval(interval)

                    tv_data = tv.get_hist(
                        symbol=symbol,
                        exchange=exchange,
                        interval=tv_interval,
                        n_bars=1000,
                    )

                    if tv_data is not None and not tv_data.empty:
                        df = tv_data.copy()
                        df.index = pd.to_datetime(df.index)
                        df.index.name = "ReportDate"
                        df.reset_index(inplace=True)

                        df.rename(
                            columns={
                                "open": "Open",
                                "high": "High",
                                "low": "Low",
                                "close": "Close_Value",
                                "volume": "Volume",
                            },
                            inplace=True,
                        )

                        final_name = name if interval == "1d" else f"{name}_{interval}"
                        df["IndicatorName"] = final_name
                        df["TargetTable"] = target_table  # Tag the destination

                        for col in req_cols:
                            if col not in df.columns:
                                df[col] = None

                        raw_data_frames.append(
                            df[["IndicatorName", "ReportDate"] + req_cols]
                        )
                except Exception as e:
                    print(f"    [ERROR] TV failed for {symbol} ({interval}): {e}")
                time.sleep(1)
    except Exception as e:
        print(f"    [CRITICAL] TradingView connection failed entirely: {e}")

    # --- 4. ASSEMBLE & SYNTHESIZE YIELD SPREAD ---
    if raw_data_frames:
        macro_df = pd.concat(raw_data_frames, ignore_index=True)

        print("\nCalculating Yield Spreads across timeframes...")
        spread_frames = []

        for interval in intervals:
            us_name = "US_10Y_Yield" if interval == "1d" else f"US_10Y_Yield_{interval}"
            in_name = (
                "India_10Y_Yield" if interval == "1d" else f"India_10Y_Yield_{interval}"
            )

            df_us = macro_df[macro_df["IndicatorName"] == us_name].set_index(
                "ReportDate"
            )
            df_in = macro_df[macro_df["IndicatorName"] == in_name].set_index(
                "ReportDate"
            )

            if not df_us.empty and not df_in.empty:
                aligned = df_in[["Close_Value"]].join(
                    df_us[["Close_Value"]], rsuffix="_us", how="outer"
                )
                aligned.ffill(inplace=True)
                aligned.dropna(inplace=True)

                spread = aligned["Close_Value"] - aligned["Close_Value_us"]
                spread_df = spread.to_frame("Close_Value").reset_index()
                spread_df["IndicatorName"] = (
                    "Yield_Spread" if interval == "1d" else f"Yield_Spread_{interval}"
                )
                spread_df["TargetTable"] = (
                    "macro_indicators"  # Spreads go to macro table
                )

                for col in ["Open", "High", "Low", "Volume"]:
                    spread_df[col] = None

                spread_frames.append(spread_df)

        if spread_frames:
            macro_df = pd.concat([macro_df] + spread_frames, ignore_index=True)

        return macro_df.dropna(subset=["Close_Value"])

    return pd.DataFrame()


def push_to_database(df):
    print("\nFormatting multi-timeframe OHLCV data for routing...")

    df["ReportDate"] = df["ReportDate"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.where(pd.notnull(df), None)

    # --- THE ROUTER: Group the dataframe by TargetTable and push dynamically ---
    for table_name, group_df in df.groupby("TargetTable"):

        # Drop the TargetTable column before converting to a dictionary for SQL
        clean_df = group_df.drop(columns=["TargetTable"])
        records = clean_df.to_dict(orient="records")

        print(f"Pushing {len(records)} records to {table_name}...")

        # Dynamically inject the table name into the SQL string
        upsert_query = text(f"""
            INSERT INTO {table_name} ("IndicatorName", "ReportDate", "Open", "High", "Low", "Close_Value", "Volume")
            VALUES (:IndicatorName, :ReportDate, :Open, :High, :Low, :Close_Value, :Volume)
            ON CONFLICT ("IndicatorName", "ReportDate") 
            DO UPDATE SET 
                "Open" = EXCLUDED."Open",
                "High" = EXCLUDED."High",
                "Low" = EXCLUDED."Low",
                "Close_Value" = EXCLUDED."Close_Value",
                "Volume" = EXCLUDED."Volume";
        """)

        try:
            with engine.begin() as conn:
                for record in records:
                    conn.execute(upsert_query, record)
            print(f"SUCCESS: DATA UPSERTED TO {table_name.upper()}")
        except Exception as e:
            print(f"DATABASE ERROR ON {table_name.upper()}\n{e}")


def run_macro_pipeline(period_days=3000):
    print(f"\nStarting Macro Pipeline...")

    # We define the intervals we want the engine to fetch
    target_intervals = ["1d", "1h", "30m", "5m", "1m"]
    final_df = fetch_hybrid_macro_data(
        intervals=target_intervals, period_days=period_days
    )

    if not final_df.empty:
        push_to_database(final_df)
        return True, len(final_df)
    else:
        print("Pipeline aborted: No data extracted.")
        return False, 0


if __name__ == "__main__":
    success, rows = run_macro_pipeline()
