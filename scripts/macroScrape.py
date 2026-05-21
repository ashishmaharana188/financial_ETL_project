import yfinance as yf
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from sqlalchemy import text
from scripts.database import engine
import time
from datetime import datetime


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
                "TargetTable": "global_assets",  # General classification
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


def get_active_global_assets():
    """Fetches dynamically discovered FOREIGN equities/assets. Ignores NSE/BSE to prevent overlap with unified_market_master."""
    query = """
        SELECT "Ticker", "AssetClass"
        FROM market_metadata 
        WHERE "IsActive" = true 
        AND "Exchange" NOT IN ('NSE', 'BSE');
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        print(f"[ERROR] Failed to fetch equities from DB: {e}")
        return pd.DataFrame()


def get_yf_period(interval):
    if interval == "1m":
        return "7d"
    elif interval in ["5m", "15m", "30m"]:
        return "60d"
    elif interval == "1h":
        return "730d"
    return "max"


def get_tv_interval(interval_str):
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

    # Standardized internal structure before routing
    req_cols = ["Open", "High", "Low", "Close", "Volume"]

    # --- 1. DEFINE TARGETS (Macro Indicators) ---
    yf_macro = {
        "US_10Y_Yield": "^TNX",
        "Brent_Crude": "BZ=F",
        "USD_INR": "INR=X",
        "US_Dollar_Index": "DX-Y.NYB",
        "Broad_Commodity": "DBC",
        "US_VIX": "^VIX",
        "Nifty_50": "^NSEI",  # Nifty 50 acts as a macro weather indicator here
    }

    tv_macro = {
        "India_10Y_Yield": ("IN10Y", "TVC"),
        "India_CPI": ("INCPI", "ECONOMICS"),
        "India_VIX": ("INDIAVIX", "NSE"),
    }

    # --- 2. FETCH MACRO (YF) ---
    for name, ticker in yf_macro.items():
        for interval in intervals:
            try:
                print(f" -> YF Macro Fetch: {name} | {interval}...")
                tick = yf.Ticker(ticker)
                hist = tick.history(period=get_yf_period(interval), interval=interval)

                if not hist.empty:
                    df = hist.copy()
                    df.index = pd.to_datetime(df.index).tz_localize(None)
                    df.index.name = "ReportDate"
                    df.reset_index(inplace=True)

                    df["EntityName"] = name
                    df["Timeframe"] = interval
                    df["Category"] = "MACRO"
                    df["AssetClass"] = None

                    for col in req_cols:
                        if col not in df.columns:
                            df[col] = None

                    raw_data_frames.append(
                        df[
                            [
                                "EntityName",
                                "ReportDate",
                                "Timeframe",
                                "Category",
                                "AssetClass",
                            ]
                            + req_cols
                        ]
                    )
            except Exception as e:
                print(f"    [ERROR] YF failed for {ticker} ({interval}): {e}")
            time.sleep(0.5)

    # --- 3. FETCH GLOBAL ASSETS (YF) ---
    global_assets_df = get_active_global_assets()
    for _, row in global_assets_df.iterrows():
        ticker = row["Ticker"]
        asset_class = row["AssetClass"]
        for interval in intervals:
            try:
                print(f" -> YF Asset Fetch: {ticker} | {interval}...")
                tick = yf.Ticker(ticker)
                hist = tick.history(period=get_yf_period(interval), interval=interval)

                if not hist.empty:
                    df = hist.copy()
                    df.index = pd.to_datetime(df.index).tz_localize(None)
                    df.index.name = "ReportDate"
                    df.reset_index(inplace=True)

                    df["EntityName"] = ticker
                    df["Timeframe"] = interval
                    df["Category"] = "ASSET"
                    df["AssetClass"] = asset_class

                    for col in req_cols:
                        if col not in df.columns:
                            df[col] = None

                    raw_data_frames.append(
                        df[
                            [
                                "EntityName",
                                "ReportDate",
                                "Timeframe",
                                "Category",
                                "AssetClass",
                            ]
                            + req_cols
                        ]
                    )
            except Exception as e:
                print(f"    [ERROR] YF failed for {ticker} ({interval}): {e}")
            time.sleep(0.5)

    # --- 4. FETCH MACRO (TV) ---
    try:
        tv = TvDatafeed()
        for name, (symbol, exchange) in tv_macro.items():
            for interval in intervals:
                if "CPI" in name and interval != "1d":
                    continue

                try:
                    print(f" -> TV Macro Fetch: {name} | {interval}...")
                    tv_data = tv.get_hist(
                        symbol=symbol,
                        exchange=exchange,
                        interval=get_tv_interval(interval),
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
                                "close": "Close",
                                "volume": "Volume",
                            },
                            inplace=True,
                        )

                        df["EntityName"] = name
                        df["Timeframe"] = interval
                        df["Category"] = "MACRO"
                        df["AssetClass"] = None

                        for col in req_cols:
                            if col not in df.columns:
                                df[col] = None

                        raw_data_frames.append(
                            df[
                                [
                                    "EntityName",
                                    "ReportDate",
                                    "Timeframe",
                                    "Category",
                                    "AssetClass",
                                ]
                                + req_cols
                            ]
                        )
                except Exception as e:
                    print(f"    [ERROR] TV failed for {symbol} ({interval}): {e}")
                time.sleep(1)
    except Exception as e:
        print(f"    [CRITICAL] TradingView connection failed entirely: {e}")

    # --- 5. SYNTHESIZE YIELD SPREAD ---
    if raw_data_frames:
        master_df = pd.concat(raw_data_frames, ignore_index=True)
        print("\nCalculating Yield Spreads across timeframes...")
        spread_frames = []

        for interval in intervals:
            df_us = master_df[
                (master_df["EntityName"] == "US_10Y_Yield")
                & (master_df["Timeframe"] == interval)
            ].set_index("ReportDate")
            df_in = master_df[
                (master_df["EntityName"] == "India_10Y_Yield")
                & (master_df["Timeframe"] == interval)
            ].set_index("ReportDate")

            if not df_us.empty and not df_in.empty:
                aligned = df_in[["Close"]].join(
                    df_us[["Close"]], rsuffix="_us", how="outer"
                )
                aligned.ffill(inplace=True)
                aligned.dropna(inplace=True)

                spread = aligned["Close"] - aligned["Close_us"]
                spread_df = spread.to_frame("Close").reset_index()

                spread_df["EntityName"] = "Yield_Spread"
                spread_df["Timeframe"] = interval
                spread_df["Category"] = "MACRO"
                spread_df["AssetClass"] = None

                for col in ["Open", "High", "Low", "Volume"]:
                    spread_df[col] = None
                spread_frames.append(spread_df)

        if spread_frames:
            master_df = pd.concat([master_df] + spread_frames, ignore_index=True)

        return master_df.dropna(subset=["Close"])

    return pd.DataFrame()


def push_to_database(df):
    print("\nRouting Data to strict Time-Series Architectures...")

    # Separate Daily vs Intraday
    daily_df = df[df["Timeframe"] == "1d"].copy()
    intraday_df = df[df["Timeframe"] != "1d"].copy()

    # Cast Daily Dates to pure YYYY-MM-DD
    daily_df["ReportDate"] = pd.to_datetime(daily_df["ReportDate"]).dt.date
    intraday_df["ReportDate"] = pd.to_datetime(intraday_df["ReportDate"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    daily_df = daily_df.where(pd.notnull(daily_df), None)
    intraday_df = intraday_df.where(pd.notnull(intraday_df), None)

    # ---------------------------------------------------------
    # 1. MACRO DAILY LEDGER
    # ---------------------------------------------------------
    macro_daily = daily_df[daily_df["Category"] == "MACRO"].copy()
    if not macro_daily.empty:
        records = macro_daily.rename(
            columns={"EntityName": "IndicatorName", "Close": "Close_Value"}
        ).to_dict(orient="records")
        print(f" -> Pushing {len(records)} records to macro_daily_ledger...")
        q = text("""
            INSERT INTO macro_daily_ledger ("IndicatorName", "ReportDate", "Open", "High", "Low", "Close_Value", "Volume")
            VALUES (:IndicatorName, :ReportDate, :Open, :High, :Low, :Close_Value, :Volume)
            ON CONFLICT ("IndicatorName", "ReportDate") DO UPDATE SET 
                "Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close_Value"=EXCLUDED."Close_Value", "Volume"=EXCLUDED."Volume";
        """)
        with engine.begin() as conn:
            for r in records:
                conn.execute(q, r)

    # ---------------------------------------------------------
    # 2. MACRO INTRADAY LEDGER
    # ---------------------------------------------------------
    macro_intra = intraday_df[intraday_df["Category"] == "MACRO"].copy()
    if not macro_intra.empty:
        records = macro_intra.rename(
            columns={"EntityName": "IndicatorName", "Close": "Close_Value"}
        ).to_dict(orient="records")
        print(f" -> Pushing {len(records)} records to macro_intraday_ledger...")
        q = text("""
            INSERT INTO macro_intraday_ledger ("IndicatorName", "ReportDate", "Timeframe", "Open", "High", "Low", "Close_Value", "Volume")
            VALUES (:IndicatorName, :ReportDate, :Timeframe, :Open, :High, :Low, :Close_Value, :Volume)
            ON CONFLICT ("IndicatorName", "ReportDate", "Timeframe") DO UPDATE SET 
                "Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close_Value"=EXCLUDED."Close_Value", "Volume"=EXCLUDED."Volume";
        """)
        with engine.begin() as conn:
            for r in records:
                conn.execute(q, r)

    # ---------------------------------------------------------
    # 3. GLOBAL ASSETS DAILY
    # ---------------------------------------------------------
    asset_daily = daily_df[daily_df["Category"] == "ASSET"].copy()
    if not asset_daily.empty:
        records = asset_daily.rename(columns={"EntityName": "Ticker"}).to_dict(
            orient="records"
        )
        print(f" -> Pushing {len(records)} records to global_assets_daily...")
        q = text("""
            INSERT INTO global_assets_daily ("Ticker", "ReportDate", "AssetClass", "Open", "High", "Low", "Close", "Volume")
            VALUES (:Ticker, :ReportDate, :AssetClass, :Open, :High, :Low, :Close, :Volume)
            ON CONFLICT ("Ticker", "ReportDate") DO UPDATE SET 
                "Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close"=EXCLUDED."Close", "Volume"=EXCLUDED."Volume";
        """)
        with engine.begin() as conn:
            for r in records:
                conn.execute(q, r)

    # ---------------------------------------------------------
    # 4. GLOBAL ASSETS INTRADAY
    # ---------------------------------------------------------
    asset_intra = intraday_df[intraday_df["Category"] == "ASSET"].copy()
    if not asset_intra.empty:
        records = asset_intra.rename(columns={"EntityName": "Ticker"}).to_dict(
            orient="records"
        )
        print(f" -> Pushing {len(records)} records to global_assets_intraday...")
        q = text("""
            INSERT INTO global_assets_intraday ("Ticker", "ReportDate", "Timeframe", "Open", "High", "Low", "Close", "Volume")
            VALUES (:Ticker, :ReportDate, :Timeframe, :Open, :High, :Low, :Close, :Volume)
            ON CONFLICT ("Ticker", "ReportDate", "Timeframe") DO UPDATE SET 
                "Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close"=EXCLUDED."Close", "Volume"=EXCLUDED."Volume";
        """)
        with engine.begin() as conn:
            for r in records:
                conn.execute(q, r)

    print("[SUCCESS] All Macro Pipeline Data Successfully Routed and Upserted.")


def run_macro_pipeline(period_days=3000):
    print(f"\nStarting Macro Pipeline...")
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
