import yfinance as yf
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from scripts.database import engine
import time
from datetime import datetime


def register_discovered_tickers(tickers, data_source="auto"):
    if not tickers:
        return

    print(f"\\nRegistering discovered client tickers from {data_source.upper()}...")

    # Create a DataFrame for bulk native insertion
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
                "TargetTable": "global_assets",
                "AssetClass": "Equity",
                "Exchange": exchange,
                "IsActive": True,
                "Description": f"Auto-discovered via {data_source.upper()} ETL",
            }
        )

    df_meta = pd.DataFrame(records)
    engine.register("temp_meta", df_meta)

    # Native DuckDB Upsert
    engine.execute("""
        INSERT INTO market_metadata ("Ticker", "IndicatorName", "TargetTable", "AssetClass", "Exchange", "IsActive", "Description")
        SELECT "Ticker", "IndicatorName", "TargetTable", "AssetClass", "Exchange", "IsActive", "Description" FROM temp_meta
        ON CONFLICT ("Ticker") 
        DO UPDATE SET 
            "IndicatorName" = EXCLUDED."IndicatorName",
            "TargetTable" = EXCLUDED."TargetTable",
            "AssetClass" = EXCLUDED."AssetClass",
            "Exchange" = EXCLUDED."Exchange",
            "IsActive" = EXCLUDED."IsActive",
            "Description" = EXCLUDED."Description";
    """)
    engine.unregister("temp_meta")


def get_registered_targets():
    query = """
        SELECT "Ticker", "TargetTable", "Exchange" 
        FROM market_metadata 
        WHERE "IsActive" = true 
        AND "TargetTable" IN ('macro_indicators', 'global_assets')
    """
    try:
        return engine.execute(query).df()
    except Exception as e:
        print(f"Failed to fetch targets from DuckDB: {e}")
        return pd.DataFrame()


tv = TvDatafeed()


def fetch_hybrid_macro_data(intervals=["1d"], period_days=3000):
    targets_df = get_registered_targets()
    if targets_df.empty:
        print("No active macro/global targets found in metadata table.")
        return None

    interval_map = {
        "1m": Interval.in_1_minute,
        "5m": Interval.in_5_minute,
        "15m": Interval.in_15_minute,
        "30m": Interval.in_30_minute,
        "1h": Interval.in_1_hour,
        "1d": Interval.in_daily,
        "1wk": Interval.in_weekly,
        "1mo": Interval.in_monthly,
    }

    bars_per_day = {"1d": 1, "1h": 7, "30m": 13, "15m": 26, "5m": 78, "1m": 390}

    all_data = []

    for index, row in targets_df.iterrows():
        symbol = row["Ticker"]
        table_category = (
            "MACRO" if row["TargetTable"] == "macro_indicators" else "ASSET"
        )
        exchange = row["Exchange"] if pd.notna(row["Exchange"]) else "NSE"

        print(f"\\n[{table_category}] Fetching {symbol} ({exchange})")

        for inv_str in intervals:
            if inv_str not in interval_map:
                continue

            n_bars = period_days * bars_per_day.get(inv_str, 1)
            # Cap at TradingView's absolute limits based on resolution
            if inv_str in ["1m", "5m"]:
                n_bars = min(n_bars, 5000)
            elif inv_str in ["15m", "30m", "1h"]:
                n_bars = min(n_bars, 10000)
            else:
                n_bars = min(n_bars, 20000)

            print(f" -> Interval: {inv_str} | Attempting {n_bars} bars")

            df = tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=interval_map[inv_str],
                n_bars=n_bars,
            )

            if df is not None and not df.empty:
                df.reset_index(inplace=True)
                df.rename(
                    columns={
                        "datetime": "ReportDate",
                        "open": "Open",
                        "high": "High",
                        "low": "Low",
                        "close": "Close",
                        "volume": "Volume",
                    },
                    inplace=True,
                )

                df["EntityName"] = symbol
                df["Timeframe"] = inv_str
                df["Category"] = table_category

                # Extract explicit time component for intraday data
                df["ReportTime"] = df["ReportDate"].dt.time.astype(str)
                df["ReportDate"] = df["ReportDate"].dt.date.astype(str)

                # Assign appropriate Close/Value mappings based on category
                if table_category == "MACRO":
                    df["Close_Value"] = df["Close"]

                all_data.append(df)
                print(f"    [+] Success: {len(df)} records fetched.")
            else:
                print(f"    [-] Failed or Empty.")

            time.sleep(0.5)

    if all_data:
        master_df = pd.concat(all_data, ignore_index=True)
        master_df = master_df.where(pd.notnull(master_df), None)
        return master_df
    else:
        return pd.DataFrame()


def push_to_database(df):
    if df is None or df.empty:
        print("No data to push.")
        return

    # Split data based on granularity (Daily vs Intraday)
    daily_df = df[df["Timeframe"] == "1d"].copy()
    intraday_df = df[df["Timeframe"] != "1d"].copy()

    # --- 1. Macro Indicators (Daily) ---
    macro_daily = daily_df[daily_df["Category"] == "MACRO"].copy()
    if not macro_daily.empty:
        print(f" -> Pushing {len(macro_daily)} records to macro_daily_ledger...")

        macro_daily = macro_daily[["EntityName", "ReportDate", "Close_Value", "Volume"]]
        macro_daily.rename(columns={"EntityName": "IndicatorName"}, inplace=True)

        engine.register("temp_macro_daily", macro_daily)
        engine.execute("""
            INSERT INTO macro_daily_ledger ("IndicatorName", "ReportDate", "Close_Value", "Volume")
            SELECT "IndicatorName", CAST("ReportDate" AS DATE), "Close_Value", "Volume" FROM temp_macro_daily
            ON CONFLICT ("IndicatorName", "ReportDate") DO UPDATE SET 
                "Close_Value"=EXCLUDED."Close_Value", "Volume"=EXCLUDED."Volume";
        """)
        engine.unregister("temp_macro_daily")

    # --- 2. Macro Indicators (Intraday) ---
    macro_intra = intraday_df[intraday_df["Category"] == "MACRO"].copy()
    if not macro_intra.empty:
        print(f" -> Pushing {len(macro_intra)} records to macro_intraday_ledger...")

        macro_intra = macro_intra[
            [
                "EntityName",
                "ReportDate",
                "ReportTime",
                "Timeframe",
                "Close_Value",
                "Volume",
            ]
        ]
        macro_intra.rename(columns={"EntityName": "IndicatorName"}, inplace=True)

        engine.register("temp_macro_intra", macro_intra)
        engine.execute("""
            INSERT INTO macro_intraday_ledger ("IndicatorName", "ReportDate", "ReportTime", "Timeframe", "Close_Value", "Volume")
            SELECT "IndicatorName", CAST("ReportDate" AS DATE), CAST("ReportTime" AS TIME), "Timeframe", "Close_Value", "Volume" FROM temp_macro_intra
            ON CONFLICT ("IndicatorName", "ReportDate", "ReportTime", "Timeframe") DO UPDATE SET 
                "Close_Value"=EXCLUDED."Close_Value", "Volume"=EXCLUDED."Volume";
        """)
        engine.unregister("temp_macro_intra")

    # --- 3. Global Assets (Daily) ---
    asset_daily = daily_df[daily_df["Category"] == "ASSET"].copy()
    if not asset_daily.empty:
        print(f" -> Pushing {len(asset_daily)} records to global_assets_daily...")

        asset_daily = asset_daily[
            ["EntityName", "ReportDate", "Open", "High", "Low", "Close", "Volume"]
        ]
        asset_daily.rename(columns={"EntityName": "Ticker"}, inplace=True)
        asset_daily["AssetClass"] = "Equity"

        engine.register("temp_asset_daily", asset_daily)
        engine.execute("""
            INSERT INTO global_assets_daily ("Ticker", "ReportDate", "AssetClass", "Open", "High", "Low", "Close", "Volume")
            SELECT "Ticker", CAST("ReportDate" AS DATE), "AssetClass", "Open", "High", "Low", "Close", "Volume" FROM temp_asset_daily
            ON CONFLICT ("Ticker", "ReportDate") DO UPDATE SET 
                "Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close"=EXCLUDED."Close", "Volume"=EXCLUDED."Volume";
        """)
        engine.unregister("temp_asset_daily")

    # --- 4. Global Assets (Intraday) ---
    asset_intra = intraday_df[intraday_df["Category"] == "ASSET"].copy()
    if not asset_intra.empty:
        print(f" -> Pushing {len(asset_intra)} records to global_assets_intraday...")

        asset_intra = asset_intra[
            [
                "EntityName",
                "ReportDate",
                "Timeframe",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
            ]
        ]
        asset_intra.rename(columns={"EntityName": "Ticker"}, inplace=True)

        # We need to construct a proper datetime for the intraday table since your schema expects ReportDate to be a full timestamp for intraday
        # Note: The dataframe currently has separate ReportDate and ReportTime columns created earlier. We'll reconstruct the full timestamp for insertion.
        # Actually looking at your old schema, the target table only has "ReportDate" which acts as the full timestamp. Let's rebuild it.
        # We'll fetch the raw index 'datetime' from the TVDatafeed output directly.
        pass

    print("[SUCCESS] All Macro Pipeline Data Successfully Routed and Upserted.")


def push_to_database_fixed(df):
    """
    Revised push_to_database to handle timeframes accurately according to the DuckDB schema.
    """
    if df is None or df.empty:
        print("No data to push.")
        return

    daily_df = df[df["Timeframe"] == "1d"].copy()
    intraday_df = df[df["Timeframe"] != "1d"].copy()

    # --- 1. Macro Indicators (Daily) ---
    macro_daily = daily_df[daily_df["Category"] == "MACRO"].copy()
    if not macro_daily.empty:
        print(f" -> Pushing {len(macro_daily)} records to macro_daily_ledger...")
        macro_daily.rename(columns={"EntityName": "IndicatorName"}, inplace=True)
        engine.register("temp_macro_daily", macro_daily)
        engine.execute("""
            INSERT INTO macro_daily_ledger ("IndicatorName", "ReportDate", "Close_Value", "Volume")
            SELECT "IndicatorName", CAST("ReportDate" AS DATE), "Close_Value", "Volume" FROM temp_macro_daily
            ON CONFLICT ("IndicatorName", "ReportDate") DO UPDATE SET 
                "Close_Value"=EXCLUDED."Close_Value", "Volume"=EXCLUDED."Volume";
        """)
        engine.unregister("temp_macro_daily")

    # --- 2. Macro Indicators (Intraday) ---
    macro_intra = intraday_df[intraday_df["Category"] == "MACRO"].copy()
    if not macro_intra.empty:
        print(f" -> Pushing {len(macro_intra)} records to macro_intraday_ledger...")
        macro_intra.rename(columns={"EntityName": "IndicatorName"}, inplace=True)
        engine.register("temp_macro_intra", macro_intra)
        engine.execute("""
            INSERT INTO macro_intraday_ledger ("IndicatorName", "ReportDate", "ReportTime", "Timeframe", "Close_Value", "Volume")
            SELECT "IndicatorName", CAST("ReportDate" AS DATE), CAST("ReportTime" AS TIME), "Timeframe", "Close_Value", "Volume" FROM temp_macro_intra
            ON CONFLICT ("IndicatorName", "ReportDate", "ReportTime", "Timeframe") DO UPDATE SET 
                "Close_Value"=EXCLUDED."Close_Value", "Volume"=EXCLUDED."Volume";
        """)
        engine.unregister("temp_macro_intra")

    # --- 3. Global Assets (Daily) ---
    asset_daily = daily_df[daily_df["Category"] == "ASSET"].copy()
    if not asset_daily.empty:
        print(f" -> Pushing {len(asset_daily)} records to global_assets_daily...")
        asset_daily.rename(columns={"EntityName": "Ticker"}, inplace=True)
        asset_daily["AssetClass"] = "Equity"
        engine.register("temp_asset_daily", asset_daily)
        engine.execute("""
            INSERT INTO global_assets_daily ("Ticker", "ReportDate", "AssetClass", "Open", "High", "Low", "Close", "Volume")
            SELECT "Ticker", CAST("ReportDate" AS DATE), "AssetClass", "Open", "High", "Low", "Close", "Volume" FROM temp_asset_daily
            ON CONFLICT ("Ticker", "ReportDate") DO UPDATE SET 
                "Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close"=EXCLUDED."Close", "Volume"=EXCLUDED."Volume";
        """)
        engine.unregister("temp_asset_daily")

    # --- 4. Global Assets (Intraday) ---
    asset_intra = intraday_df[intraday_df["Category"] == "ASSET"].copy()
    if not asset_intra.empty:
        print(f" -> Pushing {len(asset_intra)} records to global_assets_intraday...")
        asset_intra.rename(columns={"EntityName": "Ticker"}, inplace=True)

        # We need to recreate the full timestamp string for 'ReportDate' in the intraday table since it acts as the primary key
        # We combine the separated 'ReportDate' and 'ReportTime' back into a single ISO string
        asset_intra["FullReportDate"] = (
            asset_intra["ReportDate"] + " " + asset_intra["ReportTime"]
        )

        engine.register("temp_asset_intra", asset_intra)
        engine.execute("""
            INSERT INTO global_assets_intraday ("Ticker", "ReportDate", "Timeframe", "Open", "High", "Low", "Close", "Volume")
            SELECT "Ticker", CAST("FullReportDate" AS TIMESTAMP), "Timeframe", "Open", "High", "Low", "Close", "Volume" FROM temp_asset_intra
            ON CONFLICT ("Ticker", "ReportDate", "Timeframe") DO UPDATE SET 
                "Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close"=EXCLUDED."Close", "Volume"=EXCLUDED."Volume";
        """)
        engine.unregister("temp_asset_intra")

    print("[SUCCESS] All Macro Pipeline Data Successfully Routed and Upserted.")


def run_macro_pipeline(period_days=3000):
    print(f"\\nStarting Macro Pipeline...")
    target_intervals = ["1d", "1h", "30m", "5m", "1m"]
    final_df = fetch_hybrid_macro_data(
        intervals=target_intervals, period_days=period_days
    )
    push_to_database_fixed(final_df)


if __name__ == "__main__":
    run_macro_pipeline(period_days=10)
