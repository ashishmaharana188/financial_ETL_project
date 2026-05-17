import os
import json
import time
import pandas as pd
import yfinance as yf
from sqlalchemy import text
from scripts.database import engine


def get_active_tickers():
    """Extracts active tickers dynamically from mapping configuration."""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    config_path = os.path.join(base_dir, "mapping_config.json")

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return [
            f"{ticker}.NS" if not ticker.endswith(".NS") else ticker
            for ticker in config.keys()
        ]
    except FileNotFoundError:
        print("[!] mapping_config.json not found. Falling back to test tickers.")
        return ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"]


def wipe_casino_tables():
    """Clears old data from tables to ensure a clean slate for the historical pull."""
    tables = ["market_1d", "market_30m", "market_5m", "market_1m"]
    with engine.begin() as conn:
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
    print("[*] Cleaned slate. Previous market data truncated.")


def fetch_and_store(ticker, interval, period, table_name):
    """Fetches data from yfinance and appends it to PostgreSQL."""
    print(f"    -> Fetching {interval} data ({period})...")
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False)

        if df.empty:
            print(f"    [-] No data found for {ticker} at {interval}.")
            return

        # Handle yfinance multi-index column format (v0.2.40+)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.reset_index(inplace=True)

        # Standardize columns to match our database.py metadata
        df.rename(columns={"Datetime": "date", "Date": "date"}, inplace=True)
        df.columns = [c.lower() for c in df.columns]

        # Ensure date format is PostgreSQL DateTime friendly
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["ticker"] = ticker.replace(".NS", "")

        # Keep only the columns defined in metadata
        df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]

        # Push to PostgreSQL using the centralized engine
        df.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"    [+] Stored {len(df)} rows in {table_name}")

    except Exception as e:
        print(f"    [!] Error processing {ticker} at {interval}: {e}")


def open_the_spigot():
    wipe_casino_tables()
    tickers = get_active_tickers()
    print(f"[*] Opening the Data Spigot for {len(tickers)} tickers...\n")

    for ticker in tickers:
        print(f"--- Processing {ticker} ---")

        # Phase 3: Engine 3 Reality (OLS Convergence)
        fetch_and_store(ticker, interval="1d", period="max", table_name="market_1d")

        # Phase 4: Engine 5 Microstructure (The Sniper)
        fetch_and_store(ticker, interval="30m", period="60d", table_name="market_30m")
        fetch_and_store(ticker, interval="5m", period="60d", table_name="market_5m")
        fetch_and_store(ticker, interval="1m", period="7d", table_name="market_1m")

        # Brief pause to respect yfinance API rate limits
        time.sleep(1)

    print(
        "\n[SUCCESS] Casino data successfully loaded into PostgreSQL via centralized engine."
    )


if __name__ == "__main__":
    open_the_spigot()
