import pandas as pd
from sqlalchemy import text
from scripts.database import engine
from scripts.edgar_utils import get_structural_break_date
import time


def backfill_structural_breaks():
    print("Starting Corporate Action Audit...")

    with engine.begin() as conn:
        # 1. Fetch all tickers from your profiles table
        result = conn.execute(text('SELECT "Ticker" FROM company_profiles;'))
        tickers = [row[0] for row in result]

        for ticker in tickers:
            print(f"[{ticker}] Checking EDGAR for structural breaks...")

            # 2. Ping EDGAR
            break_date = get_structural_break_date(ticker)

            if break_date:
                print(
                    f"  -> WARNING: Break detected on {break_date}. Updating database."
                )
                # 3. Save the date to the database
                update_query = text("""
                    UPDATE company_profiles 
                    SET valid_data_since = :break_date 
                    WHERE "Ticker" = :ticker
                """)
                conn.execute(update_query, {"break_date": break_date, "ticker": ticker})
            else:
                print(f"  -> Clean history. No action required.")

            # Respect SEC rate limits (10 requests per second max, safe to pause for 1s)
            time.sleep(1)

    print("Audit Complete.")


if __name__ == "__main__":
    backfill_structural_breaks()
