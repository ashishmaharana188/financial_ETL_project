import os
import time
import pandas as pd
from curl_cffi import requests
from datetime import datetime

CACHE_DIR = "offline_data_cache/master_archives"
os.makedirs(CACHE_DIR, exist_ok=True)


def extract_value(val):
    """
    NiftyTrader's JSON randomly wraps floats in dictionaries.
    This helper function extracts the raw number cleanly.
    """
    if isinstance(val, dict):
        return val.get("parsedValue", 0.0)
    return val


def run_fiidii_backfill(years_to_fetch=None):
    print("=== Starting NiftyTrader FII/DII API Backfill ===\n")

    session = requests.Session(impersonate="chrome120")
    headers = {
        "Accept": "application/json",
        "Referer": "https://www.niftytrader.in/",
    }

    if years_to_fetch is None:
        current_year = datetime.now().year
        years_to_fetch = [current_year - i for i in range(5)]

    all_data = []

    for year in sorted(years_to_fetch, reverse=True):
        print(f"    -> Fetching Daily FII/DII flow for {year}...")
        url = f"https://webapi.niftytrader.in/webapi/Resource/fii-dii-activity-data?request_type=yearly&year_month={year}"

        try:
            response = session.get(url, headers=headers, timeout=15)

            if response.status_code == 200:
                json_payload = response.json()

                # Dig into the JSON structure you discovered
                daily_records = json_payload.get("resultData", {}).get(
                    "fii_dii_data", []
                )

                if not daily_records:
                    print(f"    [-] No data returned for {year}.")
                    continue

                # Clean the quirky dictionary values
                clean_records = []
                for row in daily_records:
                    clean_row = {
                        "date": row.get("created_at", "").split("T")[
                            0
                        ],  # Strip timestamp
                        "fii_buy": extract_value(row.get("fii_buy_value")),
                        "fii_sell": extract_value(row.get("fii_sell_value")),
                        "fii_net": extract_value(row.get("fii_net_value")),
                        "dii_buy": extract_value(row.get("dii_buy_value")),
                        "dii_sell": extract_value(row.get("dii_sell_value")),
                        "dii_net": extract_value(row.get("dii_net_value")),
                        "nifty_close": extract_value(row.get("last_trade_price")),
                    }
                    clean_records.append(clean_row)

                all_data.extend(clean_records)
                print(
                    f"    [+] Successfully extracted {len(clean_records)} daily rows for {year}."
                )

            else:
                print(
                    f"    [-] Failed to fetch {year}. Status Code: {response.status_code}"
                )

        except Exception as e:
            print(f"    [!] Error parsing {year}: {e}")

        time.sleep(2)  # Be polite to their API

    if all_data:
        # Save the consolidated 3.5 year master file
        df = pd.DataFrame(all_data)
        # Sort chronologically (oldest to newest)
        df.sort_values(by="date", inplace=True)

        file_path = os.path.join(CACHE_DIR, "niftytrader_fiidii_master.csv")
        df.to_csv(file_path, index=False)
        print(f"\n[SUCCESS] Saved {len(df)} total daily records to {file_path}")
    else:
        print("\n[!] No data was successfully extracted.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    # If the Orchestrator passes targeted dates
    if args.start and args.end:
        start_dt = pd.to_datetime(args.start)
        end_dt = pd.to_datetime(args.end)

        # The NiftyTrader API only takes years, so we extract the unique years
        # that fall between the start and end dates.
        start_year = start_dt.year
        end_year = end_dt.year
        target_years = list(range(start_year, end_year + 1))

        print(f"=== Running Delta Sync for FII/DII: {args.start} to {args.end} ===")
        run_fiidii_backfill(target_years)

    # If run manually with no arguments, do the full bulk backfill
    else:
        run_fiidii_backfill()
