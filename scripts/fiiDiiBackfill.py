import os
import time
import pandas as pd
from curl_cffi import requests

CACHE_DIR = "offline_data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)


def extract_value(val):
    """
    NiftyTrader's JSON randomly wraps floats in dictionaries.
    This helper function extracts the raw number cleanly.
    """
    if isinstance(val, dict):
        return val.get("parsedValue", 0.0)
    return val


def run_fiidii_backfill():
    print("=== Starting NiftyTrader FII/DII API Backfill ===\n")

    session = requests.Session(impersonate="chrome120")
    headers = {
        "Accept": "application/json",
        "Referer": "https://www.niftytrader.in/",
    }

    # Nov 2022 to current year gives us our solid 3.5 year mathematical baseline
    years_to_fetch = [2026, 2025, 2024, 2023, 2022]
    all_data = []

    for year in years_to_fetch:
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
    run_fiidii_backfill()
