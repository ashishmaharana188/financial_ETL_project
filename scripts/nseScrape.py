import os
import time
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

# The magic library that spoofs Chrome's TLS fingerprint to bypass Cloudflare
from curl_cffi import requests

CACHE_DIR = "offline_data_cache/master_archives"
os.makedirs(CACHE_DIR, exist_ok=True)


class NSEFetcher:
    def __init__(self):
        # impersonate="chrome120" gives us a perfect human TLS/SSL footprint
        self.session = requests.Session(impersonate="chrome120")

        # We only need minimal headers; curl_cffi handles the rest automatically
        self.headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/reports-indices/historical-data",
        }
        self._initialize_session()

    def _initialize_session(self):
        """Grabs the initial dynamic Cloudflare cookies."""
        print("[*] Handshaking with NSE (Chrome 120 Footprint)...")
        try:
            self.session.get(
                "https://www.nseindia.com", headers=self.headers, timeout=15
            )
            time.sleep(2)
        except Exception as e:
            print(f"[!] Failed initial handshake: {e}")

    def fetch_historical_deals(self, option_type, start_date_str, end_date_str):
        """Hits the hidden API for a specific 365-day chunk."""
        url = f"https://www.nseindia.com/api/historicalOR/bulk-block-short-deals?optionType={option_type}&from={start_date_str}&to={end_date_str}&csv=true"

        print(f"    -> Fetching {option_type} ({start_date_str} to {end_date_str})...")
        try:
            response = self.session.get(url, headers=self.headers, timeout=20)

            if response.status_code == 200:
                # Read CSV straight into pandas
                csv_data = StringIO(response.text)
                df = pd.read_csv(csv_data)

                # Check if empty
                if df.empty or len(df) <= 1:
                    print(f"    [-] No data returned for this period.")
                    return

                file_path = os.path.join(
                    CACHE_DIR,
                    f"nse_{option_type}_{start_date_str}_to_{end_date_str}.csv",
                )
                df.to_csv(file_path, index=False)
                print(f"    [+] Saved {len(df)} rows to {file_path}")
            else:
                print(f"    [-] Blocked or Failed. Status Code: {response.status_code}")

        except Exception as e:
            print(f"    [!] Error: {e}")


def run_5_year_backfill():
    print("=== Starting 5-Year NSE Smart Money Backfill ===\n")
    fetcher = NSEFetcher()

    deal_types = ["bulk_deals", "block_deals", "short_selling"]

    # Start the loop from today
    current_end_date = datetime.now()

    # Loop 5 times (5 years)
    for year in range(10):
        print(f"\n--- Processing Year {year + 1} Backwards ---")

        # NSE strictly allows max 365 days per request. We use 364 to be perfectly safe.
        current_start_date = current_end_date - timedelta(days=364)

        # Format to DD-MM-YYYY as required by the NSE API
        str_end = current_end_date.strftime("%d-%m-%Y")
        str_start = current_start_date.strftime("%d-%m-%Y")

        for d_type in deal_types:
            fetcher.fetch_historical_deals(d_type, str_start, str_end)
            # Sleep 4 seconds between files to respect rate limits
            time.sleep(4)

        # Shift the end date backwards for the next loop iteration
        current_end_date = current_start_date - timedelta(days=1)

    print("\n=== 5-Year Backfill Complete ===")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    if args.start and args.end:
        start_dt = pd.to_datetime(args.start)
        end_dt = pd.to_datetime(args.end)

        print(f"=== Running Delta Sync: {args.start} to {args.end} ===")
        fetcher = NSEFetcher()
        deal_types = ["bulk_deals", "block_deals", "short_selling"]

        str_end = end_dt.strftime("%d-%m-%Y")
        str_start = start_dt.strftime("%d-%m-%Y")

        for d_type in deal_types:
            fetcher.fetch_historical_deals(d_type, str_start, str_end)
            time.sleep(4)
    else:
        run_5_year_backfill()  # Falls back to bulk if no dates provided
