import os
import time
import json
from datetime import datetime, timedelta
from curl_cffi import requests

CACHE_DIR = "offline_data_cache/master_archives"
os.makedirs(CACHE_DIR, exist_ok=True)


class MasterArchiveScraper:
    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")

        # Standard headers for NSE GET requests
        self.nse_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.nseindia.com/",
        }

    def fetch_nse_file(self, url, save_path, file_desc):
        """Standard GET request for NSE Archive static files."""
        if os.path.exists(save_path):
            return  # Skip if already downloaded

        try:
            res = self.session.get(url, headers=self.nse_headers, timeout=15)
            if res.status_code == 200:
                with open(save_path, "wb") as f:
                    f.write(res.content)
                print(f"      [+] Saved {file_desc}")
            elif res.status_code == 404:
                # 404 means weekend or holiday. Silently pass.
                pass
            else:
                print(f"      [-] Failed {file_desc} (Status: {res.status_code})")
        except Exception as e:
            print(f"      [!] Error on {file_desc}: {e}")

    def fetch_fo_bhavcopy(self, current_date, save_path):
        """Dedicated F&O method handling the 2024 UDiFF URL migration and Cache corruption."""
        # Fix the silent bug: Only skip if file exists AND is a valid size (> 1KB)
        if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
            return

        yyyy = current_date.strftime("%Y")
        MMM = current_date.strftime("%b").upper()
        ddMMMyyyy = current_date.strftime("%d%b%Y").upper()
        yyyymmdd = current_date.strftime("%Y%m%d")

        # URL 1: Legacy Format (Pre-July 2024)
        legacy_url = f"https://archives.nseindia.com/content/historical/DERIVATIVES/{yyyy}/{MMM}/fo{ddMMMyyyy}bhav.csv.zip"

        # URL 2: UDiFF Format (Post-July 2024)
        udiff_url = f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"

        # Prioritize UDiFF for modern dates, otherwise default to legacy
        urls_to_try = (
            [udiff_url, legacy_url] if current_date.year >= 2024 else [legacy_url]
        )

        for url in urls_to_try:
            try:
                res = self.session.get(url, headers=self.nse_headers, timeout=15)
                if res.status_code == 200:
                    with open(save_path, "wb") as f:
                        f.write(res.content)
                    print(f"      [+] Saved NSE F&O Bhavcopy (ZIP)")
                    return  # Exit cleanly on success
            except Exception:
                continue  # Try the next URL fallback if one fails

        # If it reaches here, both URLs failed (likely a market holiday)
        pass

    def fetch_mcx_json(self, date_YYYYMMDD, save_path, file_desc):
        """POST request to the hidden MCX backend API."""
        if os.path.exists(save_path):
            return

        url = "https://www.mcxindia.com/backpage.aspx/GetDateWiseBhavCopy"
        payload = {"Date": date_YYYYMMDD, "InstrumentName": "ALL"}

        mcx_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "Referer": "https://www.mcxindia.com/market-data/bhavcopy",
        }

        try:
            res = self.session.post(url, json=payload, headers=mcx_headers, timeout=15)
            if res.status_code == 200:
                data = res.json()

                # Check if data payload exists (ASP.NET usually returns {"d": "..."})
                # If the day was a holiday, the data array will be empty
                with open(save_path, "w") as f:
                    json.dump(data, f)
                print(f"      [+] Saved {file_desc}")

            elif res.status_code == 404:
                pass
            else:
                print(f"      [-] Failed {file_desc} (Status: {res.status_code})")
        except Exception as e:
            print(f"      [!] Error on {file_desc}: {e}")


def run_master_archive_backfill():
    print("=== Starting Master 10-Year Deep Archive Backfill (NSE + MCX) ===\n")
    scraper = MasterArchiveScraper()

    # 10-Year Window
    start_date = datetime(2016, 1, 1)
    end_date = datetime.now()

    current_date = end_date

    while current_date >= start_date:
        # Ignore obvious weekends to save network calls
        if current_date.weekday() in [5, 6]:
            current_date -= timedelta(days=1)
            continue

        # --- Date Formatters ---
        # NSE Date Formats
        ddmmyyyy = current_date.strftime("%d%m%Y")  # e.g., 31012023
        yyyy = current_date.strftime("%Y")  # e.g., 2023
        MMM = current_date.strftime("%b").upper()  # e.g., JAN
        ddMMMyyyy = current_date.strftime("%d%b%Y").upper()  # e.g., 31JAN2023

        # MCX Date Format
        mcx_yyyymmdd = current_date.strftime("%Y%m%d")  # e.g., 20230131

        print(f"\n  -> Checking Archives for {current_date.strftime('%d-%b-%Y')}...")

        # --- Construct URLs & Paths ---

        cash_url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
        cash_path = os.path.join(CACHE_DIR, f"nse_cash_{ddmmyyyy}.csv")

        # 2. NSE FII Participant OI
        oi_url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{ddmmyyyy}.csv"
        oi_path = os.path.join(CACHE_DIR, f"nse_part_oi_{ddmmyyyy}.csv")

        # 3. NSE F&O Bhavcopy (Path only, URL logic is now handled in the method)
        fo_path = os.path.join(CACHE_DIR, f"nse_fo_bhav_{ddmmyyyy}.zip")

        # 4. MCX Bhavcopy
        mcx_path = os.path.join(CACHE_DIR, f"mcx_bhav_{mcx_yyyymmdd}.json")

        # --- Execute Downloads ---
        scraper.fetch_nse_file(cash_url, cash_path, "NSE Cash Bhavcopy")
        scraper.fetch_nse_file(oi_url, oi_path, "NSE Participant OI")

        # Use our new dedicated method for F&O
        scraper.fetch_fo_bhavcopy(current_date, fo_path)

        scraper.fetch_mcx_json(mcx_yyyymmdd, mcx_path, "MCX All Commodities (JSON)")

        # Step back one day
        current_date -= timedelta(days=1)

        # Sleep to respect rate limits on both exchanges
        time.sleep(2)


if __name__ == "__main__":
    import argparse
    import pandas as pd
    import time
    from datetime import timedelta

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()

    # --- DELTA SYNC EXECUTION BLOCK ---
    if args.start and args.end:
        start_dt = pd.to_datetime(args.start)
        end_dt = pd.to_datetime(args.end)

        print(f"\n=== Running Delta Sync: {args.start} to {args.end} ===")
        scraper = MasterArchiveScraper()
        current_date = start_dt

        while current_date <= end_dt:
            # 1. Skip weekends to avoid unnecessary 404s
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            print(f"\n  -> Fetching {current_date.strftime('%Y-%m-%d')}")

            # 2. Establish uniform date variables
            ddmmyyyy = current_date.strftime("%d%m%Y")
            mcx_yyyymmdd = current_date.strftime("%Y%m%d")

            # 3. Fetch Cash Market (Direct URL)
            cash_url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
            cash_path = os.path.join(CACHE_DIR, f"nse_cash_{ddmmyyyy}.csv")
            scraper.fetch_nse_file(cash_url, cash_path, "NSE Cash Bhavcopy")

            # 4. Fetch Participant OI (Direct URL)
            oi_url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{ddmmyyyy}.csv"
            oi_path = os.path.join(CACHE_DIR, f"nse_part_oi_{ddmmyyyy}.csv")
            scraper.fetch_nse_file(oi_url, oi_path, "Participant OI")

            # 5. Fetch F&O Market (Delegated to UDiFF-aware method)
            fo_path = os.path.join(CACHE_DIR, f"nse_fo_bhav_{ddmmyyyy}.zip")
            scraper.fetch_fo_bhavcopy(current_date, fo_path)

            # 6. Fetch MCX Market (Delegated to JSON handler)
            mcx_path = os.path.join(CACHE_DIR, f"mcx_bhav_{mcx_yyyymmdd}.json")
            scraper.fetch_mcx_json(mcx_yyyymmdd, mcx_path, "MCX All Commodities")

            # 7. Advance date and respect Exchange rate limits
            current_date += timedelta(days=1)
            time.sleep(2)

    # --- FULL BACKFILL EXECUTION BLOCK ---
    else:
        run_master_archive_backfill()
