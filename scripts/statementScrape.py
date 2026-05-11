import json
import os
import re
import time
import urllib.parse
from pathlib import Path
import numpy as np
import pandas as pd
import requests
import urllib3
import yfinance as yf
from bs4 import BeautifulSoup
from sqlalchemy.dialects.postgresql import insert
from scripts.ai_agent import trigger_semantic_router
from scripts.reconciliation import extract_mapped_keys, execute_three_way_match
from scripts.ai_agent import trigger_semantic_router
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from scripts.database import engine, raw_financials, company_profiles
from scripts.reconciliation import extract_mapped_keys, execute_three_way_match
from sqlalchemy import text
from scripts.model_runtime import runtime
from scripts.model_runtime import runtime
from scripts.vectorize import get_top_buckets
from scripts.reasoning import analyze_key_with_phi3
import streamlit as st
from datetime import datetime

runtime.load_models()

# vantage api key
# vantage api key
API_KEY = "V6FLFA1K7ECKP0RK"
# fmp api key
FMP_API_KEY = "039c30159a83647be8f02d571df7f52a"
# disable certificate warnings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

pd.set_option("display.float_format", lambda x: "%.2f" % x)


CACHE_DIR = "offline_statements"
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
    print(f"Created folder: {CACHE_DIR}")


##DEFINE API CALL FUNTIONS

# Dictionary CONFIGS

BASE_DIR = Path(__file__).resolve().parent.parent
config_file_path = BASE_DIR / "mapping_config.json"


if os.path.exists(config_file_path):
    with open(config_file_path, "r") as f:
        config = json.load(f)

    ittelson_income_statement_columns = config["ittelson_income_statement_columns"]

    print("SUCCESS: JSON Configuration Loaded Perfectly!")
    print(f"Loaded {len(ittelson_income_statement_columns)} IS columns.")

    normalized_is_synonym_map = config["normalized_is_synonym_map"]

    ittelson_screener_balance_sheet_map = config["ittelson_screener_balance_sheet_map"]
    yfinance_to_ittelson_map = config["yfinance_to_ittelson_map"]
    normalized_bs_synonym_map = config["normalized_bs_synonym_map"]
    ittelson_balance_sheet_columns = config["ittelson_balance_sheet_columns"]

    ittelson_cash_flow_columns = config["ittelson_cash_flow_columns"]
    normalized_cf_synonym_map = config["normalized_cf_synonym_map"]
    normalized_indirect_cf_synonym_map = config["normalized_indirect_cf_synonym_map"]
    ittelson_indirect_cf_columns = config["ittelson_indirect_cf_columns"]

else:
    raise FileNotFoundError(
        f"CRITICAL ERROR: {config_file_path} is missing. The ETL cannot run without its mapping rules."
    )


# Define the Custom Upsert Logic
def postgres_upsert(table, conn, keys, data_iter):

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_stmt = insert(table.table).values(data)

    update_dict = {
        c.name: getattr(insert_stmt.excluded, c.name)
        for c in table.table.columns
        if c.name not in ("Ticker", "ReportDate")
    }

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["Ticker", "ReportDate"], set_=update_dict
    )

    conn.execute(upsert_stmt)


## Yfinance
def get_yfinance(ticker, statement_type, freq, cache_dir=CACHE_DIR):
    if freq not in ("quarterly", "yearly"):
        raise ValueError("freq must be 'quarterly' or 'yearly'")

    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)

    filename = f"yfinance_{ticker}_{statement_type}_{freq}.json"
    file_path = os.path.join(cache_dir, filename)

    if os.path.exists(file_path):
        print(f"Loading yfinance {file_path} from local cache")
        with open(file_path, "r") as f:
            return pd.read_json(file_path)

    print(f"Fetching {ticker} {statement_type} from Yfinance")

    # NEW: Create a local ticker instance so it changes with every loop iteration
    yf_ticker = yf.Ticker(ticker)

    # call yfinance using the local yf_ticker instead of global tickerName
    if statement_type == "INCOME_STATEMENT":
        df = yf_ticker.get_income_stmt(as_dict=False, pretty=False, freq=freq)
    elif statement_type == "BALANCE_SHEET":
        df = yf_ticker.get_balance_sheet(as_dict=False, pretty=False, freq=freq)
    else:
        # Simplifies the CF logic as freq is already passed correctly
        df = yf_ticker.get_cash_flow(as_dict=False, pretty=False, freq=freq)

    if df is None or df.empty:
        print(f"No {freq} {statement_type} available from yfinance")
        return None

    # Save to cache
    df.to_json(file_path)
    print(f"Saved yfinance {ticker} {statement_type} {freq} to cache")

    return df


## Alpha Vantage
def get_alpha_vantage(ticker, statement_type, api_key, cache_dir=CACHE_DIR):
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    filename = f"vantage_{ticker}_{statement_type}.json"
    file_path = os.path.join(cache_dir, filename)

    if os.path.exists(file_path):
        print(f"Loading vantage {ticker} {statement_type} from local cache")
        with open(file_path, "r") as f:
            return json.load(f)

    print(f"Fetching {ticker} {statement_type} from Alpha Vantage")
    url = (
        f"https://www.alphavantage.co/query"
        f"?function={statement_type}"
        f"&symbol={ticker}"
        f"&apikey={api_key}"
    )

    try:
        response = requests.get(url, verify=False, timeout=20)
        data = response.json()

        # CHANGED: Check for either annual or quarterly reports to be more robust
        if "annualReports" in data or "quarterlyReports" in data:
            with open(file_path, "w") as f:
                json.dump(data, f)
            print(f"Successfully saved {ticker} to local cache.")
            return data
        else:
            error_msg = data.get("Note", data.get("Error Message", "Unknown Error"))
            print(f"Alpha Vantage Error/Limit for {ticker}: {error_msg}")
            return None

    except Exception as e:
        print(f"Request failed for {ticker}: {e}")
        return None


##FMP
## Financial Modeling Prep (FMP)
def get_fmp_financials(ticker, statement_type, freq, api_key, cache_dir=CACHE_DIR):
    if freq not in ("quarter", "annual"):
        raise ValueError("freq must be 'quarter' or 'annual'")

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    filename = f"fmp_{ticker}_{statement_type}_{freq}.json"
    file_path = os.path.join(cache_dir, filename)

    if os.path.exists(file_path):
        print(f"Loading FMP {ticker} {statement_type} ({freq}) from local cache")
        with open(file_path, "r") as f:
            return json.load(f)

    print(f"Fetching {ticker} {statement_type} ({freq}) from FMP...")

    fmp_endpoints = {
        "INCOME_STATEMENT": "income-statement",
        "BALANCE_SHEET": "balance-sheet-statement",
        "CASH_FLOW": "cash-flow-statement",
    }
    endpoint = fmp_endpoints[statement_type]

    url = f"https://financialmodelingprep.com/api/v3/{endpoint}/{ticker}?limit=20&apikey={api_key}"
    if freq == "quarter":
        url += "&period=quarter"

    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                with open(file_path, "w") as f:
                    json.dump(data, f)
                return data
            else:
                print(f"FMP returned empty data for {ticker}. Check ticker format.")
                return None
        elif response.status_code == 429:
            print(f"CRITICAL: FMP Daily Rate Limit Exceeded (429).")
            return "LIMIT_REACHED"
        else:
            print(f"FMP Error {response.status_code} for {ticker}")
            return None
    except Exception as e:
        print(f"FMP Request failed for {ticker}: {e}")
        return None


## SCREENER SCRAPPER
def get_screener_financials(ticker, report_type="yearly"):
    filename = f"screener_{ticker}_{report_type}.json"
    file_path = os.path.join(CACHE_DIR, filename)

    # Check Cache
    if os.path.exists(file_path):
        print(f"Loading {ticker} {report_type} from Screener cache...")
        with open(file_path, "r") as f:
            return json.load(f)

    # Use a Session to retain cookies across requests
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "X-Requested-With": "XMLHttpRequest",  # Tells Screener this is an AJAX API call
        }
    )

    url = f"https://www.screener.in/company/{ticker}/consolidated/"
    response = session.get(url)

    if response.status_code != 200:
        print(f"Error fetching Screener page: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract the hidden Company ID for sub item api
    company_div = soup.find(attrs={"data-company-id": True})
    if not company_div:
        print(f"Could not find company ID for {ticker}")
        return None
    company_id = company_div["data-company-id"]

    # Identify the section ID
    section_id = []
    if report_type == "quarterly":
        section_id = "quarters"
    elif report_type == "yearly":
        section_id = "profit-loss"
    elif report_type == "balance-sheet":
        section_id = "balance-sheet"
    else:
        section_id = "cash-flow"

    statement_section = soup.find("section", {"id": section_id})

    if not statement_section:
        print(f"Could not find {report_type} section for {ticker}")
        return None

    table = statement_section.find("table", class_="data-table")

    if table:
        # EXACT ORIGINAL PARSING: Date columns (Headers)
        headers = [
            th.get_text(strip=True) for th in table.find("thead").find_all("th")
        ][1:]
        financial_data = {date: {} for date in headers}

        # Parse Rows (Main Line Items)
        for tr in table.find("tbody").find_all("tr"):
            cells = tr.find_all("td")
            if cells:
                row_label_cell = cells[0]

                # EXACT ORIGINAL PARSING
                row_label = row_label_cell.get_text(strip=True).replace("+", "").strip()
                row_values = [
                    td.get_text(strip=True).replace(",", "") for td in cells[1:]
                ]

                for i, date in enumerate(headers):
                    if i < len(row_values):
                        financial_data[date][row_label] = row_values[i]

                button = row_label_cell.find("button")
                if button:
                    safe_parent = urllib.parse.quote(row_label)
                    api_url = f"https://www.screener.in/api/company/{company_id}/schedules/?parent={safe_parent}&section={section_id}&consolidated=true"

                    try:
                        # ARCHITECTURAL FIX: Add a half-second delay so the bot doesn't ban us during the loop
                        time.sleep(0.5)

                        sub_response = session.get(api_url)
                        if sub_response.status_code == 200:
                            sub_data = sub_response.json()

                            for sub_label, date_values in sub_data.items():
                                final_label = f"{sub_label}"

                                for d in headers:
                                    financial_data[d][final_label] = "0"

                                for date_key, val in date_values.items():
                                    clean_api_date = date_key.strip()

                                    if clean_api_date in financial_data:
                                        financial_data[clean_api_date][final_label] = (
                                            str(val).replace(",", "")
                                        )
                        else:
                            # ARCHITECTURAL FIX: Explicitly print the error code so we aren't blind
                            print(
                                f"    - Sub-API Error for '{row_label}': {sub_response.status_code}"
                            )

                    except Exception as e:
                        print(f"    - Assignment failed for '{row_label}': {e}")

        print(f"\nFinalized scraping {report_type} data from Screener.")
        with open(file_path, "w") as f:
            json.dump(financial_data, f)

        return financial_data

    return None


## ALL API CALL FUNCTION FOR FINANCIALS


def fetch_all_financials(ticker, requested_source="auto"):
    """
    Acts as a router. Fetches from the explicitly requested source.
    If 'auto', it exhausts FMP daily limits before smoothly rolling to Alpha Vantage.
    """
    print(f"\n" + "=" * 40)
    print(f"[{ticker}] INITIATING DATA FETCH (Source: {requested_source.upper()})")
    print("=" * 40)

    current_source = "fmp" if requested_source == "auto" else requested_source

    # --- FINANCIAL MODELING PREP (FMP) ---
    if current_source == "fmp":
        print(f"[{ticker}] Pinging Financial Modeling Prep (FMP)...")
        fmp_data = {}
        limit_hit = False

        for stmt in ["INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW"]:
            for freq in ["annual", "quarter"]:
                res = get_fmp_financials(ticker, stmt, freq, FMP_API_KEY)
                if res == "LIMIT_REACHED":
                    limit_hit = True
                    break
                fmp_data[f"{stmt}_{freq}"] = res
                time.sleep(0.5)
            if limit_hit:
                break

        if not limit_hit and all(v is not None for v in fmp_data.values()):
            print(f"[{ticker}] SUCCESS: Complete FMP dataset acquired.")
            return {"source": "fmp", "data": fmp_data}

        if limit_hit and requested_source == "auto":
            print(f"[{ticker}] AUTO-ROTATE TRIGGERED: Switching to Alpha Vantage...")
            current_source = "vantage"
        else:
            print(f"[{ticker}] FMP fetch failed or incomplete.")
            return None

    # --- ALPHA VANTAGE (Slow-Drain Defense) ---
    if current_source == "vantage":
        print(f"[{ticker}] Pinging Alpha Vantage (Slow-Drain Active)...")

        av_is = get_alpha_vantage(ticker, "INCOME_STATEMENT", API_KEY)
        time.sleep(20)  # DEFENSE: Protects the 5/min limit
        av_bs = get_alpha_vantage(ticker, "BALANCE_SHEET", API_KEY)
        time.sleep(20)  # DEFENSE: Protects the 5/min limit
        av_cf = get_alpha_vantage(ticker, "CASH_FLOW", API_KEY)

        if av_is and av_bs and av_cf and "annualReports" in av_is:
            print(f"[{ticker}] SUCCESS: Complete Alpha Vantage dataset acquired.")
            return {
                "source": "vantage",
                "data": {"is": av_is, "bs": av_bs, "cf": av_cf},
            }
        print(f"[{ticker}] Alpha Vantage fetch incomplete.")
        return None

    # --- YAHOO FINANCE ---
    if current_source == "yfinance":
        print(f"[{ticker}] Pinging YFinance...")
        yf_is_q = get_yfinance(ticker, "INCOME_STATEMENT", "quarterly")
        yf_is_y = get_yfinance(ticker, "INCOME_STATEMENT", "yearly")
        yf_bs_q = get_yfinance(ticker, "BALANCE_SHEET", "quarterly")
        yf_bs_y = get_yfinance(ticker, "BALANCE_SHEET", "yearly")
        yf_cf_y = get_yfinance(ticker, "CASH_FLOW", "yearly")

        if all(
            v is not None and not v.empty
            for v in [yf_is_q, yf_is_y, yf_bs_q, yf_bs_y, yf_cf_y]
        ):
            print(f"[{ticker}] SUCCESS: Complete YFinance dataset acquired.")
            return {
                "source": "yfinance",
                "data": {
                    "is_q": yf_is_q,
                    "is_y": yf_is_y,
                    "bs_q": yf_bs_q,
                    "bs_y": yf_bs_y,
                    "cf_y": yf_cf_y,
                    "cf_q": get_yfinance(ticker, "CASH_FLOW", "quarterly"),
                },
            }
        return None

    # --- SCREENER.IN ---
    if current_source == "screener":
        print(f"[{ticker}] Pinging Screener.in...")
        sc_is_q = get_screener_financials(ticker, report_type="quarterly")
        sc_is_y = get_screener_financials(ticker, report_type="yearly")
        sc_bs_y = get_screener_financials(ticker, report_type="balance-sheet")
        sc_cf_y = get_screener_financials(ticker, report_type="cash-flow")

        if sc_is_y and sc_bs_y and sc_cf_y:
            print(f"[{ticker}] SUCCESS: Complete Screener dataset acquired.")
            return {
                "source": "screener",
                "data": {
                    "is_q": sc_is_q,
                    "is_y": sc_is_y,
                    "bs_y": sc_bs_y,
                    "cf_y": sc_cf_y,
                },
            }
        return None

    return None


## CLEAN DATA FUNCTIONS


def update_company_profile(ticker: str):
    """Fetches Sector/Industry from yfinance and upserts into the database."""
    try:
        info = yf.Ticker(ticker).info

        # Use .get() so it doesn't crash if a specific stock is missing data
        sector = info.get("sector", "Unknown")
        industry = info.get("industry", "Unknown")
        short_name = info.get("shortName", ticker)

        # PostgreSQL Upsert Logic
        stmt = insert(company_profiles).values(
            Ticker=ticker, CompanyName=short_name, Sector=sector, Industry=industry
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["Ticker"],
            set_={
                "CompanyName": stmt.excluded.CompanyName,
                "Sector": stmt.excluded.Sector,
                "Industry": stmt.excluded.Industry,
            },
        )

        with engine.begin() as conn:
            conn.execute(stmt)

        print(f"[{ticker}] Profile Synced -> Sector: {sector} | Industry: {industry}")

    except Exception as e:
        print(f"[{ticker}] Warning: Failed to sync company profile: {e}")


def clean_financial_dataframe(df):

    return df.replace(r"[%,+]", "", regex=True).apply(pd.to_numeric, errors="coerce")


def format_statement_for_db(
    df,
    target_columns,
    ticker,
    currency,
    data_source,
    multiplier=1.0,
    index_col_name="index",
    transpose=False,
):

    if transpose:
        df = df.T

    # Filter to only the columns needed for the database
    clean_df = df.loc[:, target_columns]

    # normalize values decimals
    clean_df = (clean_df * multiplier).round(4)

    # Reset index to bring the dates into a standard column
    clean_df = clean_df.reset_index()

    # Rename the date column (handles Alpha Vantage vs Yfinance/Screener differences)
    clean_df = clean_df.rename(columns={index_col_name: "ReportDate"})

    # Standardize end-of-month date format
    clean_df["ReportDate"] = (
        pd.to_datetime(clean_df["ReportDate"]) + pd.offsets.MonthEnd(0)
    ).dt.strftime("%Y-%m-%d")

    # Insert Ticker at the beginning
    clean_df.insert(1, "Ticker", ticker)
    clean_df.insert(2, "Currency", currency)
    clean_df.insert(3, "DataSource", data_source)

    return clean_df


def to_pascal_case(text):

    if not isinstance(text, str):
        return text

    # Insert spaces before capital letters (e.g., "CostOfRevenue" -> "Cost Of Revenue")
    spaced_text = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)

    # Replace anything that is NOT a letter with a space
    clean_text = re.sub(r"[^a-zA-Z]", " ", spaced_text)

    # Split into individual words, capitalize the first letter, and glue together
    pascal_str = "".join(word.capitalize() for word in clean_text.split())

    return pascal_str


def standardize_dataframe_labels(df):

    df.index = [to_pascal_case(str(idx)) for idx in df.index]
    return df


# % to values for screener


def convert_screener_percentages_to_absolute(df_screener_is):

    if df_screener_is.attrs.get("screener_converted_to_absolute"):
        return df_screener_is

    # 1. Cost Items (Base = Sales)
    if "Sales" in df_screener_is.index:
        sales = df_screener_is.loc["Sales"].fillna(0)

        if "MaterialCost" in df_screener_is.index:
            df_screener_is.loc["MaterialCost"] = sales * (
                df_screener_is.loc["MaterialCost"].fillna(0) / 100
            )

        if "ManufacturingCost" in df_screener_is.index:
            df_screener_is.loc["ManufacturingCost"] = sales * (
                df_screener_is.loc["ManufacturingCost"].fillna(0) / 100
            )

        if "EmployeeCost" in df_screener_is.index:
            df_screener_is.loc["EmployeeCost"] = sales * (
                df_screener_is.loc["EmployeeCost"].fillna(0) / 100
            )

        if "OtherCost" in df_screener_is.index:
            df_screener_is.loc["OtherCost"] = sales * (
                df_screener_is.loc["OtherCost"].fillna(0) / 100
            )

    # 2. Tax Item (Base = Profit before tax)
    if "ProfitBeforeTax" in df_screener_is.index and "Tax" in df_screener_is.index:
        pbt = df_screener_is.loc["ProfitBeforeTax"].fillna(0)
        df_screener_is.loc["Tax"] = pbt * (df_screener_is.loc["Tax"].fillna(0) / 100)

    df_screener_is.attrs["screener_converted_to_absolute"] = True

    return df_screener_is


# CHECK ITEMS UNIFORM
def safe_fetch(df, target_item, synonym_map, bucket_mode=False):

    # Get the raw mapping from the dictionary
    raw_mapping = synonym_map.get(target_item, [target_item])

    if raw_mapping and isinstance(raw_mapping[0], str):
        search_groups = [raw_mapping]
    else:
        # It is already a list of lists (New CF dict)
        search_groups = raw_mapping

    if bucket_mode:
        # BUCKET: Sum exactly ONE item from each sub-list
        matched_values = []
        for group in search_groups:
            # Loop through aliases in the current group
            for label in group:
                if label in df.index:
                    result = df.loc[label]
                    val = result.iloc[0] if isinstance(result, pd.DataFrame) else result
                    matched_values.append(val.fillna(0))
                    break  # CRITICAL: Stop looking in this group to prevent double-counting!

        if matched_values:
            return sum(matched_values)

        return pd.Series(np.nan, index=df.columns)

    else:
        # SYNONYM: Stop entirely after finding the very first match anywhere
        for group in search_groups:
            for label in group:
                if label in df.index:
                    result = df.loc[label]
                    return (
                        result.iloc[0] if isinstance(result, pd.DataFrame) else result
                    )

        return pd.Series(np.nan, index=df.columns)


def map_statement_via_dictionary(df, synonym_map, target_columns, bucket_columns=None):
    if bucket_columns is None:
        bucket_columns = []

    mapped_data = {}

    # Run the scanner for every column you need
    for target_col in target_columns:
        # Pass bucket_mode=True only if the column is explicitly flagged as a bucket
        mode = True if target_col in bucket_columns else False
        mapped_data[target_col] = safe_fetch(
            df, target_col, synonym_map, bucket_mode=mode
        )

    return pd.DataFrame(mapped_data).T


def store_raw_data_jsonb(
    ticker, data_source, statement_type, raw_df, conn_engine, table_def
):
    if raw_df is None or (isinstance(raw_df, pd.DataFrame) and raw_df.empty):
        return

    df = pd.DataFrame(raw_df) if isinstance(raw_df, dict) else raw_df.copy()
    df = df.T

    df.columns = [to_pascal_case(str(col)) for col in df.columns]

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[df.index.notna()]
    df.index = (df.index + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")

    records = []
    for date_str, row in df.iterrows():
        clean_row = row.dropna().to_dict()
        records.append(
            {
                "DataSource": data_source,
                "Ticker": ticker,
                "ReportDate": date_str,
                "StatementType": statement_type,
                "RawData": clean_row,
            }
        )

    if not records:
        return

    insert_stmt = insert(table_def).values(records)
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["Ticker", "ReportDate", "StatementType"],
        set_={
            "RawData": insert_stmt.excluded.RawData,
            "DataSource": insert_stmt.excluded.DataSource,
        },
    )

    with conn_engine.begin() as conn:
        conn.execute(upsert_stmt)

    print(
        f"      [BRONZE VAULT] Saved {len(records)} raw {statement_type} records for {ticker}."
    )


## FALLBACK FUNTIONS


# FALLBACK


# Income Statement Fallback
def apply_income_statement_fallbacks(df, target_columns):

    # CostOfRevenue Fallback: Pure addition of absolute values (No Revenue Multiplier)
    if df.loc["CostOfRevenue"].isna().any():
        if "MaterialCost" in df.index and "ManufacturingCost" in df.index:
            calc_cost = df.loc["MaterialCost"].fillna(0) + df.loc[
                "ManufacturingCost"
            ].fillna(0)
            has_screener_cogs = ~(
                df.loc["MaterialCost"].isna() & df.loc["ManufacturingCost"].isna()
            )
            df.loc["CostOfRevenue"] = df.loc["CostOfRevenue"].fillna(
                calc_cost.where(has_screener_cogs)
            )

        elif "GrossProfit" in df.index:
            calc_cost_gaap = df.loc["TotalRevenue"] - df.loc["GrossProfit"]
            df.loc["CostOfRevenue"] = df.loc["CostOfRevenue"].fillna(calc_cost_gaap)

    # GrossProfit Fallback: TotalRevenue - CostOfRevenue
    if df.loc["GrossProfit"].isna().any():
        calc_gp = df.loc["TotalRevenue"] - df.loc["CostOfRevenue"].fillna(0)
        df.loc["GrossProfit"] = df.loc["GrossProfit"].fillna(calc_gp)

    # OperatingExpense Fallback: Anchor Row minus Calculated COGS (No Revenue Multiplier)
    if df.loc["OperatingExpense"].isna().any():
        if "TotalScreenerExpenses" in df.index:
            calc_opex = df.loc["TotalScreenerExpenses"] - df.loc[
                "CostOfRevenue"
            ].fillna(0)
            df.loc["OperatingExpense"] = df.loc["OperatingExpense"].fillna(calc_opex)

        elif "OperatingIncome" in df.index:
            calc_opex_gaap = df.loc["GrossProfit"].fillna(0) - df.loc["OperatingIncome"]
            df.loc["OperatingExpense"] = df.loc["OperatingExpense"].fillna(
                calc_opex_gaap
            )

    # OperatingIncome Fallback (Ensure calculation exists if API skips it)
    calc_op_inc = df.loc["GrossProfit"].fillna(0) - df.loc["OperatingExpense"].fillna(0)

    # If the API skipped it, fill it
    if df.loc["OperatingIncome"].isna().any():
        df.loc["OperatingIncome"] = df.loc["OperatingIncome"].fillna(calc_op_inc)

    # If the API gave a number that mathematically violates the GP - Opex equation by more than a rounding error, overwrite it.
    discrepancy = (df.loc["OperatingIncome"] - calc_op_inc).abs()
    df.loc["OperatingIncome"] = df.loc["OperatingIncome"].where(
        discrepancy < 10, calc_op_inc
    )

    # NetInterestIncome Fallback: PretaxIncome - OperatingIncome
    if df.loc["NetInterestIncome"].isna().any():
        calc_interest = df.loc["PretaxIncome"] - df.loc["OperatingIncome"]
        df.loc["NetInterestIncome"] = df.loc["NetInterestIncome"].fillna(calc_interest)

    # TaxProvision Fallback: PretaxIncome - NetIncome
    if df.loc["TaxProvision"].isna().any():
        calc_tax = df.loc["PretaxIncome"] - df.loc["NetIncome"]
        df.loc["TaxProvision"] = df.loc["TaxProvision"].fillna(calc_tax)

    # Isolate the strict Ittelson columns and safely convert any remaining NaNs to 0
    final_df = df.loc[target_columns].fillna(0)

    return final_df


# Balance Sheet Fallback


def apply_balance_sheet_fallbacks(df, target_columns):

    # ASSETS

    # Cash & Equivalents Fallback
    if df.loc["CashCashEquivalentsAndShortTermInvestments"].isna().any():
        calc_cash = df.loc["CashEquivalents"].fillna(0)
        # using .get shows attribute error
        if "ShortTermInvestments" in df.index:
            calc_cash += df.loc["ShortTermInvestments"].fillna(0)

        df.loc["CashCashEquivalentsAndShortTermInvestments"] = df.loc[
            "CashCashEquivalentsAndShortTermInvestments"
        ].fillna(calc_cash)

    # Current Assets Fallback
    if df.loc["CurrentAssets"].isna().any():
        calc_ca = (
            df.loc["Inventory"].fillna(0)
            + df.loc["Receivables"].fillna(0)
            + df.loc["CashEquivalents"].fillna(0)
            + df.loc["LoansNAdvances"].fillna(0)
            + df.loc["OtherAssetItems"].fillna(0)
        )
        df.loc["CurrentAssets"] = df.loc["CurrentAssets"].fillna(calc_ca)

    # Inventory
    if df.loc["Inventory"].isna().any():
        calc_inv = df.loc["CurrentAssets"] - (
            df.loc["CashCashEquivalentsAndShortTermInvestments"].fillna(0)
            + df.loc["Receivables"].fillna(0)
            + df.loc["LoansNAdvances"].fillna(0)
            + df.loc["OtherAssetItems"].fillna(0)
        )
        df.loc["Inventory"] = df.loc["Inventory"].fillna(calc_inv)

    # Total Non-Current Assets Fallback
    if df.loc["TotalNonCurrentAssets"].isna().any():
        calc_nca = df.loc["TotalAssets"] - df.loc["CurrentAssets"]
        df.loc["TotalNonCurrentAssets"] = df.loc["TotalNonCurrentAssets"].fillna(
            calc_nca
        )

    # PPE Math
    if df.loc["NetPPE"].isna().any():
        df.loc["NetPPE"] = df.loc["NetPPE"].fillna(
            df.loc["GrossPPE"] - df.loc["AccumulatedDepreciation"].fillna(0)
        )

    if df.loc["GrossPPE"].isna().any():
        df.loc["GrossPPE"] = df.loc["GrossPPE"].fillna(
            df.loc["NetPPE"] + df.loc["AccumulatedDepreciation"].fillna(0)
        )

    # LIABILITIES
    # Payables & Accrued Expenses Fallback
    if df.loc["PayablesAndAccruedExpenses"].isna().any():
        calc_payables = df.loc["TradePayables"].fillna(0) + df.loc[
            "AdvanceFromCustomers"
        ].fillna(0)
        df.loc["PayablesAndAccruedExpenses"] = df.loc[
            "PayablesAndAccruedExpenses"
        ].fillna(calc_payables)

    # Current Debt Fallback
    if df.loc["CurrentDebtAndCapitalLeaseObligation"].isna().any():
        calc_cdebt = df.loc["ShortTermBorrowings"].fillna(0) + df.loc[
            "LeaseLiabilities"
        ].fillna(0)
        df.loc["CurrentDebtAndCapitalLeaseObligation"] = df.loc[
            "CurrentDebtAndCapitalLeaseObligation"
        ].fillna(calc_cdebt)

    # Current Liabilities Fallback
    if df.loc["CurrentLiabilities"].isna().any():
        calc_cl = (
            df.loc["PayablesAndAccruedExpenses"].fillna(0)
            + df.loc["CurrentDebtAndCapitalLeaseObligation"].fillna(0)
            + df.loc["OtherLiabilityItems"].fillna(0)
        )
        df.loc["CurrentLiabilities"] = df.loc["CurrentLiabilities"].fillna(calc_cl)

    # Long-Term Debt Fallback
    if df.loc["LongTermDebtAndCapitalLeaseObligation"].isna().any():
        calc_ltdebt = df.loc["LongTermBorrowings"].fillna(0) + df.loc[
            "OtherBorrowings"
        ].fillna(0)
        df.loc["LongTermDebtAndCapitalLeaseObligation"] = df.loc[
            "LongTermDebtAndCapitalLeaseObligation"
        ].fillna(calc_ltdebt)

    # Total Liabilities Fallback
    if df.loc["TotalLiabilitiesNetMinorityInterest"].isna().any():
        calc_tl = df.loc["Borrowings"].fillna(0) + df.loc["OtherLiabilities"].fillna(0)
        df.loc["TotalLiabilitiesNetMinorityInterest"] = df.loc[
            "TotalLiabilitiesNetMinorityInterest"
        ].fillna(calc_tl)

    # --- EQUITY ---
    # Stockholders Equity Fallback
    if df.loc["StockholdersEquity"].isna().any():
        calc_equity = df.loc["CapitalStock"].fillna(0) + df.loc[
            "RetainedEarnings"
        ].fillna(0)
        df.loc["StockholdersEquity"] = df.loc["StockholdersEquity"].fillna(calc_equity)

    final_df = df.loc[target_columns].fillna(0)

    if "TotalAssets" in final_df.index:
        # Keep only columns (years) where Total Assets is strictly greater than 0
        valid_columns = final_df.columns[final_df.loc["TotalAssets"] > 0]
        final_df = final_df[valid_columns]

    return final_df


# Cash Flow Fallbacks


def apply_cash_flow_fallbacks(df, target_columns, df_is_calc=None, df_bs_calc=None):

    #  NET BORROWING
    if df.loc["NetBorrowing"].isna().any():
        if "IssuanceOfDebt" in df.index and "RepaymentOfDebt" in df.index:
            calc_borrowing = df.loc["IssuanceOfDebt"].fillna(0) - df.loc[
                "RepaymentOfDebt"
            ].fillna(0)

            # Ensure we only fill where we actually had data (don't inject 0s if both were NaN)
            has_debt_data = ~(
                df.loc["IssuanceOfDebt"].isna() & df.loc["RepaymentOfDebt"].isna()
            )
            df.loc["NetBorrowing"] = df.loc["NetBorrowing"].fillna(
                calc_borrowing.where(has_debt_data)
            )

    #  NET BORROWING (Balance Sheet Bridge for missing Q data)
    if df.loc["NetBorrowing"].isna().any() and df_bs_calc is not None:
        if (
            "LongTermDebtAndCapitalLeaseObligation" in df_bs_calc.index
            and "CurrentDebtAndCapitalLeaseObligation" in df_bs_calc.index
        ):
            common_cols = df.columns.intersection(df_bs_calc.columns)

            total_debt = df_bs_calc.loc[
                "LongTermDebtAndCapitalLeaseObligation", common_cols
            ].fillna(0) + df_bs_calc.loc[
                "CurrentDebtAndCapitalLeaseObligation", common_cols
            ].fillna(
                0
            )

            # Temporarily sort chronologically to calculate the difference
            temp_s = total_debt.copy()
            original_idx = temp_s.index
            temp_s.index = pd.to_datetime(temp_s.index)
            diff_s = temp_s.sort_index().diff()

            mapping = {pd.to_datetime(idx): idx for idx in original_idx}
            diff_s.index = diff_s.index.map(mapping)
            calc_bridge = diff_s[original_idx]

            df.loc["NetBorrowing", common_cols] = df.loc[
                "NetBorrowing", common_cols
            ].fillna(calc_bridge)

    #  ENDING CASH (From Balance Sheet)
    # We always bridge this directly from the BS as the absolute source of truth
    if df.loc["EndingCashBalance"].isna().any() and df_bs_calc is not None:
        if "CashCashEquivalentsAndShortTermInvestments" in df_bs_calc.index:
            common_cols = df.columns.intersection(df_bs_calc.columns)
            df.loc["EndingCashBalance", common_cols] = df.loc[
                "EndingCashBalance", common_cols
            ].fillna(
                df_bs_calc.loc[
                    "CashCashEquivalentsAndShortTermInvestments", common_cols
                ]
            )

    #  BEGINNING CASH (Internal CF Math)
    # Strictly calculated using the bridged Ending Cash and the internal NetCashFlow
    if df.loc["BeginningCashBalance"].isna().any():
        if "NetCashFlow" in df.index:
            calc_beg = df.loc["EndingCashBalance"].fillna(0) - df.loc[
                "NetCashFlow"
            ].fillna(0)
            has_beg_data = ~(
                df.loc["EndingCashBalance"].isna() & df.loc["NetCashFlow"].isna()
            )
            df.loc["BeginningCashBalance"] = df.loc["BeginningCashBalance"].fillna(
                calc_beg.where(has_beg_data)
            )

    #  DIRECT METHOD CONVERSIONS
    # Cash Receipts (Income Statement Bridge)
    if df.loc["CashReceipts"].isna().any() and df_is_calc is not None:
        if "TotalRevenue" in df_is_calc.index:
            common_cols = df.columns.intersection(df_is_calc.columns)
            df.loc["CashReceipts", common_cols] = df.loc[
                "CashReceipts", common_cols
            ].fillna(df_is_calc.loc["TotalRevenue", common_cols])

    # Cash Disbursements (Internal CF Math)
    if df.loc["CashDisbursements"].isna().any():
        calc_disbursements = df.loc["CashReceipts"].fillna(0) - df.loc[
            "CashFromOperations"
        ].fillna(0)
        has_disb_data = ~(
            df.loc["CashReceipts"].isna() & df.loc["CashFromOperations"].isna()
        )
        df.loc["CashDisbursements"] = df.loc["CashDisbursements"].fillna(
            calc_disbursements.where(has_disb_data)
        )

    #  FINAL CLEANUP
    final_df = df.loc[target_columns].fillna(0)

    return final_df


def apply_indirect_cash_flow_fallbacks(
    df, target_columns, df_is_calc=None, df_bs_calc=None
):

    #  Standardize all indices to String YYYY-MM-DD to prevent Timestamp KeyErrors
    df.columns = [pd.to_datetime(c).strftime("%Y-%m-%d") for c in df.columns]

    if df_bs_calc is not None:
        df_bs_calc.columns = [
            pd.to_datetime(c).strftime("%Y-%m-%d") for c in df_bs_calc.columns
        ]

        #  Identify dates present in BOTH statements
        common_cols = df.columns.intersection(df_bs_calc.columns)

        # Prepare sorted BS for the dates we actually have in CF
        bs_subset = df_bs_calc[common_cols].copy()
        bs_subset.columns = pd.to_datetime(bs_subset.columns)
        bs_sorted = bs_subset.sort_index(axis=1)

        #  DEPRECIATION
        if "DepreciationAndAmortization" in df.index:
            if "AccumulatedDepreciation" in bs_sorted.index:
                # .diff() results in NaN for the first year; we fill with 0 to avoid KeyError
                dep_diff = (
                    bs_sorted.loc["AccumulatedDepreciation"].diff().abs().fillna(0)
                )
                dep_diff.index = [c.strftime("%Y-%m-%d") for c in dep_diff.index]
                df.loc["DepreciationAndAmortization", common_cols] = df.loc[
                    "DepreciationAndAmortization", common_cols
                ].fillna(dep_diff)

        #  WORKING CAPITAL (Prior - Current)
        # ONLY use BS differences if the actual CF values are 0 or NaN

        if (
            "ChangeInAccountsReceivable" in df.index
            and "Receivables" in bs_sorted.index
        ):
            ar_diff = -bs_sorted.loc["Receivables"].diff().fillna(0)
            ar_diff.index = [c.strftime("%Y-%m-%d") for c in ar_diff.index]
            # Replace NaNs or absolute 0s with the BS diff
            df.loc["ChangeInAccountsReceivable", common_cols] = (
                df.loc["ChangeInAccountsReceivable", common_cols]
                .replace(0, np.nan)
                .fillna(ar_diff)
                .fillna(0)
            )

        if "ChangeInInventory" in df.index and "Inventory" in bs_sorted.index:
            inv_diff = -bs_sorted.loc["Inventory"].diff().fillna(0)
            inv_diff.index = [c.strftime("%Y-%m-%d") for c in inv_diff.index]
            df.loc["ChangeInInventory", common_cols] = (
                df.loc["ChangeInInventory", common_cols]
                .replace(0, np.nan)
                .fillna(inv_diff)
                .fillna(0)
            )

        if (
            "ChangeInAccountsPayable" in df.index
            and "PayablesAndAccruedExpenses" in bs_sorted.index
        ):
            ap_diff = bs_sorted.loc["PayablesAndAccruedExpenses"].diff().fillna(0)
            ap_diff.index = [c.strftime("%Y-%m-%d") for c in ap_diff.index]
            df.loc["ChangeInAccountsPayable", common_cols] = (
                df.loc["ChangeInAccountsPayable", common_cols]
                .replace(0, np.nan)
                .fillna(ap_diff)
                .fillna(0)
            )

        # CASH ANCHORS
        if (
            "EndingCash" in df.index
            and "CashCashEquivalentsAndShortTermInvestments" in bs_sorted.index
        ):
            cash_vals = bs_sorted.loc["CashCashEquivalentsAndShortTermInvestments"]
            cash_vals.index = [c.strftime("%Y-%m-%d") for c in cash_vals.index]
            df.loc["EndingCash", common_cols] = df.loc[
                "EndingCash", common_cols
            ].fillna(cash_vals)

        if (
            "BeginningCash" in df.index
            and "CashCashEquivalentsAndShortTermInvestments" in bs_sorted.index
        ):
            # Shift allows us to get the previous year's ending cash
            beg_cash = (
                bs_sorted.loc["CashCashEquivalentsAndShortTermInvestments"]
                .shift(1)
                .fillna(0)
            )
            beg_cash.index = [c.strftime("%Y-%m-%d") for c in beg_cash.index]
            df.loc["BeginningCash", common_cols] = df.loc[
                "BeginningCash", common_cols
            ].fillna(beg_cash)

    #  Income Statement Bridge
    if df_is_calc is not None and "NetIncome" in df.index:
        df_is_calc.columns = [
            pd.to_datetime(c).strftime("%Y-%m-%d") for c in df_is_calc.columns
        ]
        is_cols = df.columns.intersection(df_is_calc.columns)
        if "NetIncome" in df_is_calc.index:
            df.loc["NetIncome", is_cols] = df.loc["NetIncome", is_cols].fillna(
                df_is_calc.loc["NetIncome", is_cols]
            )

    # Final Totals
    op_items = [
        "NetIncome",
        "DepreciationAndAmortization",
        "OtherNonCashAdjustments",
        "ChangeInAccountsReceivable",
        "ChangeInInventory",
        "ChangeInAccountsPayable",
        "OtherWorkingCapital_Changes",
    ]
    valid_op = [i for i in op_items if i in df.index]
    df.loc["TotalOperatingCashFlow"] = df.loc["TotalOperatingCashFlow"].fillna(
        df.loc[valid_op].sum()
    )

    return df.loc[target_columns].fillna(0)


# Validation Check


def validate_financial_statements(
    df_is,
    df_bs,
    df_cf,
    df_indirect_cf=None,
    ticker=None,
    df_cf_raw=None,
    stmt_multiplier=1.0,
):
    print(f"\n{'='*40}")
    print(f"RUNNING 3-STATEMENT VALIDATION: {ticker}")
    print(f"{'='*40}")

    df_is = df_is.set_index("ReportDate")
    df_bs = df_bs.set_index("ReportDate")
    df_cf = df_cf.set_index("ReportDate")
    if df_indirect_cf is not None:
        df_indirect_cf = df_indirect_cf.set_index("ReportDate")

    print("\n--- DIRECT STATEMENTS AUDIT ---")

    bs_calc = df_bs["TotalLiabilitiesNetMinorityInterest"] + df_bs["StockholdersEquity"]
    bs_gap = df_bs["TotalAssets"] - bs_calc
    bs_check = bs_gap.abs() <= 10
    if bs_check.all():
        print(" Balance Sheet Equation (Assets = L + E): PERFECT MATCH")
    else:
        print("❌ BALANCE SHEET LEAK DETECTED:")
        for date, is_valid in bs_check.items():
            if not is_valid:
                print(
                    f"   -> [{date}] Assets: {df_bs.at[date, 'TotalAssets']:.2f} | Calculated L+E: {bs_calc[date]:.2f} | Gap: {bs_gap[date]:.2f}"
                )

    is_calc = df_is["GrossProfit"] - df_is["OperatingExpense"]
    is_gap = df_is["OperatingIncome"] - is_calc
    is_check = is_gap.abs() < 1
    if is_check.all():
        print(" Income Statement Equation (GP - Opex = OpInc): PERFECT MATCH")
    else:
        print("❌ INCOME STATEMENT LEAK DETECTED:")
        for date, is_valid in is_check.items():
            if not is_valid:
                print(
                    f"   -> [{date}] OpInc: {df_is.at[date, 'OperatingIncome']:.2f} | Calculated (GP-Opex): {is_calc[date]:.2f} | Gap: {is_gap[date]:.2f}"
                )

    common_bs_cf = df_cf.index.intersection(df_bs.index)
    bs_aggregate = df_bs.loc[common_bs_cf, "CashCashEquivalentsAndShortTermInvestments"]
    cf_cash = df_cf.loc[common_bs_cf, "EndingCashBalance"]
    bs_cf_gap = cf_cash - bs_aggregate
    bs_cf_check = (bs_cf_gap.abs() < 5.0) | (cf_cash <= bs_aggregate + 5.0)

    if bs_cf_check.all():
        print(" BS/CF Cash Link (Ending Cash): PERFECT MATCH")
    else:
        print(" BS/CF LINK LEAK DETECTED:")
        for date, is_valid in bs_cf_check.items():
            if not is_valid:
                print(
                    f"   -> [{date}] CF Ending Cash: {cf_cash[date]:.2f} | BS Cash: {bs_aggregate[date]:.2f} | Gap: {bs_cf_gap[date]:.2f}"
                )

    cf_calc = df_cf["CashReceipts"] - df_cf["CashDisbursements"]
    cf_gap = df_cf["CashFromOperations"] - cf_calc
    cf_check = cf_gap.abs() < 1
    if cf_check.all():
        print(" Direct Cash Flow Equation: PERFECT MATCH")
    else:
        print(" DIRECT CASH FLOW LEAK DETECTED:")
        for date, is_valid in cf_check.items():
            if not is_valid:
                print(
                    f"   -> [{date}] CFO: {df_cf.at[date, 'CashFromOperations']:.2f} | Calc (Rec-Disb): {cf_calc[date]:.2f} | Gap: {cf_gap[date]:.2f}"
                )

    audit_dict = {
        "BS_IsValid": bs_check,
        "IS_IsValid": is_check,
        "BS_CF_Link_Match": bs_cf_check,
        "Direct_CF_Match": cf_check,
    }

    if df_indirect_cf is not None:
        print("\n--- TARGETED INDIRECT CASH FLOW FORENSIC AUDIT ---")
        is_screener = ("DataSource" in df_indirect_cf.columns) and (
            str(df_indirect_cf["DataSource"].iloc[0]).lower() == "screener"
        )

        calc_ocf_base = (
            df_indirect_cf["NetIncome"]
            + df_indirect_cf["OtherNonCashAdjustments"]
            + df_indirect_cf["ChangeInAccountsReceivable"]
            + df_indirect_cf["ChangeInInventory"]
            + df_indirect_cf["ChangeInAccountsPayable"]
            + df_indirect_cf["OtherWorkingCapitalChanges"]
            + (
                df_indirect_cf["IncomeTaxPaid"].fillna(0)
                if is_screener
                else df_indirect_cf["DepreciationAndAmortization"]
            )
        )

        calc_icf_base = (
            df_indirect_cf["CapExPurchaseOfPPE"]
            + df_indirect_cf["PurchaseSaleOfInvestments"]
            + df_indirect_cf["OtherInvestingActivities"]
        )
        calc_fcf_base = (
            df_indirect_cf["NetDebtIssuedRepaid"]
            + df_indirect_cf["NetStockIssuedRepurchased"]
            + df_indirect_cf["DividendsPaid"]
            + df_indirect_cf["OtherFinancingActivities"]
        )

        gap_ocf = df_indirect_cf["TotalOperatingCashFlow"] - calc_ocf_base
        gap_icf = df_indirect_cf["TotalInvestingCashFlow"] - calc_icf_base
        gap_fcf = df_indirect_cf["TotalFinancingCashFlow"] - calc_fcf_base

        df_indirect_cf["Unmapped_Operating"] = 0.0
        df_indirect_cf["Unmapped_Investing"] = 0.0
        df_indirect_cf["Unmapped_Financing"] = 0.0

        for date in df_indirect_cf.index:
            if abs(gap_ocf[date]) > 5:
                df_indirect_cf.at[date, "Unmapped_Operating"] = gap_ocf[date]
                print(
                    f"   -> [{date}] OCF Leak: Plugging {gap_ocf[date]:.2f} into Unmapped_Operating"
                )
            if abs(gap_icf[date]) > 5:
                df_indirect_cf.at[date, "Unmapped_Investing"] = gap_icf[date]
                print(
                    f"   -> [{date}] ICF Leak: Plugging {gap_icf[date]:.2f} into Unmapped_Investing"
                )
            if abs(gap_fcf[date]) > 5:
                df_indirect_cf.at[date, "Unmapped_Financing"] = gap_fcf[date]
                print(
                    f"   -> [{date}] FCF Leak: Plugging {gap_fcf[date]:.2f} into Unmapped_Financing"
                )

        calc_ocf_final = calc_ocf_base + df_indirect_cf["Unmapped_Operating"]
        calc_icf_final = calc_icf_base + df_indirect_cf["Unmapped_Investing"]
        calc_fcf_final = calc_fcf_base + df_indirect_cf["Unmapped_Financing"]

        indirect_ocf_check = (
            df_indirect_cf["TotalOperatingCashFlow"] - calc_ocf_final
        ).abs() <= (df_indirect_cf["TotalOperatingCashFlow"].abs() * 0.05)

        dust_tolerance = 15.0
        calc_net_change = calc_ocf_final + calc_icf_final + calc_fcf_final
        indirect_net_change_check = (
            df_indirect_cf["NetChangeInCash"] - calc_net_change
        ).abs() <= dust_tolerance

        pure_calc_ending = (
            df_indirect_cf["BeginningCash"]
            + df_indirect_cf["NetChangeInCash"]
            + df_indirect_cf["EffectOfExchangeRates"].fillna(0)
        )

        # 2. Calculate the TRUE total leak
        total_leak = df_indirect_cf["EndingCash"] - pure_calc_ending

        # 3. Store the actual mathematical leak regardless of what the API says
        df_indirect_cf["Unmapped_Rollforward"] = total_leak

        # 4. Extract the API's plug to see if it explains our leak
        raw_adj = pd.Series(0.0, index=df_indirect_cf.index)
        if (
            df_cf_raw is not None
            and "OtherCashAdjustmentOutsideChangeinCash" in df_cf_raw.index
        ):
            extracted_adj = df_cf_raw.loc[
                "OtherCashAdjustmentOutsideChangeinCash"
            ].fillna(0)
            extracted_adj.index = (
                pd.to_datetime(extracted_adj.index) + pd.offsets.MonthEnd(0)
            ).strftime("%Y-%m-%d")
            raw_adj = extracted_adj * stmt_multiplier

        # 5. Validation Check: The statement is valid ONLY if the API's adjustment perfectly explains the total leak
        indirect_rollforward_check = (total_leak - raw_adj).abs() <= dust_tolerance

        # 6. Logging the results
        for date in df_indirect_cf.index:
            leak_val = df_indirect_cf.at[date, "Unmapped_Rollforward"]
            if abs(leak_val) > dust_tolerance:
                if abs(leak_val - raw_adj.get(date, 0)) <= dust_tolerance:
                    print(
                        f"   -> [{date}] Rollforward Leak: {leak_val:.2f} (Successfully reconciled with raw API adjustment)"
                    )
                else:
                    print(
                        f"   -> [{date}] CRITICAL ROLLFORWARD LEAK: {leak_val:.2f} (API adjustment {raw_adj.get(date, 0):.2f} failed to reconcile)"
                    )

        audit_dict.update(
            {
                "Indirect_CF_OCF_Match": indirect_ocf_check,
                "Indirect_CF_NetChange_Match": indirect_net_change_check,
                "Indirect_CF_Rollforward": indirect_rollforward_check,
            }
        )

    return pd.DataFrame(audit_dict), (
        df_indirect_cf.reset_index()
        if df_indirect_cf is not None
        else pd.DataFrame(audit_dict)
    )


# INDIRECT BUCKETS

indirect_cf_buckets = [
    "OtherNonCashAdjustments",
    "OtherWorkingCapitalChanges",
    "NetDebtIssuedRepaid",
    "NetStockIssuedRepurchased",
    "PurchaseSaleOfInvestments",
    "OtherInvestingActivities",
    "OtherFinancingActivities",
]

# CALL ALL API FUNCTION


def run_etl_pipeline(target_tickers, ai_mode="local", requested_source="auto"):
    """
    Executes the ETL pipeline for the provided list of tickers and
    returns a summary matrix of the forensic validation results.
    """
    failed_tickers = []
    batch_summary = []

    print("\n" + "=" * 40)
    print(f"STARTING BATCH PROCESSING (Mode: {ai_mode.upper()})")
    print("=" * 40)

    try:
        # Boot the PyTorch/Ollama engine if running locally
        if ai_mode == "local":
            runtime.load_models()

        for ticker in target_tickers:
            # FETCH UNIFIED PAYLOAD
            payload = fetch_all_financials(ticker, requested_source)

            if not payload:
                failed_tickers.append(ticker)
                batch_summary.append(
                    {
                        "Ticker": ticker,
                        "Status": "Failed to Fetch",
                        "Direct Validation": "N/A",
                        "Indirect Validation": "N/A",
                        "Rows Upserted": 0,
                    }
                )
                continue

            source = payload["source"]
            data = payload["data"]
            update_company_profile(ticker)
            # UNPACK TO DATAFRAMES
            print(f"[{ticker}] Formatting DataFrames for {source}...")

            if source == "yfinance":
                dfIncomeStatementQ = pd.DataFrame(data["is_q"])
                dfIncomeStatementY = pd.DataFrame(data["is_y"])
                dfBalanceSheetQ = pd.DataFrame(data["bs_q"])
                dfBalanceSheetY = pd.DataFrame(data["bs_y"])
                dfCashFlowY = pd.DataFrame(data["cf_y"])
                dfCashFlowQ = (
                    pd.DataFrame(data["cf_q"]) if data["cf_q"] is not None else None
                )

            elif source == "vantage":
                dfIncomeStatementQ = (
                    pd.DataFrame(data["is"]["quarterlyReports"])
                    .set_index("fiscalDateEnding")
                    .rename_axis(None)
                    .T
                )
                dfIncomeStatementY = (
                    pd.DataFrame(data["is"]["annualReports"])
                    .set_index("fiscalDateEnding")
                    .rename_axis(None)
                    .T
                )
                dfBalanceSheetQ = pd.DataFrame(
                    data["bs"]["quarterlyReports"]
                ).set_index("fiscalDateEnding")
                dfBalanceSheetY = pd.DataFrame(data["bs"]["annualReports"]).set_index(
                    "fiscalDateEnding"
                )
                dfCashFlowQ = pd.DataFrame(data["cf"]["quarterlyReports"]).set_index(
                    "fiscalDateEnding"
                )
                dfCashFlowY = pd.DataFrame(data["cf"]["annualReports"]).set_index(
                    "fiscalDateEnding"
                )

            elif source == "screener":
                dfIncomeStatementQ = pd.DataFrame(data["is_q"])
                dfIncomeStatementY = pd.DataFrame(data["is_y"])
                dfBalanceSheetY = pd.DataFrame(data["bs_y"])
                dfCashFlowY = pd.DataFrame(data["cf_y"])
                dfBalanceSheetQ = None
                dfCashFlowQ = None

            elif source == "fmp":
                dfIncomeStatementQ = (
                    pd.DataFrame(data["INCOME_STATEMENT_quarter"])
                    .set_index("date")
                    .rename_axis(None)
                    .T
                )
                dfIncomeStatementY = (
                    pd.DataFrame(data["INCOME_STATEMENT_annual"])
                    .set_index("date")
                    .rename_axis(None)
                    .T
                )
                dfBalanceSheetQ = (
                    pd.DataFrame(data["BALANCE_SHEET_quarter"])
                    .set_index("date")
                    .rename_axis(None)
                    .T
                )
                dfBalanceSheetY = (
                    pd.DataFrame(data["BALANCE_SHEET_annual"])
                    .set_index("date")
                    .rename_axis(None)
                    .T
                )
                dfCashFlowQ = (
                    pd.DataFrame(data["CASH_FLOW_quarter"])
                    .set_index("date")
                    .rename_axis(None)
                    .T
                )
                dfCashFlowY = (
                    pd.DataFrame(data["CASH_FLOW_annual"])
                    .set_index("date")
                    .rename_axis(None)
                    .T
                )

            print(f"[{ticker}] Backing up raw data to JSONB Vault...")

            store_raw_data_jsonb(
                ticker, source, "IS_Y", dfIncomeStatementY, engine, raw_financials
            )
            store_raw_data_jsonb(
                ticker, source, "BS_Y", dfBalanceSheetY, engine, raw_financials
            )
            store_raw_data_jsonb(
                ticker, source, "CF_Y", dfCashFlowY, engine, raw_financials
            )

            if dfIncomeStatementQ is not None:
                store_raw_data_jsonb(
                    ticker, source, "IS_Q", dfIncomeStatementQ, engine, raw_financials
                )
            if dfBalanceSheetQ is not None:
                store_raw_data_jsonb(
                    ticker, source, "BS_Q", dfBalanceSheetQ, engine, raw_financials
                )
            if dfCashFlowQ is not None:
                store_raw_data_jsonb(
                    ticker, source, "CF_Q", dfCashFlowQ, engine, raw_financials
                )

            print(f"[{ticker}] Cleaning and Mapping Data for Silver Tables...")

            if source == "vantage":
                stmt_currency = "USD"
                stmt_multiplier = 0.000001
            elif source == "yfinance":
                ticker_info = yf.Ticker(ticker).info
                stmt_currency = ticker_info.get(
                    "financialCurrency", ticker_info.get("currency", "USD")
                ).upper()
                stmt_multiplier = 0.000001
            elif source == "screener":
                stmt_currency = "INR"
                stmt_multiplier = 10.0
            elif source == "fmp":
                # Safely get the currency, default to USD
                stmt_currency = (
                    str(dfIncomeStatementY.loc["reportedCurrency"].iloc[0]).upper()
                    if "reportedCurrency" in dfIncomeStatementY.index
                    else "USD"
                )
                stmt_multiplier = 0.000001

            if dfIncomeStatementY is not None:
                dfIncomeStatementY = clean_financial_dataframe(
                    standardize_dataframe_labels(to_pascal_case(dfIncomeStatementY))
                )
            if dfIncomeStatementQ is not None:
                dfIncomeStatementQ = clean_financial_dataframe(
                    standardize_dataframe_labels(to_pascal_case(dfIncomeStatementQ))
                )
            if dfBalanceSheetY is not None:
                dfBalanceSheetY = clean_financial_dataframe(
                    standardize_dataframe_labels(to_pascal_case(dfBalanceSheetY))
                )
            if dfBalanceSheetQ is not None:
                dfBalanceSheetQ = clean_financial_dataframe(
                    standardize_dataframe_labels(to_pascal_case(dfBalanceSheetQ))
                )
            if dfCashFlowY is not None:
                dfCashFlowY = clean_financial_dataframe(
                    standardize_dataframe_labels(to_pascal_case(dfCashFlowY))
                )
            if dfCashFlowQ is not None:
                dfCashFlowQ = clean_financial_dataframe(
                    standardize_dataframe_labels(to_pascal_case(dfCashFlowQ))
                )

            if source == "screener":
                if dfIncomeStatementQ is not None:
                    dfIncomeStatementQ = convert_screener_percentages_to_absolute(
                        dfIncomeStatementQ
                    )
                if dfIncomeStatementY is not None:
                    dfIncomeStatementY = convert_screener_percentages_to_absolute(
                        dfIncomeStatementY
                    )

            if source in ["yfinance", "screener"]:
                is_keys = ittelson_income_statement_columns + [
                    "PretaxIncome",
                    "MaterialCost",
                    "ManufacturingCost",
                    "EmployeeCost",
                    "OtherCost",
                ]
                bs_keys = ittelson_balance_sheet_columns + [
                    "CashEquivalents",
                    "Investments",
                    "LoansNAdvances",
                    "OtherAssetItems",
                    "TradePayables",
                    "AdvanceFromCustomers",
                    "ShortTermBorrowings",
                    "LeaseLiabilities",
                    "LongTermBorrowings",
                    "OtherBorrowings",
                    "OtherLiabilityItems",
                    "Borrowings",
                    "OtherLiabilities",
                ]
                cf_keys = ittelson_cash_flow_columns + [
                    "IssuanceOfDebt",
                    "RepaymentOfDebt",
                    "NetCashFlow",
                ]
                cf_indirect_keys = ittelson_indirect_cf_columns + [
                    "IssuanceOfDebt",
                    "RepaymentOfDebt",
                    "NetCashFlow",
                ]

                if dfIncomeStatementQ is not None:
                    dfIncomeStatementQ_calc = apply_income_statement_fallbacks(
                        map_statement_via_dictionary(
                            dfIncomeStatementQ, normalized_is_synonym_map, is_keys
                        ),
                        ittelson_income_statement_columns,
                    )
                if dfIncomeStatementY is not None:
                    df_norm_is_y = map_statement_via_dictionary(
                        dfIncomeStatementY, normalized_is_synonym_map, is_keys
                    ).iloc[:, :-1]
                    if source == "yfinance":
                        df_norm_is_y = df_norm_is_y.iloc[:, :-1]
                    dfIncomeStatementY_calc = apply_income_statement_fallbacks(
                        df_norm_is_y, ittelson_income_statement_columns
                    )

                if dfBalanceSheetQ is not None:
                    dfBalanceSheetQ_calc = apply_balance_sheet_fallbacks(
                        map_statement_via_dictionary(
                            dfBalanceSheetQ, normalized_bs_synonym_map, bs_keys
                        ),
                        ittelson_balance_sheet_columns,
                    )
                if dfBalanceSheetY is not None:
                    dfBalanceSheetY_calc = apply_balance_sheet_fallbacks(
                        map_statement_via_dictionary(
                            dfBalanceSheetY, normalized_bs_synonym_map, bs_keys
                        ),
                        ittelson_balance_sheet_columns,
                    )

                if dfCashFlowQ is not None:
                    dfCashFlowQ_calc = apply_cash_flow_fallbacks(
                        map_statement_via_dictionary(
                            dfCashFlowQ, normalized_cf_synonym_map, cf_keys
                        ),
                        ittelson_cash_flow_columns,
                        df_is_calc=dfIncomeStatementQ_calc,
                        df_bs_calc=dfBalanceSheetQ_calc,
                    )
                if dfCashFlowY is not None:
                    dfCashFlowY_calc = apply_cash_flow_fallbacks(
                        map_statement_via_dictionary(
                            dfCashFlowY, normalized_cf_synonym_map, cf_keys
                        ),
                        ittelson_cash_flow_columns,
                        df_is_calc=dfIncomeStatementY_calc,
                        df_bs_calc=dfBalanceSheetY_calc,
                    )

                if dfCashFlowY is not None:
                    dfInDirectCashFlowY_calc = apply_indirect_cash_flow_fallbacks(
                        map_statement_via_dictionary(
                            dfCashFlowY,
                            normalized_indirect_cf_synonym_map,
                            cf_indirect_keys,
                            bucket_columns=indirect_cf_buckets,
                        ),
                        ittelson_indirect_cf_columns,
                        df_is_calc=dfIncomeStatementY_calc,
                        df_bs_calc=dfBalanceSheetY_calc,
                    )

            else:
                dfIncomeStatementQ_calc = dfIncomeStatementQ
                dfIncomeStatementY_calc = dfIncomeStatementY
                dfBalanceSheetQ_calc = dfBalanceSheetQ
                dfBalanceSheetY_calc = dfBalanceSheetY
                dfCashFlowQ_calc = dfCashFlowQ
                dfCashFlowY_calc = dfCashFlowY
                dfInDirectCashFlowY_calc = dfCashFlowY

            # Format for Database
            clean_yearly_income_statement = format_statement_for_db(
                dfIncomeStatementY_calc,
                ittelson_income_statement_columns,
                ticker,
                currency=stmt_currency,
                data_source=source,
                multiplier=stmt_multiplier,
                transpose=True,
            ).replace("None", np.nan)
            clean_yearly_balance_sheet = format_statement_for_db(
                dfBalanceSheetY_calc,
                ittelson_balance_sheet_columns,
                ticker,
                currency=stmt_currency,
                data_source=source,
                multiplier=stmt_multiplier,
                transpose=True,
            ).replace("None", np.nan)
            clean_yearly_cash_flow = format_statement_for_db(
                dfCashFlowY_calc,
                ittelson_cash_flow_columns,
                ticker,
                currency=stmt_currency,
                data_source=source,
                multiplier=stmt_multiplier,
                transpose=True,
            ).replace("None", np.nan)
            clean_yearly_indirect_cash_flow = format_statement_for_db(
                dfInDirectCashFlowY_calc,
                ittelson_indirect_cf_columns,
                ticker,
                currency=stmt_currency,
                data_source=source,
                multiplier=stmt_multiplier,
                transpose=True,
            ).replace("None", np.nan)

            if dfIncomeStatementQ is not None:
                clean_quarterly_income_statement = format_statement_for_db(
                    dfIncomeStatementQ_calc,
                    ittelson_income_statement_columns,
                    ticker,
                    currency=stmt_currency,
                    data_source=source,
                    multiplier=stmt_multiplier,
                    transpose=True,
                ).replace("None", np.nan)
            if dfBalanceSheetQ is not None:
                clean_quarterly_balance_sheet = format_statement_for_db(
                    dfBalanceSheetQ_calc,
                    ittelson_balance_sheet_columns,
                    ticker,
                    currency=stmt_currency,
                    data_source=source,
                    multiplier=stmt_multiplier,
                    transpose=True,
                ).replace("None", np.nan)
            if dfCashFlowQ is not None:
                clean_quarterly_cash_flow = format_statement_for_db(
                    dfCashFlowQ_calc,
                    ittelson_cash_flow_columns,
                    ticker,
                    currency=stmt_currency,
                    data_source=source,
                    multiplier=stmt_multiplier,
                    transpose=True,
                ).replace("None", np.nan)

            # VALIDATE & FLAG
            audit_results_Y, clean_yearly_indirect_cash_flow = (
                validate_financial_statements(
                    clean_yearly_income_statement,
                    clean_yearly_balance_sheet,
                    clean_yearly_cash_flow,
                    clean_yearly_indirect_cash_flow,
                    ticker=ticker,
                    df_cf_raw=dfCashFlowY,
                    stmt_multiplier=stmt_multiplier,
                )
            )

            direct_passed = (
                audit_results_Y["IS_IsValid"].all()
                and audit_results_Y["BS_IsValid"].all()
                and audit_results_Y["Direct_CF_Match"].all()
            )

            # Extract unique indirect leak types by checking the plugged columns
            indirect_leaks = set()
            if (clean_yearly_indirect_cash_flow["Unmapped_Operating"].abs() > 5).any():
                indirect_leaks.add("OCF Leak")
            if (clean_yearly_indirect_cash_flow["Unmapped_Investing"].abs() > 5).any():
                indirect_leaks.add("ICF Leak")
            if (clean_yearly_indirect_cash_flow["Unmapped_Financing"].abs() > 5).any():
                indirect_leaks.add("FCF Leak")
            if (
                clean_yearly_indirect_cash_flow["Unmapped_Rollforward"].abs() > 15
            ).any():
                indirect_leaks.add("Rollforward Mismatch")

            if not indirect_leaks:
                indirect_status = " Passed"
            else:
                indirect_status = " Leaks: " + ", ".join(sorted(list(indirect_leaks)))

            # Append the forensic status AND the DataFrames for the Streamlit Inspector
            batch_summary.append(
                {
                    "Ticker": ticker,
                    "Status": f" Success ({source})",
                    "Direct Validation": (
                        " Passed" if direct_passed else " Leaks Detected"
                    ),
                    "Indirect Validation": indirect_status,
                    "Rows Upserted": len(clean_yearly_income_statement),
                    "DataPayload": {
                        "IS": {
                            "Raw": (
                                dfIncomeStatementY
                                if dfIncomeStatementY is not None
                                else pd.DataFrame()
                            ),
                            "Clean": clean_yearly_income_statement,
                        },
                        "BS": {
                            "Raw": (
                                dfBalanceSheetY
                                if dfBalanceSheetY is not None
                                else pd.DataFrame()
                            ),
                            "Clean": clean_yearly_balance_sheet,
                        },
                        "CF": {
                            "Raw": (
                                dfCashFlowY
                                if dfCashFlowY is not None
                                else pd.DataFrame()
                            ),
                            "Clean": clean_yearly_cash_flow,
                        },
                        "ICF": {
                            "Raw": (
                                dfCashFlowY
                                if dfCashFlowY is not None
                                else pd.DataFrame()
                            ),
                            "Clean": clean_yearly_indirect_cash_flow,
                        },
                    },
                }
            )

            clean_yearly_income_statement["IsValid"] = (
                clean_yearly_income_statement["ReportDate"]
                .map(audit_results_Y["IS_IsValid"])
                .fillna(False)
            )
            clean_yearly_balance_sheet["IsValid"] = (
                clean_yearly_balance_sheet["ReportDate"]
                .map(audit_results_Y["BS_IsValid"])
                .fillna(False)
            )
            clean_yearly_cash_flow["IsValid"] = (
                clean_yearly_cash_flow["ReportDate"]
                .map(audit_results_Y["Direct_CF_Match"])
                .fillna(False)
            )
            clean_yearly_indirect_cash_flow["IsValid"] = (
                clean_yearly_indirect_cash_flow["ReportDate"]
                .map(audit_results_Y["Indirect_CF_Rollforward"])
                .fillna(False)
            )

            # UPSERT TO POSTGRES
            print(f"[{ticker}] Upserting Validated Data to Postgres...")

            clean_yearly_income_statement.to_sql(
                name="yearly_income_statement",
                con=engine,
                schema="public",
                if_exists="append",
                index=False,
                method=postgres_upsert,
            )
            clean_yearly_balance_sheet.to_sql(
                name="yearly_balance_sheet",
                con=engine,
                schema="public",
                if_exists="append",
                index=False,
                method=postgres_upsert,
            )
            clean_yearly_cash_flow.to_sql(
                name="yearly_cash_flow",
                con=engine,
                schema="public",
                if_exists="append",
                index=False,
                method=postgres_upsert,
            )
            clean_yearly_indirect_cash_flow.to_sql(
                name="yearly_indirect_cash_flow",
                con=engine,
                schema="public",
                if_exists="append",
                index=False,
                method=postgres_upsert,
            )

            if dfIncomeStatementQ is not None:
                clean_quarterly_income_statement.to_sql(
                    name="quarterly_income_statement",
                    con=engine,
                    schema="public",
                    if_exists="append",
                    index=False,
                    method=postgres_upsert,
                )
            if dfBalanceSheetQ is not None:
                clean_quarterly_balance_sheet.to_sql(
                    name="quarterly_balance_sheet",
                    con=engine,
                    schema="public",
                    if_exists="append",
                    index=False,
                    method=postgres_upsert,
                )
            if dfCashFlowQ is not None:
                clean_quarterly_cash_flow.to_sql(
                    name="quarterly_cash_flow",
                    con=engine,
                    schema="public",
                    if_exists="append",
                    index=False,
                    method=postgres_upsert,
                )

            print(f"[{ticker}] Processing complete. Pausing for rate limits...")
            time.sleep(12)

    finally:
        runtime.purge_memory()

        print("\n" + "=" * 40)
        print("BATCH PROCESSING COMPLETE")
        if failed_tickers:
            print(f"Failed tickers requiring review: {failed_tickers}")
        print("=" * 40)

    # Return the clean summary dictionary (now carrying the DataFrames) to the frontend
    return batch_summary
