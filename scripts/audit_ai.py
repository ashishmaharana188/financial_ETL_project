import json
import sys
import os

# Ensure the script can find your local modules if run from the root directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import ONLY what is necessary from your existing structured pipeline
from scripts.statementScrape import fetch_screener_data
from scripts.ai_agent import trigger_semantic_router


def run_reconciliation_sandbox(
    ticker, target_year, raw_total_key="Cash from Operating Activities"
):
    """
    Acts as an isolated validation environment. Fetches raw data, triggers
    the AI for classification, and tests the AI's output for leaks.
    """
    print(f"\n{'='*50}")
    print(f"🔍 ISOLATED AI VALIDATION: {ticker} ({target_year})")
    print(f"{'='*50}")

    # 1. Provide the ETL Data (Fetch Raw Source)
    print("-> Fetching raw API data...")
    raw_data = fetch_screener_data(ticker)
    year_data = raw_data.get(target_year, {})

    if not year_data:
        print(f"[!] Error: Could not extract raw JSON for {target_year}.")
        return

    unmapped_keys = list(year_data.keys())

    # 2. Use AI Agent STRICTLY for Generation
    print("-> Triggering Semantic Router...")
    ai_result_dict = trigger_semantic_router(ticker, unmapped_keys)

    if not ai_result_dict:
        print("[!] Error: AI returned empty payload. Cannot proceed.")
        return

    # 3. Reconcile and Validate
    print("\n-> Running Mathematical Reconciliation on AI Output:")
    raw_calculated_sum = 0.0

    # Safely extract the array of keys the AI classified as Operating Cash Flow
    ocf_keys = ai_result_dict.get("OperatingCashFlow", [])

    for raw_key in ocf_keys:
        val = year_data.get(raw_key, 0.0)
        if val is None or val == "":
            val = 0.0
        val = float(val)

        print(f"   [+] {raw_key}: {val}")
        raw_calculated_sum += val

    raw_reported_total = float(year_data.get(raw_total_key, 0.0))
    raw_leak = raw_reported_total - raw_calculated_sum

    # 4. Output the Verdict
    print("-" * 50)
    print(f"Calculated Sum (from AI keys): {raw_calculated_sum:,.2f}")
    print(f"Reported Total (from Source):  {raw_reported_total:,.2f}")
    print(f"GAP (The Leak):                {raw_leak:,.2f}")
    print("-" * 50)

    if abs(raw_leak) < 100:
        print(
            "VERDICT: AI extraction is perfect. The -405B leak is in the ETL scaling logic."
        )
    elif abs(raw_leak) > 100000000:
        print(
            "VERDICT: AI selected a massive incorrect key (e.g., Market Cap). Check the mapped list above."
        )
    else:
        print(
            f"VERDICT: True API Leak detected ({raw_leak:,.2f}). Screener dropped rows."
        )
    print(f"{'='*50}\n")


if __name__ == "__main__":
    # Run the isolated test for the known failure point
    run_reconciliation_sandbox(ticker="TATAPOWER.NS", target_year="2025-03-31")
