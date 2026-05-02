import json
import os
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


def build_semantic_prompt(ticker, unmapped_keys):
    prompt = f"""
    You are an Expert Forensic Accounting AI acting as a Semantic Router.
    
    TICKER: {ticker}
    
    UNMAPPED RAW KEYS:
    {json.dumps(unmapped_keys, indent=2)}
    
    YOUR MISSION:
    Do not perform any math. Your only job is to semantically classify every single key provided in the UNMAPPED RAW KEYS list into its correct Cash Flow accounting boundary.
    
    ACCOUNTING BOUNDARIES (Use these exact PascalCase names as your JSON keys):
    - OperatingCashFlow: (Net Income adjustments, Non-Cash Items like Depreciation/Amortization, Working Capital changes).
    - InvestingCashFlow: (CapEx, Purchase/Sale of Investments, Interest/Dividends Received).
    - FinancingCashFlow: (Debt issuance/repayment, Equity issuance/buybacks, Dividends Paid).
    - Anomaly: (Keys that clearly do not belong on a cash flow statement, or obvious data errors).
    
    CRITICAL RULE: 
    You MUST preserve the exact spelling, spacing, and casing of the original raw keys. Do not format the raw keys themselves, or the downstream Python pipeline will crash.
    
    OUTPUT FORMAT:
    Respond ONLY in strict, parsable JSON matching this exact schema. Every key from the input must be placed into one of these four arrays:
    {{
        "OperatingCashFlow": ["ExactRawKey1", "ExactRawKey2"],
        "InvestingCashFlow": ["ExactRawKey3"],
        "FinancingCashFlow": [],
        "Anomaly": ["ExactRawKey4"]
    }}
    """
    return prompt


def trigger_semantic_router(ticker, unmapped_keys):
    """
    Takes the unmapped keys, asks Gemini to sort them, and returns the JSON dictionary.
    No math, no database logging. Just pure semantic classification.
    """
    try:
        # 1. Build and Fire the Prompt
        prompt = build_semantic_prompt(ticker, unmapped_keys)

        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json"),
        )

        # 2. Parse and return the JSON directly back to the pipeline
        ai_result = json.loads(response.text)
        print(
            f"   [AI AGENT] Successfully routed {len(unmapped_keys)} keys for {ticker}."
        )

        return ai_result

    except json.JSONDecodeError as je:
        print(
            f"   [AI AGENT] JSON Parse Error: {je}. Raw output was: {response.text[:100]}..."
        )
        return None
    except Exception as e:
        print(f"   [AI AGENT] Semantic Routing failed: {e}")
        return None
