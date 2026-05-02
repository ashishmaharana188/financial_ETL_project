# ai_agent.py
import json
import uuid
import os
from datetime import datetime
import google.generativeai as genai
from dotenv import load_dotenv
import google.generativeai as genai
from sqlalchemy import insert
from scripts.database import engine, ai_forensic_logs

load_dotenv()

api_key = os.getenv("GEMINI_API_KEY")

if not api_key:
    raise ValueError("CRITICAL: GEMINI_API_KEY not found in .env file.")

genai.configure(api_key=api_key)


def build_forensic_prompt(
    ticker, leak_type, leak_amount, raw_data_snippet, current_mapping
):
    prompt = f"""
    You are an expert Forensic Accounting AI. An automated ETL pipeline has detected a missing accounting line item leak.
    
    TICKER: {ticker}
    STATEMENT AND LEAK TYPE: {leak_type}
    LEAK AMOUNT: {leak_amount}
    
    RAW FINANCIAL STATEMENT DATA FOR THIS PERIOD:
    {json.dumps(raw_data_snippet, indent=2)}
    
    CURRENT DICTIONARY MAPPING FOR THIS SECTION:
    {json.dumps(current_mapping, indent=2)}
    
    YOUR MISSION:
    Find the exact string key in the RAW DATA that perfectly matches the LEAK AMOUNT. 
    It is likely missing from the CURRENT DICTIONARY MAPPING.
    
    OUTPUT FORMAT:
    Respond ONLY in strict, parsable JSON matching this exact schema, with no markdown formatting or conversational text:
    {{
        "Missing_Key_Found": "The exact string found in the raw data",
        "Suggested_Category": "The dictionary category it should belong to",
        "Reasoning": "A brief, 1-sentence explanation"
    }}
    """
    return prompt


def trigger_ai_forensic_audit(
    ticker, leak_type, leak_amount, raw_data_snippet, current_mapping
):
    try:
        # 1. Build and Fire the Prompt
        prompt = build_forensic_prompt(
            ticker, leak_type, leak_amount, raw_data_snippet, current_mapping
        )
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)

        # 2. Parse the JSON (cleaning any potential markdown blocks the AI sneaks in)
        clean_json = response.text.replace("```json", "").replace("```", "").strip()
        ai_result = json.loads(clean_json)

        # 3. Save the Ticket to PostgreSQL
        ticket_id = f"{ticker}-{leak_type}-{uuid.uuid4().hex[:6]}"

        with engine.begin() as conn:
            stmt = insert(ai_forensic_logs).values(
                TicketID=ticket_id,
                Timestamp=datetime.now().date(),
                Ticker=ticker,
                LeakType=leak_type,
                LeakAmount=leak_amount,
                MissingKeyFound=ai_result.get("Missing_Key_Found", "UNKNOWN"),
                SuggestedCategory=ai_result.get("Suggested_Category", "UNKNOWN"),
                Reasoning=ai_result.get("Reasoning", "No reasoning provided"),
                Status="PENDING",
            )
            conn.execute(stmt)

        print(
            f"   [AI AGENT] Logged ticket {ticket_id} for {ticker} ({leak_type}). Pending human review."
        )

    except Exception as e:
        print(f"   [AI AGENT] Failed to generate or save forensic audit: {e}")
