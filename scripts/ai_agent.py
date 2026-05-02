# ai_agent.py
import json
import uuid
import os
from datetime import datetime
from google import genai
from google.genai import types
from dotenv import load_dotenv
from sqlalchemy import insert
from scripts.database import engine, ai_forensic_logs

load_dotenv()


client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


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

        safe_leak_amount = float(leak_amount)
        # 1. Build and Fire the Prompt
        prompt = build_forensic_prompt(
            ticker,
            leak_type,
            safe_leak_amount,
            raw_data_snippet,
            current_mapping,
        )
        response = client.models.generate_content(
            model="gemini-3.1-flash-lite-preview",
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json"),
        )

        # raw_text = response.text
        # clean_json = raw_text.strip()
        # if clean_json.startswith("```"):

        #    lines = clean_json.splitlines()
        #    if lines[0].startswith("```"):
        #        lines = lines[1:]
        #    if lines[-1].startswith("```"):
        #        lines = lines[:-1]
        #    clean_json = "\n".join(lines).strip()

        ai_result = json.loads(response.text)

        # Save the Ticket to PostgreSQL
        ticket_id = f"{ticker}-{leak_type}-{uuid.uuid4().hex[:6]}"

        with engine.begin() as conn:
            stmt = insert(ai_forensic_logs).values(
                TicketID=ticket_id,
                Timestamp=datetime.now().date(),
                Ticker=ticker,
                LeakType=leak_type,
                LeakAmount=safe_leak_amount,
                MissingKeyFound=ai_result.get("Missing_Key_Found", "UNKNOWN"),
                SuggestedCategory=ai_result.get("Suggested_Category", "UNKNOWN"),
                Reasoning=ai_result.get("Reasoning", "No reasoning provided"),
                Status="PENDING",
            )
            conn.execute(stmt)

        print(f"   [AI AGENT] Logged ticket {ticket_id} for {ticker} ({leak_type}).")

    except json.JSONDecodeError as je:
        print(
            f"   [AI AGENT] JSON Parse Error: {je}. Raw output was: {response.text[:100]}..."
        )
    except Exception as e:
        print(f"   [AI AGENT] Failed to generate or save forensic audit: {e}")
