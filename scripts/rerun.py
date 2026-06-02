from database import engine


def purge_corrupted_mcx():
    # Identify the exact phantom rows using standard MCX instrument types
    # Ensure standard NSE instruments ('CASH', 'EQ', etc.) are protected
    query = """
        DELETE FROM unified_market_master 
        WHERE "ReportDate" = '2026-01-06' 
        AND "InstrumentType" IN ('FUTCOM', 'OPTFUT', 'OPTCOM');
    """

    try:
        # If using your DuckDB proxy, execute directly
        engine.execute(query)
        print("[SUCCESS] Corrupted January 6th MCX rows have been purged.")
    except Exception as e:
        print(f"[-] Deletion Failed: {e}")


if __name__ == "__main__":
    purge_corrupted_mcx()
