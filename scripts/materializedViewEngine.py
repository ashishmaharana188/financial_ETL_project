import sqlalchemy
from sqlalchemy import text
from scripts.database import engine  # Ensure this points to your SQLAlchemy engine


def build_materialized_views():
    """
    Creates the high-performance Materialized Views directly inside the database.
    These views automatically calculate PCR and Cost of Carry from the raw unified matrix.
    """
    print("[*] Initializing the Database Alpha Factory...")

    # =====================================================================
    # VIEW 1: OPTIONS AGGREGATES (PCR & Volume Flow)
    # Automatically calculates Put/Call Ratios per Ticker, Date, and Expiry.
    # =====================================================================
    query_pcr_view = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_options_aggregates AS
    SELECT 
        "Ticker",
        "ReportDate",
        "ExpiryDate",
        
        -- Aggregate Open Interest
        SUM(CASE WHEN "OptionType" = 'PE' THEN "Open_Interest" ELSE 0 END) AS "Total_Put_OI",
        SUM(CASE WHEN "OptionType" = 'CE' THEN "Open_Interest" ELSE 0 END) AS "Total_Call_OI",
        
        -- OI PCR Calculation (Prevents Divide by Zero)
        CASE 
            WHEN SUM(CASE WHEN "OptionType" = 'CE' THEN "Open_Interest" ELSE 0 END) = 0 THEN NULL
            ELSE SUM(CASE WHEN "OptionType" = 'PE' THEN "Open_Interest" ELSE 0 END)::FLOAT / 
                 SUM(CASE WHEN "OptionType" = 'CE' THEN "Open_Interest" ELSE 0 END)
        END AS "OI_PCR",
        
        -- Aggregate Volume
        SUM(CASE WHEN "OptionType" = 'PE' THEN "Volume" ELSE 0 END) AS "Total_Put_Vol",
        SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END) AS "Total_Call_Vol",
        
        -- Volume PCR Calculation
        CASE 
            WHEN SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END) = 0 THEN NULL
            ELSE SUM(CASE WHEN "OptionType" = 'PE' THEN "Volume" ELSE 0 END)::FLOAT / 
                 SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END)
        END AS "Volume_PCR"
        
    FROM unified_market_master
    WHERE "InstrumentType" IN ('OPTSTK', 'OPTIDX')
    GROUP BY "Ticker", "ReportDate", "ExpiryDate";
    """

    # =====================================================================
    # VIEW 2: BASIS RISK (Cost of Carry)
    # Joins Spot Cash prices with Futures prices to calculate the premium/discount.
    # =====================================================================
    query_basis_view = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_spot_futures_basis AS
    SELECT 
        spot."Ticker",
        spot."ReportDate",
        fut."ExpiryDate",
        spot."Close" AS "Spot_Close",
        fut."Close" AS "Future_Close",
        
        -- Cost of Carry Percentage
        CASE 
            WHEN spot."Close" = 0 OR spot."Close" IS NULL THEN NULL
            ELSE ((fut."Close" - spot."Close") / spot."Close") * 100
        END AS "Cost_Of_Carry_Pct"
        
    FROM 
        (SELECT "Ticker", "ReportDate", "Close" FROM unified_market_master WHERE "InstrumentType" = 'CASH') spot
    JOIN 
        (SELECT "Ticker", "ReportDate", "ExpiryDate", "Close" FROM unified_market_master WHERE "InstrumentType" IN ('FUTSTK', 'FUTIDX')) fut
    ON 
        spot."Ticker" = fut."Ticker" AND spot."ReportDate" = fut."ReportDate";
    """

    # =====================================================================
    # CREATING INDEXES FOR MILLISECOND QUERY SPEEDS
    # =====================================================================
    indexes = [
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_opt_agg ON mv_options_aggregates ("Ticker", "ReportDate", "ExpiryDate");',
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_basis ON mv_spot_futures_basis ("Ticker", "ReportDate", "ExpiryDate");',
    ]

    try:
        with engine.connect() as conn:
            # Execute View Creation
            conn.execute(text(query_pcr_view))
            conn.execute(text(query_basis_view))

            # Execute Index Creation
            for idx in indexes:
                conn.execute(text(idx))

            conn.commit()
            print("[+] Materialized Views and Indexes successfully created.")
    except Exception as e:
        print(f"[-] Error creating views: {e}")


def refresh_alpha_factory():
    """
    This is the function your Orchestrator will call every night AFTER
    the parsers finish pushing the raw CSV data into the database.
    """
    print("[*] Refreshing Materialized Views (Calculating Math)...")
    try:
        with engine.connect() as conn:
            # CONCURRENTLY allows the DB to be read by Engine 1 while it updates in the background
            conn.execute(
                text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_options_aggregates;")
            )
            conn.execute(
                text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_spot_futures_basis;")
            )
            conn.commit()
            print("[+] Alpha Factory Refresh Complete. Data is ready for Engine 1.")
    except Exception as e:
        print(
            f"[-] Error refreshing views. (Note: CONCURRENTLY requires the unique indexes to exist). Error: {e}"
        )


if __name__ == "__main__":
    # Run this ONCE to build the architecture.
    build_materialized_views()

    # Run this EVERY NIGHT after ingestion.
    # refresh_alpha_factory()
