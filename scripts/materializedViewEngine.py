import sqlalchemy
from sqlalchemy import text
import argparse
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
        SUM(CASE WHEN "OptionType" = 'PE' THEN "Volume" ELSE 0 END) AS "Total_Put_Volume",
        SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END) AS "Total_Call_Volume",
        
        -- Volume PCR Calculation
        CASE 
            WHEN SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END) = 0 THEN NULL
            ELSE SUM(CASE WHEN "OptionType" = 'PE' THEN "Volume" ELSE 0 END)::FLOAT / 
                 SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END)
        END AS "Volume_PCR"
        
    FROM unified_market_master
    WHERE "InstrumentType" LIKE 'OPT%'  -- Filter strictly for Options
    GROUP BY "Ticker", "ReportDate", "ExpiryDate";
    """

    # =====================================================================
    # VIEW 2: SPOT-FUTURES BASIS (Cost of Carry)
    # Measures the premium/discount of Futures compared to the Cash Equity
    # =====================================================================
    query_basis_view = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_spot_futures_basis AS
    WITH CashData AS (
        SELECT "Ticker", "ReportDate", "Close" AS "Spot_Price"
        FROM unified_market_master
        WHERE "InstrumentType" = 'CASH'
    ),
    FuturesData AS (
        SELECT "Ticker", "ReportDate", "ExpiryDate", "Close" AS "Futures_Price", "Open_Interest"
        FROM unified_market_master
        WHERE "InstrumentType" LIKE 'FUT%'
    )
    SELECT 
        f."Ticker",
        f."ReportDate",
        f."ExpiryDate",
        c."Spot_Price",
        f."Futures_Price",
        f."Open_Interest",
        (f."Futures_Price" - c."Spot_Price") AS "Absolute_Basis",
        
        -- Percentage Basis (Annualized Cost of Carry can be derived from this)
        CASE 
            WHEN c."Spot_Price" = 0 THEN NULL
            ELSE ((f."Futures_Price" - c."Spot_Price") / c."Spot_Price") * 100 
        END AS "Basis_Percentage"
        
    FROM FuturesData f
    JOIN CashData c 
      ON f."Ticker" = c."Ticker" 
     AND f."ReportDate" = c."ReportDate";
    """

    query_inst_flow_view = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_institutional_flow AS
    WITH NetPositions AS (
        SELECT 
            "ReportDate", 
            "ClientType",
            ("Future_Index_Long" - "Future_Index_Short") AS "Future_Index_Net",
            ("Option_Index_Call_Long" - "Option_Index_Call_Short") AS "Option_Index_Call_Net",
            ("Option_Index_Put_Long" - "Option_Index_Put_Short") AS "Option_Index_Put_Net",
            ("Future_Stock_Long" - "Future_Stock_Short") AS "Future_Stock_Net",
            ("Option_Stock_Call_Long" - "Option_Stock_Call_Short") AS "Option_Stock_Call_Net",
            ("Option_Stock_Put_Long" - "Option_Stock_Put_Short") AS "Option_Stock_Put_Net"
        FROM institutional_ledger
    )
    SELECT 
        "ReportDate",
        "ClientType",
        
        "Future_Index_Net",
        "Future_Index_Net" - LAG("Future_Index_Net") OVER (PARTITION BY "ClientType" ORDER BY "ReportDate") AS "Future_Index_Net_Change",
        
        "Option_Index_Call_Net",
        "Option_Index_Call_Net" - LAG("Option_Index_Call_Net") OVER (PARTITION BY "ClientType" ORDER BY "ReportDate") AS "Option_Index_Call_Net_Change",
        
        "Option_Index_Put_Net",
        "Option_Index_Put_Net" - LAG("Option_Index_Put_Net") OVER (PARTITION BY "ClientType" ORDER BY "ReportDate") AS "Option_Index_Put_Net_Change",
        
        "Future_Stock_Net",
        "Future_Stock_Net" - LAG("Future_Stock_Net") OVER (PARTITION BY "ClientType" ORDER BY "ReportDate") AS "Future_Stock_Net_Change",
        
        "Option_Stock_Call_Net",
        "Option_Stock_Call_Net" - LAG("Option_Stock_Call_Net") OVER (PARTITION BY "ClientType" ORDER BY "ReportDate") AS "Option_Stock_Call_Net_Change",
        
        "Option_Stock_Put_Net",
        "Option_Stock_Put_Net" - LAG("Option_Stock_Put_Net") OVER (PARTITION BY "ClientType" ORDER BY "ReportDate") AS "Option_Stock_Put_Net_Change"
        
    FROM NetPositions;
    """

    # --- INDEXES FOR MILLISECOND QUERY SPEEDS ---
    indexes = [
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_opt_pcr ON mv_options_aggregates ("Ticker", "ReportDate", "ExpiryDate");',
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_basis ON mv_spot_futures_basis ("Ticker", "ReportDate", "ExpiryDate");',
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_inst_flow ON mv_institutional_flow ("ReportDate", "ClientType");',
        'CREATE INDEX IF NOT EXISTS idx_mv_opt_date ON mv_options_aggregates ("ReportDate");',
        'CREATE INDEX IF NOT EXISTS idx_mv_basis_date ON mv_spot_futures_basis ("ReportDate");',
        'CREATE INDEX IF NOT EXISTS idx_mv_inst_date ON mv_institutional_flow ("ReportDate");',
    ]

    try:
        with engine.connect() as conn:
            # Execute View Creation
            conn.execute(text(query_pcr_view))
            conn.execute(text(query_basis_view))
            conn.execute(text(query_inst_flow_view))

            # Execute Index Creation
            for idx in indexes:
                conn.execute(text(idx))

            conn.commit()
            print("[+] Materialized Views and Indexes successfully created.")
    except Exception as e:
        print(f"[-] Error creating views: {e}")


def refresh_alpha_factory():
    """
    Performs a standard refresh: Truncates old cached data and completely
    refills the views using the latest tables.
    """
    print("[*] Refreshing Materialized Views (Truncate & Refill)...")
    try:
        with engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_options_aggregates;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_spot_futures_basis;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_institutional_flow;"))
            conn.commit()
            print("[+] Alpha Factory Refresh Complete. Data is ready for Engine 1.")
    except Exception as e:
        print(f"[-] Error refreshing views: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="Build the initial views")
    parser.add_argument("--refresh", action="store_true", help="Refresh existing views")
    args = parser.parse_args()

    if args.build:
        build_materialized_views()
    elif args.refresh:
        refresh_alpha_factory()
    else:
        print("Please specify --build or --refresh")
