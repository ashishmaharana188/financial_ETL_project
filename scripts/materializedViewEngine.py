import sqlalchemy
from sqlalchemy import text
import argparse
from scripts.database import engine  # Ensure this points to your SQLAlchemy engine


def build_materialized_views():
    """
    Clears old structures and creates high-performance Materialized Views.
    """
    print("[*] Initializing the Database Alpha Factory...")

    teardown_queries = [
        "DROP MATERIALIZED VIEW IF EXISTS unified_market_matrix CASCADE;",
        "DROP MATERIALIZED VIEW IF EXISTS mv_options_aggregates CASCADE;",
        "DROP MATERIALIZED VIEW IF EXISTS mv_spot_futures_basis CASCADE;",
        "DROP MATERIALIZED VIEW IF EXISTS mv_institutional_flow CASCADE;",
    ]

    query_pcr_view = """
    CREATE MATERIALIZED VIEW mv_options_aggregates AS
    SELECT 
        "Ticker",
        "ReportDate",
        "ExpiryDate",
        
        SUM(CASE WHEN "OptionType" = 'PE' THEN "Open_Interest" ELSE 0 END) AS "Total_Put_OI",
        SUM(CASE WHEN "OptionType" = 'CE' THEN "Open_Interest" ELSE 0 END) AS "Total_Call_OI",
        
        CASE 
            WHEN SUM(CASE WHEN "OptionType" = 'CE' THEN "Open_Interest" ELSE 0 END) = 0 THEN NULL
            ELSE SUM(CASE WHEN "OptionType" = 'PE' THEN "Open_Interest" ELSE 0 END)::FLOAT / 
                 SUM(CASE WHEN "OptionType" = 'CE' THEN "Open_Interest" ELSE 0 END)
        END AS "OI_PCR",
        
        SUM(CASE WHEN "OptionType" = 'PE' THEN "Volume" ELSE 0 END) AS "Total_Put_Volume",
        SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END) AS "Total_Call_Volume",
        
        CASE 
            WHEN SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END) = 0 THEN NULL
            ELSE SUM(CASE WHEN "OptionType" = 'PE' THEN "Volume" ELSE 0 END)::FLOAT / 
                 SUM(CASE WHEN "OptionType" = 'CE' THEN "Volume" ELSE 0 END)
        END AS "Volume_PCR"
        
    FROM unified_market_master
    WHERE "InstrumentType" IN ('OPTSTK', 'OPTIDX', 'STO', 'IDO')
    GROUP BY "Ticker", "ReportDate", "ExpiryDate";
    """

    query_basis_view = """
    CREATE MATERIALIZED VIEW mv_spot_futures_basis AS
    WITH CashData AS (
        SELECT "Ticker", "ReportDate", "Close" AS "Spot_Price"
        FROM unified_market_master
        WHERE "InstrumentType" = 'CASH'
        AND "Exchange_Series" = 'EQ'
    ),
    FuturesData AS (
        SELECT "Ticker", "ReportDate", "ExpiryDate", "Close" AS "Futures_Price", "Open_Interest"
        FROM unified_market_master
        WHERE "InstrumentType" IN ('FUTSTK', 'FUTIDX', 'STF', 'IDF', 'FUTIVX')
        AND "OptionType" = 'XX'
    )
    SELECT 
        f."Ticker",
        f."ReportDate",
        f."ExpiryDate",
        c."Spot_Price",
        f."Futures_Price",
        f."Open_Interest",
        (f."Futures_Price" - c."Spot_Price") AS "Absolute_Basis",
        
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
    CREATE MATERIALIZED VIEW mv_institutional_flow AS
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

    query_unified_matrix_view = """
        CREATE MATERIALIZED VIEW unified_market_matrix AS
        WITH CashBase AS (
            SELECT 
                "Ticker" AS ticker,
                "ReportDate"::DATE AS date,
                "Close" AS close,
                "Volume" AS volume,
                "Delivery_Percentage" AS delivery_percentage,
                
                -- PROXY: Daily Volatility (High-Low Spread)
                CASE 
                    WHEN "Close" = 0 OR "Close" IS NULL THEN 0 
                    ELSE (("High"::FLOAT - "Low"::FLOAT) / "Close"::FLOAT) * 100 
                END AS daily_hl_spread,
                
                -- PROXY: Daily Order Book Pressure (VWAP Deviation)
                -- FIX: Multiply Turnover by 100,000 to correct NSE Lakh scaling
                CASE 
                    WHEN "Volume" = 0 OR "Volume" IS NULL OR "Close" = 0 THEN 0 
                    ELSE (( (("Turnover"::FLOAT * 100000) / "Volume"::FLOAT) - "Close"::FLOAT ) / "Close"::FLOAT) * 100 
                END AS daily_vwap_dev
                
            FROM unified_market_master
            WHERE "InstrumentType" = 'CASH'
            AND "Exchange_Series" = 'EQ'
        ),
        DailyPCR AS (
            SELECT 
                "Ticker" AS ticker,
                "ReportDate"::DATE AS date,
                CASE 
                    WHEN SUM("Total_Call_OI") = 0 THEN NULL
                    ELSE SUM("Total_Put_OI")::FLOAT / SUM("Total_Call_OI")
                END AS oi_pcr
            FROM mv_options_aggregates
            GROUP BY "Ticker", "ReportDate"
        ),
        DailyPCRWithDelta AS (
            SELECT 
                ticker,
                date,
                oi_pcr,
                oi_pcr - LAG(oi_pcr) OVER (PARTITION BY ticker ORDER BY date) AS delta_oi_pcr
            FROM DailyPCR
        ),
        NearMonthBasis AS (
            SELECT DISTINCT ON ("Ticker", "ReportDate")
                "Ticker" AS ticker,
                "ReportDate"::DATE AS date,
                "Absolute_Basis" AS futures_basis
            FROM mv_spot_futures_basis
            ORDER BY "Ticker", "ReportDate", "Open_Interest" DESC
        ),
        BlockBulkEvents AS (
            SELECT 
                "Ticker" AS ticker,
                "ReportDate"::DATE AS date,
                SUM(CASE WHEN "TransactionType" = 'BUY' THEN "Quantity" ELSE -"Quantity" END) AS net_block_volume,
                AVG("TradePrice") AS avg_block_price
            FROM trade_events_ledger
            GROUP BY "Ticker", "ReportDate"
        )
        SELECT 
            c.ticker,
            c.date,
            c.close,
            c.volume,
            c.delivery_percentage,
            c.daily_hl_spread,
            c.daily_vwap_dev,
            
            p.oi_pcr,               
            p.delta_oi_pcr,   
            b.futures_basis,
            
            COALESCE(e.net_block_volume, 0) AS net_block_volume,
            
            -- PROXY: Block Premium/Discount relative to EOD Spot Close
            CASE 
                WHEN e.avg_block_price IS NULL OR c.close = 0 THEN 0
                ELSE ((e.avg_block_price - c.close) / c.close) * 100
            END AS avg_block_premium
            
        FROM CashBase c
        LEFT JOIN DailyPCRWithDelta p ON c.ticker = p.ticker AND c.date = p.date
        LEFT JOIN NearMonthBasis b ON c.ticker = b.ticker AND c.date = b.date
        LEFT JOIN BlockBulkEvents e ON c.ticker = e.ticker AND c.date = e.date;
        """

    indexes = [
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_opt_pcr ON mv_options_aggregates ("Ticker", "ReportDate", "ExpiryDate");',
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_basis ON mv_spot_futures_basis ("Ticker", "ReportDate", "ExpiryDate");',
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_inst_flow ON mv_institutional_flow ("ReportDate", "ClientType");',
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_unified_matrix ON unified_market_matrix (ticker, date);",
        'CREATE INDEX IF NOT EXISTS idx_mv_opt_date ON mv_options_aggregates ("ReportDate");',
        'CREATE INDEX IF NOT EXISTS idx_mv_basis_date ON mv_spot_futures_basis ("ReportDate");',
        'CREATE INDEX IF NOT EXISTS idx_mv_inst_date ON mv_institutional_flow ("ReportDate");',
        "CREATE INDEX IF NOT EXISTS idx_mv_matrix_date ON unified_market_matrix (date);",
    ]

    try:
        with engine.connect() as conn:
            # Execute clear instructions
            for drop_statement in teardown_queries:
                conn.execute(text(drop_statement))
            conn.commit()
            print("[+] Old materialized cache structures successfully dropped.")

            # Compile Fresh Base Templates
            conn.execute(text(query_pcr_view))
            conn.execute(text(query_basis_view))
            conn.execute(text(query_inst_flow_view))
            conn.commit()

            # Compile Interconnected Analytical Matrix View
            conn.execute(text(query_unified_matrix_view))
            conn.commit()

            # Re-bind index references
            for idx in indexes:
                conn.execute(text(idx))
            conn.commit()

            print("[+] Materialized Views and Indexes successfully created.")
    except Exception as e:
        print(f"[-] Error creating views: {e}")


def refresh_alpha_factory():
    """
    Performs an atomic data refresh on existing schema allocations.
    """
    print("[*] Refreshing Materialized Views (Truncate & Refill)...")
    try:
        with engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_options_aggregates;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_spot_futures_basis;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_institutional_flow;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW unified_market_matrix;"))
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
