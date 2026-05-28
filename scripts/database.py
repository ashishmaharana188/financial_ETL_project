import duckdb
import os
import sys
from datetime import datetime, timedelta

DB_PATH = "market_data.duckdb"


def get_db_connection(read_only=True):
    con = duckdb.connect(database=DB_PATH, read_only=read_only)
    con.execute("INSTALL json;")
    con.execute("LOAD json;")
    return con


class DuckDBEngineProxy:
    """Thread-safe proxy that automatically assigns read/write locks based on the script."""

    def __init__(self):
        self.db_path = DB_PATH
        main_script = os.path.basename(sys.argv[0])
        # Dashboard is strictly read-only so it never locks the DB
        self.default_read_only = "dashboard.py" in main_script or "UI" in main_script

    def execute(self, query_string, params=None):
        con = get_db_connection(read_only=self.default_read_only)
        try:
            if params:
                res = con.execute(query_string, params).df()
            else:
                res = con.execute(query_string).df()
            return DuckDBResultContainer(res)
        finally:
            con.close()

    def register(self, view_name, df):
        # Temporarily upgrade to write-mode for ingestion
        self.default_read_only = False
        self._active_write_con = duckdb.connect(database=self.db_path, read_only=False)
        self._active_write_con.execute("INSTALL json; LOAD json;")
        self._active_write_con.register(view_name, df)

    def unregister(self, view_name):
        if hasattr(self, "_active_write_con"):
            self._active_write_con.unregister(view_name)
            self._active_write_con.close()
            del self._active_write_con
        # Revert back to default state
        self.default_read_only = "dashboard.py" in os.path.basename(sys.argv[0])


class DuckDBResultContainer:
    def __init__(self, dataframe):
        self._df = dataframe

    def df(self):
        return self._df

    def fetchone(self):
        return tuple(self._df.iloc[0]) if not self._df.empty else None

    def fetchall(self):
        return [tuple(x) for x in self._df.to_numpy()]


# The global engine used by all your scripts
engine = DuckDBEngineProxy()


def initialize_database():
    print("[*] Initializing Native DuckDB Schema...")

    # 1. Market Metadata
    engine.execute("""
        CREATE TABLE IF NOT EXISTS market_metadata (
            "Ticker" VARCHAR PRIMARY KEY,
            "IndicatorName" VARCHAR,
            "TargetTable" VARCHAR NOT NULL,
            "Sector" VARCHAR,
            "Industry" VARCHAR,
            "AssetClass" VARCHAR NOT NULL,
            "Exchange" VARCHAR,
            "IsActive" BOOLEAN DEFAULT true,
            "valid_data_since" DATE,
            "Description" VARCHAR
        );
    """)

    # 2. Macro Daily Ledger
    engine.execute("""
        CREATE TABLE IF NOT EXISTS macro_daily_ledger (
            "IndicatorName" VARCHAR,
            "ReportDate" DATE,
            "Open" DOUBLE,
            "High" DOUBLE,
            "Low" DOUBLE,
            "Close_Value" DOUBLE NOT NULL,
            "Volume" BIGINT,
            PRIMARY KEY ("IndicatorName", "ReportDate")
        );
    """)

    # 3. Macro Intraday Ledger
    engine.execute("""
        CREATE TABLE IF NOT EXISTS macro_intraday_ledger (
            "IndicatorName" VARCHAR,
            "ReportDate" TIMESTAMP,
            "Timeframe" VARCHAR,
            "Open" DOUBLE,
            "High" DOUBLE,
            "Low" DOUBLE,
            "Close_Value" DOUBLE NOT NULL,
            "Volume" BIGINT,
            PRIMARY KEY ("IndicatorName", "ReportDate", "Timeframe")
        );
    """)

    # 4. Prediction Ledger
    engine.execute("""
        CREATE TABLE IF NOT EXISTS prediction_ledger (
            "engine_name" VARCHAR,
            "ticker" VARCHAR,
            "asof_date" DATE,
            "horizon" VARCHAR,
            "signal" VARCHAR,
            "score" DOUBLE,
            "confidence" DOUBLE,
            "veto_flag" BOOLEAN DEFAULT false,
            "penalty" DOUBLE DEFAULT 0.0,
            "target_metric" VARCHAR,
            "reason_json" JSON,
            "feature_json" JSON,
            "data_quality_score" DOUBLE,
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY ("engine_name", "ticker", "asof_date", "horizon")
        );
    """)

    # 5. Global Assets Daily
    engine.execute("""
        CREATE TABLE IF NOT EXISTS global_assets_daily (
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "AssetClass" VARCHAR,
            "Open" DOUBLE,
            "High" DOUBLE,
            "Low" DOUBLE,
            "Close" DOUBLE NOT NULL,
            "Volume" BIGINT,
            PRIMARY KEY ("Ticker", "ReportDate")
        );
    """)

    # 6. Global Assets Intraday
    engine.execute("""
        CREATE TABLE IF NOT EXISTS global_assets_intraday (
            "Ticker" VARCHAR,
            "ReportDate" TIMESTAMP,
            "Timeframe" VARCHAR,
            "Open" DOUBLE,
            "High" DOUBLE,
            "Low" DOUBLE,
            "Close" DOUBLE NOT NULL,
            "Volume" BIGINT,
            PRIMARY KEY ("Ticker", "ReportDate", "Timeframe")
        );
    """)

    # 7. Unified Market Master
    engine.execute("""
        CREATE TABLE IF NOT EXISTS unified_market_master (
            "Ticker" VARCHAR,
            "ReportDate" TIMESTAMP,
            "InstrumentType" VARCHAR,
            "ExpiryDate" DATE,
            "StrikePrice" DOUBLE,
            "Open" DOUBLE,
            "High" DOUBLE,
            "Low" DOUBLE,
            "Close" DOUBLE,
            "Volume" BIGINT,
            "Exchange_Series" VARCHAR,
            "Turnover" DOUBLE,
            "No_Of_Trades" BIGINT,
            "Delivery_Qty" BIGINT,
            "Delivery_Percentage" DOUBLE,
            "Short_Volume" BIGINT,
            "OptionType" VARCHAR,
            "Open_Interest" BIGINT,
            "Change_In_OI" BIGINT,
            "Settlement_Price" DOUBLE,
            "Underlying_Price" DOUBLE,
            PRIMARY KEY ("Ticker", "ReportDate", "InstrumentType", "ExpiryDate", "StrikePrice")
        );
    """)

    # 8. Institutional Ledger
    engine.execute("""
        CREATE TABLE IF NOT EXISTS institutional_ledger (
            "ReportDate" TIMESTAMP,
            "ClientType" VARCHAR,
            "Cash_Buy_Value" DOUBLE,
            "Cash_Sell_Value" DOUBLE,
            "Cash_Net_Value" DOUBLE,
            "Nifty_Close" DOUBLE,
            "Future_Index_Long" BIGINT,
            "Future_Index_Short" BIGINT,
            "Future_Stock_Long" BIGINT,
            "Future_Stock_Short" BIGINT,
            "Option_Index_Call_Long" BIGINT,
            "Option_Index_Put_Long" BIGINT,
            "Option_Index_Call_Short" BIGINT,
            "Option_Index_Put_Short" BIGINT,
            "Option_Stock_Call_Long" BIGINT,
            "Option_Stock_Put_Long" BIGINT,
            "Option_Stock_Call_Short" BIGINT,
            "Option_Stock_Put_Short" BIGINT,
            "Total_Long_Contracts" BIGINT,
            "Total_Short_Contracts" BIGINT,
            PRIMARY KEY ("ReportDate", "ClientType")
        );
    """)

    # 9. Trade Events Ledger
    # Creates an auto-incrementing sequence for EventID mapping to previous SQLAlchemy autoincrement
    engine.execute("CREATE SEQUENCE IF NOT EXISTS seq_trade_event_id;")
    engine.execute("""
        CREATE TABLE IF NOT EXISTS trade_events_ledger (
            "EventID" BIGINT DEFAULT nextval('seq_trade_event_id') PRIMARY KEY,
            "ReportDate" TIMESTAMP NOT NULL,
            "Ticker" VARCHAR NOT NULL,
            "EventType" VARCHAR NOT NULL,
            "SecurityName" VARCHAR,
            "ClientName" VARCHAR NOT NULL,
            "TransactionType" VARCHAR NOT NULL,
            "Quantity" BIGINT NOT NULL,
            "TradePrice" DOUBLE NOT NULL,
            "Remarks" VARCHAR,
            UNIQUE("ReportDate", "Ticker", "ClientName", "TransactionType", "Quantity", "TradePrice")
        );
    """)

    # 10. AI Forensic Logs
    engine.execute("""
        CREATE TABLE IF NOT EXISTS ai_forensic_logs (
            "TicketID" VARCHAR PRIMARY KEY,
            "Timestamp" DATE,
            "Ticker" VARCHAR,
            "LeakType" VARCHAR,
            "LeakAmount" DOUBLE,
            "MissingKeyFound" VARCHAR,
            "SuggestedCategory" VARCHAR,
            "Reasoning" VARCHAR,
            "Status" VARCHAR DEFAULT 'PENDING'
        );
    """)

    # 11. Raw Financials (JSONB -> JSON)
    engine.execute("""
        CREATE TABLE IF NOT EXISTS raw_financials (
            "DataSource" VARCHAR,
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "StatementType" VARCHAR,
            "RawData" JSON,
            PRIMARY KEY ("Ticker", "ReportDate", "StatementType")
        );
    """)

    # 12. Quarterly Income Statement
    engine.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_income_statement (
            "DataSource" VARCHAR,
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "Currency" VARCHAR,
            "TotalRevenue" DOUBLE,
            "CostOfRevenue" DOUBLE,
            "GrossProfit" DOUBLE,
            "OperatingExpense" DOUBLE,
            "OperatingIncome" DOUBLE,
            "NetInterestIncome" DOUBLE,
            "TaxProvision" DOUBLE,
            "NetIncome" DOUBLE,
            PRIMARY KEY ("Ticker", "ReportDate")
        );
    """)

    # 13. Yearly Income Statement
    engine.execute("""
        CREATE TABLE IF NOT EXISTS yearly_income_statement (
            "DataSource" VARCHAR,
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "Currency" VARCHAR,
            "IsValid" BOOLEAN,
            "TotalRevenue" DOUBLE,
            "CostOfRevenue" DOUBLE,
            "GrossProfit" DOUBLE,
            "OperatingExpense" DOUBLE,
            "OperatingIncome" DOUBLE,
            "NetInterestIncome" DOUBLE,
            "TaxProvision" DOUBLE,
            "NetIncome" DOUBLE,
            PRIMARY KEY ("Ticker", "ReportDate")
        );
    """)

    # 14. Quarterly Balance Sheet
    engine.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_balance_sheet (
            "DataSource" VARCHAR,
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "Currency" VARCHAR,
            "CashCashEquivalentsAndShortTermInvestments" DOUBLE,
            "Receivables" DOUBLE,
            "Inventory" DOUBLE,
            "CurrentAssets" DOUBLE,
            "TotalNonCurrentAssets" DOUBLE,
            "GrossPPE" DOUBLE,
            "AccumulatedDepreciation" DOUBLE,
            "NetPPE" DOUBLE,
            "TotalAssets" DOUBLE,
            "PayablesAndAccruedExpenses" DOUBLE,
            "CurrentDebtAndCapitalLeaseObligation" DOUBLE,
            "TotalTaxPayable" DOUBLE,
            "CurrentLiabilities" DOUBLE,
            "LongTermDebtAndCapitalLeaseObligation" DOUBLE,
            "TotalLiabilitiesNetMinorityInterest" DOUBLE,
            "CapitalStock" DOUBLE,
            "RetainedEarnings" DOUBLE,
            "StockholdersEquity" DOUBLE,
            PRIMARY KEY ("Ticker", "ReportDate")
        );
    """)

    # 15. Yearly Balance Sheet
    engine.execute("""
        CREATE TABLE IF NOT EXISTS yearly_balance_sheet (
            "DataSource" VARCHAR,
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "Currency" VARCHAR,
            "IsValid" BOOLEAN,
            "CashCashEquivalentsAndShortTermInvestments" DOUBLE,
            "Receivables" DOUBLE,
            "Inventory" DOUBLE,
            "CurrentAssets" DOUBLE,
            "TotalNonCurrentAssets" DOUBLE,
            "GrossPPE" DOUBLE,
            "AccumulatedDepreciation" DOUBLE,
            "NetPPE" DOUBLE,
            "TotalAssets" DOUBLE,
            "PayablesAndAccruedExpenses" DOUBLE,
            "CurrentDebtAndCapitalLeaseObligation" DOUBLE,
            "TotalTaxPayable" DOUBLE,
            "CurrentLiabilities" DOUBLE,
            "LongTermDebtAndCapitalLeaseObligation" DOUBLE,
            "TotalLiabilitiesNetMinorityInterest" DOUBLE,
            "CapitalStock" DOUBLE,
            "RetainedEarnings" DOUBLE,
            "StockholdersEquity" DOUBLE,
            PRIMARY KEY ("Ticker", "ReportDate")
        );
    """)

    # 16. Quarterly Cash Flow
    engine.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_cash_flow (
            "DataSource" VARCHAR,
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "Currency" VARCHAR,
            "BeginningCashBalance" DOUBLE,
            "CashReceipts" DOUBLE,
            "CashDisbursements" DOUBLE,
            "CashFromOperations" DOUBLE,
            "FixedAssetPurchases" DOUBLE,
            "NetBorrowing" DOUBLE,
            "IncomeTaxPaid" DOUBLE,
            "SaleOfStock" DOUBLE,
            "EndingCashBalance" DOUBLE,
            PRIMARY KEY ("Ticker", "ReportDate")
        );
    """)

    # 17. Yearly Cash Flow
    engine.execute("""
        CREATE TABLE IF NOT EXISTS yearly_cash_flow (
            "DataSource" VARCHAR,
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "Currency" VARCHAR,
            "IsValid" BOOLEAN,
            "BeginningCashBalance" DOUBLE,
            "CashReceipts" DOUBLE,
            "CashDisbursements" DOUBLE,
            "CashFromOperations" DOUBLE,
            "FixedAssetPurchases" DOUBLE,
            "NetBorrowing" DOUBLE,
            "IncomeTaxPaid" DOUBLE,
            "SaleOfStock" DOUBLE,
            "EndingCashBalance" DOUBLE,
            PRIMARY KEY ("Ticker", "ReportDate")
        );
    """)

    # 18. Yearly Indirect Cash Flow
    engine.execute("""
        CREATE TABLE IF NOT EXISTS yearly_indirect_cash_flow (
            "DataSource" VARCHAR,
            "Ticker" VARCHAR,
            "ReportDate" DATE,
            "Currency" VARCHAR,
            "IsValid" BOOLEAN,
            "IsSectionValid" BOOLEAN,
            "IsRollforwardValid" BOOLEAN,
            "TreasuryOpacityRatio" DOUBLE,
            "NetIncome" DOUBLE,
            "DepreciationAndAmortization" DOUBLE,
            "OtherNonCashAdjustments" DOUBLE,
            "ChangeInAccountsReceivable" DOUBLE,
            "ChangeInInventory" DOUBLE,
            "ChangeInAccountsPayable" DOUBLE,
            "OtherWorkingCapitalChanges" DOUBLE,
            "IncomeTaxPaid" DOUBLE,
            "TotalOperatingCashFlow" DOUBLE,
            "Unmapped_Operating" DOUBLE,
            "CapExPurchaseOfPPE" DOUBLE,
            "PurchaseSaleOfInvestments" DOUBLE,
            "OtherInvestingActivities" DOUBLE,
            "TotalInvestingCashFlow" DOUBLE,
            "Unmapped_Investing" DOUBLE,
            "NetDebtIssuedRepaid" DOUBLE,
            "NetStockIssuedRepurchased" DOUBLE,
            "DividendsPaid" DOUBLE,
            "OtherFinancingActivities" DOUBLE,
            "TotalFinancingCashFlow" DOUBLE,
            "Unmapped_Financing" DOUBLE,
            "EffectOfExchangeRates" DOUBLE,
            "NetChangeInCash" DOUBLE,
            "BeginningCash" DOUBLE,
            "EndingCash" DOUBLE,
            "Unmapped_Rollforward" DOUBLE,
            PRIMARY KEY ("Ticker", "ReportDate")
        );
    """)

    print("[+] All tables validated and created successfully.")


# Automatically run on script import to ensure database readiness
if not engine.default_read_only:
    try:
        initialize_database()
    except duckdb.IOException:
        # If another ingestion script is currently writing, silently pass
        pass


def text(query_string):
    return query_string
