from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy import (
    Column,
    String,
    Date,
    Table,
    DateTime,
    Float,
    BigInteger,
    TIMESTAMP,
    create_engine,
    Numeric,
    MetaData,
    Boolean,
    text,
    Integer,
)
import pandas as pd
from datetime import datetime, timedelta

# Setup Engine
engine = create_engine("postgresql+psycopg2://postgres:123456@localhost:5432/postgres")

# Setup Metadata
metadata = MetaData(schema="public")

market_metadata = Table(
    "market_metadata",
    metadata,
    # The actual symbol yfinance or the exchange uses (e.g., '^TNX', 'RELIANCE.NS', 'DX-Y.NYB')
    Column("Ticker", String(50), primary_key=True, index=True),
    # The translated name used in your macro table (e.g., 'US_10Y_Yield').
    # Can be NULL for regular equities.
    Column("IndicatorName", String(100), nullable=True),
    # Tells the Python scraper exactly which table to insert the data into
    Column(
        "TargetTable", String(50), nullable=False
    ),  # 'market_pricing_daily' OR 'macro_indicators'
    Column(
        "AssetClass", String(50), nullable=False
    ),  # 'Equity', 'Macro_Index', 'Volatility'
    Column("Exchange", String(50)),
    Column("IsActive", Boolean, default=True),
    Column("Description", String(255)),
)

market_pricing_daily = Table(
    "market_pricing_daily",
    metadata,
    Column("IndicatorName", String(100), primary_key=True, index=True),
    Column("ReportDate", TIMESTAMP, primary_key=True, index=True),
    # The OHLCV columns
    Column("Open", Float, nullable=True),
    Column("High", Float, nullable=True),
    Column("Low", Float, nullable=True),
    Column("Close_Value", Float, nullable=False),
    Column("Volume", BigInteger, nullable=True),
)

macro_indicators = Table(
    "macro_indicators",
    metadata,
    Column("IndicatorName", String(100), primary_key=True, index=True),
    Column("ReportDate", TIMESTAMP, primary_key=True, index=True),
    Column("Open", Float, nullable=True),
    Column("High", Float, nullable=True),
    Column("Low", Float, nullable=True),
    Column("Close_Value", Float, nullable=False),
    Column("Volume", BigInteger, nullable=True),
)

market_bhavcopy_metrics = Table(
    "market_bhavcopy_metrics",
    metadata,
    Column(
        "IndicatorName", String(100), primary_key=True, index=True
    ),  # Ticker or Commodity Name
    Column("ReportDate", TIMESTAMP, primary_key=True, index=True),
    # Standard OHLCV (Used for MCX, ETFs, SGBs)
    Column("Open", Float, nullable=True),
    Column("High", Float, nullable=True),
    Column("Low", Float, nullable=True),
    Column("Close_Value", Float, nullable=True),
    Column("Volume", BigInteger, nullable=True),
    # The Bhavcopy Extensions (For Equities/Indices)
    Column("Delivery_Percentage", Float, nullable=True),
    Column("Short_Volume", BigInteger, nullable=True),  # From nse_short_selling.csv
    Column("Cost_Of_Carry", Float, nullable=True),  # Calculated Spot vs Futures
    Column("Open_Interest", BigInteger, nullable=True),  # For MCX Commodities
    # Asset Classification to prevent pollution
    Column(
        "AssetClass", String(50), nullable=False
    ),  # 'Equity', 'ETF', 'SGB', 'Commodity'
)

derivatives_matrix = Table(
    "derivatives_matrix",
    metadata,
    Column("Ticker", String(50), primary_key=True, index=True),
    Column("ReportDate", TIMESTAMP, primary_key=True, index=True),
    Column("ExpiryDate", Date, primary_key=True, index=True),  # e.g., '2026-05-28'
    # 'FUT', 'CE', 'PE', or 'AGGREGATE' (for PCR/Rollover)
    Column("InstrumentType", String(20), primary_key=True, index=True),
    # 0.0 for Futures or Aggregates
    Column("StrikePrice", Float, primary_key=True),
    # The Matrix Values
    Column("Close_Price", Float, nullable=True),
    Column("Open_Interest", BigInteger, nullable=True),
    Column("Change_In_OI", BigInteger, nullable=True),
    Column("Volume", BigInteger, nullable=True),
    # Aggregate Metrics (Populated only when InstrumentType = 'AGGREGATE')
    Column("OI_PCR", Float, nullable=True),
    Column("Change_In_OI_PCR", Float, nullable=True),
    Column("Volume_PCR", Float, nullable=True),
    Column("Rollover_Percentage", Float, nullable=True),
)

trade_events_ledger = Table(
    "trade_events_ledger",
    metadata,
    Column("EventID", Integer, primary_key=True, autoincrement=True),  # Surrogate PK
    Column("ReportDate", TIMESTAMP, index=True, nullable=False),
    Column("Ticker", String(50), index=True, nullable=False),
    # The Event Details
    Column("EventType", String(50), nullable=False),  # 'Bulk Deal', 'Block Deal'
    Column("ClientName", String(255), nullable=False),
    Column("TransactionType", String(20), nullable=False),  # 'BUY' or 'SELL'
    Column("Quantity", BigInteger, nullable=False),
    Column("AveragePrice", Float, nullable=False),
)

company_profiles = Table(
    "company_profiles",
    metadata,
    Column("Ticker", String(50), primary_key=True),
    Column("CompanyName", String(200)),
    Column("Sector", String(100)),
    Column("Industry", String(100)),
    Column("valid_data_since", Date),
)


ai_forensic_logs = Table(
    "ai_forensic_logs",
    metadata,
    Column("TicketID", String(100), primary_key=True),
    Column("Timestamp", Date),
    Column("Ticker", String(50)),
    Column("LeakType", String(50)),
    Column("LeakAmount", Numeric),
    Column("MissingKeyFound", String(200)),
    Column("SuggestedCategory", String(200)),
    Column("Reasoning", String(1000)),
    Column("Status", String(50), default="PENDING"),
)


# Define the Raw JSONB Vault (Bronze Layer)
raw_financials = Table(
    "raw_financials",
    metadata,
    Column("DataSource", String(50)),
    Column("Ticker", String(50), primary_key=True),
    Column("ReportDate", Date, primary_key=True),
    Column("StatementType", String(50), primary_key=True),  # e.g., 'IS', 'BS', 'CF'
    Column("RawData", JSONB),
)

# Define Tables
quarterly_income_statement = Table(
    "quarterly_income_statement",
    metadata,
    Column("DataSource", String(50)),
    Column("Ticker", String(50), primary_key=True),
    Column("ReportDate", Date, primary_key=True),
    Column("Currency", String(10)),
    Column("TotalRevenue", Numeric),
    Column("CostOfRevenue", Numeric),
    Column("GrossProfit", Numeric),
    Column("OperatingExpense", Numeric),
    Column("OperatingIncome", Numeric),
    Column("NetInterestIncome", Numeric),
    Column("TaxProvision", Numeric),
    Column("NetIncome", Numeric),
)

yearly_income_statement = Table(
    "yearly_income_statement",
    metadata,
    Column("DataSource", String(50)),
    Column("Ticker", String(50), primary_key=True),
    Column("ReportDate", Date, primary_key=True),
    Column("Currency", String(10)),
    Column("IsValid", Boolean),
    Column("TotalRevenue", Numeric),
    Column("CostOfRevenue", Numeric),
    Column("GrossProfit", Numeric),
    Column("OperatingExpense", Numeric),
    Column("OperatingIncome", Numeric),
    Column("NetInterestIncome", Numeric),
    Column("TaxProvision", Numeric),
    Column("NetIncome", Numeric),
)

quarterly_balance_sheet = Table(
    "quarterly_balance_sheet",
    metadata,
    Column("DataSource", String(50)),
    Column("Ticker", String(50), primary_key=True),
    Column("ReportDate", Date, primary_key=True),
    Column("Currency", String(10)),
    Column("CashCashEquivalentsAndShortTermInvestments", Numeric),
    Column("Receivables", Numeric),
    Column("Inventory", Numeric),
    Column("CurrentAssets", Numeric),
    Column("TotalNonCurrentAssets", Numeric),
    Column("GrossPPE", Numeric),
    Column("AccumulatedDepreciation", Numeric),
    Column("NetPPE", Numeric),
    Column("TotalAssets", Numeric),
    Column("PayablesAndAccruedExpenses", Numeric),
    Column("CurrentDebtAndCapitalLeaseObligation", Numeric),
    Column("TotalTaxPayable", Numeric),
    Column("CurrentLiabilities", Numeric),
    Column("LongTermDebtAndCapitalLeaseObligation", Numeric),
    Column("TotalLiabilitiesNetMinorityInterest", Numeric),
    Column("CapitalStock", Numeric),
    Column("RetainedEarnings", Numeric),
    Column("StockholdersEquity", Numeric),
)

yearly_balance_sheet = Table(
    "yearly_balance_sheet",
    metadata,
    Column("DataSource", String(50)),
    Column("Ticker", String(50), primary_key=True),
    Column("ReportDate", Date, primary_key=True),
    Column("Currency", String(10)),
    Column("IsValid", Boolean),
    Column("CashCashEquivalentsAndShortTermInvestments", Numeric),
    Column("Receivables", Numeric),
    Column("Inventory", Numeric),
    Column("CurrentAssets", Numeric),
    Column("TotalNonCurrentAssets", Numeric),
    Column("GrossPPE", Numeric),
    Column("AccumulatedDepreciation", Numeric),
    Column("NetPPE", Numeric),
    Column("TotalAssets", Numeric),
    Column("PayablesAndAccruedExpenses", Numeric),
    Column("CurrentDebtAndCapitalLeaseObligation", Numeric),
    Column("TotalTaxPayable", Numeric),
    Column("CurrentLiabilities", Numeric),
    Column("LongTermDebtAndCapitalLeaseObligation", Numeric),
    Column("TotalLiabilitiesNetMinorityInterest", Numeric),
    Column("CapitalStock", Numeric),
    Column("RetainedEarnings", Numeric),
    Column("StockholdersEquity", Numeric),
)

quarterly_cash_flow = Table(
    "quarterly_cash_flow",
    metadata,
    Column("DataSource", String(50)),
    Column("Ticker", String(50), primary_key=True),
    Column("ReportDate", Date, primary_key=True),
    Column("Currency", String(10)),
    Column("BeginningCashBalance", Numeric),
    Column("CashReceipts", Numeric),
    Column("CashDisbursements", Numeric),
    Column("CashFromOperations", Numeric),
    Column("FixedAssetPurchases", Numeric),
    Column("NetBorrowing", Numeric),
    Column("IncomeTaxPaid", Numeric),
    Column("SaleOfStock", Numeric),
    Column("EndingCashBalance", Numeric),
)

yearly_cash_flow = Table(
    "yearly_cash_flow",
    metadata,
    Column("DataSource", String(50)),
    Column("Ticker", String(50), primary_key=True),
    Column("ReportDate", Date, primary_key=True),
    Column("Currency", String(10)),
    Column("IsValid", Boolean),
    Column("BeginningCashBalance", Numeric),
    Column("CashReceipts", Numeric),
    Column("CashDisbursements", Numeric),
    Column("CashFromOperations", Numeric),
    Column("FixedAssetPurchases", Numeric),
    Column("NetBorrowing", Numeric),
    Column("IncomeTaxPaid", Numeric),
    Column("SaleOfStock", Numeric),
    Column("EndingCashBalance", Numeric),
)

yearly_indirect_cash_flow = Table(
    "yearly_indirect_cash_flow",
    metadata,
    Column("DataSource", String(50)),
    Column("Ticker", String(50), primary_key=True),
    Column("ReportDate", Date, primary_key=True),
    Column("Currency", String(10)),
    Column("IsValid", Boolean),
    Column("IsSectionValid", Boolean),
    Column("IsRollforwardValid", Boolean),
    Column("TreasuryOpacityRatio", Numeric),
    Column("NetIncome", Numeric),
    Column("DepreciationAndAmortization", Numeric),
    Column("OtherNonCashAdjustments", Numeric),
    Column("ChangeInAccountsReceivable", Numeric),
    Column("ChangeInInventory", Numeric),
    Column("ChangeInAccountsPayable", Numeric),
    Column("OtherWorkingCapitalChanges", Numeric),
    Column("IncomeTaxPaid", Numeric),
    Column("TotalOperatingCashFlow", Numeric),
    Column("Unmapped_Operating", Numeric),
    Column("CapExPurchaseOfPPE", Numeric),
    Column("PurchaseSaleOfInvestments", Numeric),
    Column("OtherInvestingActivities", Numeric),
    Column("TotalInvestingCashFlow", Numeric),
    Column("Unmapped_Investing", Numeric),
    Column("NetDebtIssuedRepaid", Numeric),
    Column("NetStockIssuedRepurchased", Numeric),
    Column("DividendsPaid", Numeric),
    Column("OtherFinancingActivities", Numeric),
    Column("TotalFinancingCashFlow", Numeric),
    Column("Unmapped_Financing", Numeric),
    Column("EffectOfExchangeRates", Numeric),
    Column("NetChangeInCash", Numeric),
    Column("BeginningCash", Numeric),
    Column("EndingCash", Numeric),
    Column("Unmapped_Rollforward", Numeric),
)

# Create all tables
metadata.create_all(engine)
print("All tables validated and created successfully.")


def get_missing_dates(table_name):
    """
    Queries the database for the most recent date in a given table,
    and returns a list of missing business days up to today.
    """
    # Using double quotes around ReportDate to respect PostgreSQL case sensitivity
    query = text(f'SELECT MAX("ReportDate") FROM {table_name};')

    try:
        with engine.connect() as conn:
            result = conn.execute(query).scalar()

        if result is None:
            print(f"[*] Table {table_name} is empty. Bulk load required.")
            return []

        last_db_date = pd.to_datetime(result).date()
        today = datetime.today().date()

        if last_db_date >= today:
            print(f"[*] {table_name} is fully up to date ({last_db_date}).")
            return []

        # Generate business days (B) between last_db_date (exclusive) and today (inclusive)
        # We add 1 day to last_db_date so we don't re-process the exact day we already have.
        start_date = last_db_date + timedelta(days=1)
        missing_b_days = pd.bdate_range(start=start_date, end=today).date.tolist()

        print(
            f"[*] {table_name}: Found {len(missing_b_days)} missing business days between {last_db_date} and {today}."
        )
        return missing_b_days

    except Exception as e:
        print(f"[-] Error checking missing dates for {table_name}: {e}")
        return []
