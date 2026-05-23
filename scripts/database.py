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
    UniqueConstraint,
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
    Column("Ticker", String(50), primary_key=True, index=True),
    Column("IndicatorName", String(100), nullable=True),
    Column("TargetTable", String(50), nullable=False),
    Column("Sector", String(100)),
    Column("Industry", String(100)),
    Column(
        "AssetClass", String(50), nullable=False
    ),  # 'Equity', 'Macro_Index', 'Volatility'
    Column("Exchange", String(50)),
    Column("IsActive", Boolean, default=True),
    Column("valid_data_since", Date),
    Column("Description", String(255)),
)

macro_daily_ledger = Table(
    "macro_daily_ledger",
    metadata,
    Column(
        "IndicatorName", String(100), primary_key=True, index=True
    ),  # 'US_10Y_Yield'
    Column(
        "ReportDate", Date, primary_key=True, index=True
    ),  # Stored purely as YYYY-MM-DD
    Column("Open", Float, nullable=True),
    Column("High", Float, nullable=True),
    Column("Low", Float, nullable=True),
    Column("Close_Value", Float, nullable=False),
    Column("Volume", BigInteger, nullable=True),
)

macro_intraday_ledger = Table(
    "macro_intraday_ledger",
    metadata,
    Column(
        "IndicatorName", String(100), primary_key=True, index=True
    ),  # 'US_10Y_Yield'
    Column(
        "ReportDate", TIMESTAMP, primary_key=True, index=True
    ),  # Includes exact Hr:Min:Sec
    Column("Timeframe", String(10), primary_key=True, index=True),  # '1h', '30m', '5m'
    Column("Open", Float, nullable=True),
    Column("High", Float, nullable=True),
    Column("Low", Float, nullable=True),
    Column("Close_Value", Float, nullable=False),
    Column("Volume", BigInteger, nullable=True),
)

# --- ADD THIS BLOCK TO YOUR EXISTING TABLE DEFINITIONS ---
prediction_ledger = Table(
    "prediction_ledger",
    metadata,
    Column("engine_name", String, primary_key=True),
    Column("ticker", String, primary_key=True),
    Column("asof_date", Date, primary_key=True),
    Column("horizon", String, primary_key=True),  # e.g., '2D', '5D', '20D'
    Column("signal", String),  # 'BUY', 'WATCH', 'AVOID', 'SHORT-BIAS'
    Column("score", Float),  # Normalized score (e.g., -1.0 to 1.0)
    Column("confidence", Float),  # 0.0 to 1.0
    Column("veto_flag", Boolean, default=False),
    Column("penalty", Float, default=0.0),
    Column("target_metric", String),  # Expected return band
    Column("reason_json", JSONB),  # Text explanation of drivers
    Column("feature_json", JSONB),  # Raw input metrics
    Column("data_quality_score", Float),
    Column("created_at", DateTime, default=datetime.utcnow),
)

global_assets_daily = Table(
    "global_assets_daily",
    metadata,
    Column("Ticker", String(100), primary_key=True, index=True),
    Column("ReportDate", Date, primary_key=True, index=True),
    Column("AssetClass", String(50), nullable=True),  # 'US Equity', 'Index', 'Crypto'
    Column("Open", Float, nullable=True),
    Column("High", Float, nullable=True),
    Column("Low", Float, nullable=True),
    Column("Close", Float, nullable=False),
    Column("Volume", BigInteger, nullable=True),
)

global_assets_intraday = Table(
    "global_assets_intraday",
    metadata,
    Column("Ticker", String(100), primary_key=True, index=True),
    Column("ReportDate", TIMESTAMP, primary_key=True, index=True),
    Column("Timeframe", String(10), primary_key=True, index=True),
    Column("Open", Float, nullable=True),
    Column("High", Float, nullable=True),
    Column("Low", Float, nullable=True),
    Column("Close", Float, nullable=False),
    Column("Volume", BigInteger, nullable=True),
)

unified_market_master = Table(
    "unified_market_master",
    metadata,
    # --- COMPOSITE PRIMARY KEY (Prevents Overwriting) ---
    Column("Ticker", String(100), primary_key=True, index=True),
    Column("ReportDate", TIMESTAMP, primary_key=True, index=True),
    Column(
        "InstrumentType", String(20), primary_key=True, index=True
    ),  # 'CASH', 'FUTSTK', 'OPTIDX', 'FUTCOM', etc.
    # NOTE: SQL databases do not allow NULL in Primary Keys.
    # For CASH/Spot assets, your parser should insert a dummy date (e.g., '2099-12-31')
    # and a strike of 0.0 to safely store them alongside derivatives.
    Column("ExpiryDate", Date, primary_key=True, index=True),
    Column("StrikePrice", Float, primary_key=True),
    # --- STANDARD OHLCV ---
    Column("Open", Float, nullable=True),
    Column("High", Float, nullable=True),
    Column("Low", Float, nullable=True),
    Column("Close", Float, nullable=True),
    Column(
        "Volume", BigInteger, nullable=True
    ),  # 'TTL_TRD_QNTY', 'CONTRACTS', 'Volume'
    # --- CASH MARKET ALPHA (nse_cash & nse_short_selling) ---
    Column("Exchange_Series", String(10), nullable=True),  # 'EQ', 'BE'
    Column("Turnover", Float, nullable=True),  # 'TURNOVER_LACS', 'VAL_INLAKH', 'Value'
    Column(
        "No_Of_Trades", BigInteger, nullable=True
    ),  # 'NO_OF_TRADES', 'TtlNbOfTxsExctd'
    Column("Delivery_Qty", BigInteger, nullable=True),  # 'DELIV_QTY'
    Column("Delivery_Percentage", Float, nullable=True),  # 'DELIV_PER'
    Column("Short_Volume", BigInteger, nullable=True),  # 'Quantity' from short selling
    # --- DERIVATIVES ALPHA (F&O & MCX) ---
    Column("OptionType", String(10), nullable=True),  # 'CE', 'PE'
    Column(
        "Open_Interest", BigInteger, nullable=True
    ),  # 'OpnIntrst', 'OPEN_INT', 'OpenInterest'
    Column("Change_In_OI", BigInteger, nullable=True),  # 'ChngInOpnIntrst', 'CHG_IN_OI'
    Column("Settlement_Price", Float, nullable=True),  # 'SttlmPric', 'SETTLE_PR'
    Column("Underlying_Price", Float, nullable=True),  # 'UndrlygPric'
)


institutional_ledger = Table(
    "institutional_ledger",
    metadata,
    # --- COMPOSITE PRIMARY KEY ---
    Column("ReportDate", TIMESTAMP, primary_key=True, index=True),
    Column(
        "ClientType", String(50), primary_key=True, index=True
    ),  # 'FII', 'DII', 'Pro', 'Client'
    # --- CASH FLOW METRICS (From NiftyTrader FII/DII) ---
    # Will only be populated when ClientType is 'FII' or 'DII'
    Column("Cash_Buy_Value", Float, nullable=True),
    Column("Cash_Sell_Value", Float, nullable=True),
    Column("Cash_Net_Value", Float, nullable=True),
    Column("Nifty_Close", Float, nullable=True),
    # --- DERIVATIVE CONTRACT METRICS (From nse_part_oi) ---
    # Populated for all Client Types
    Column("Future_Index_Long", BigInteger, nullable=True),
    Column("Future_Index_Short", BigInteger, nullable=True),
    Column("Future_Stock_Long", BigInteger, nullable=True),
    Column("Future_Stock_Short", BigInteger, nullable=True),
    Column("Option_Index_Call_Long", BigInteger, nullable=True),
    Column("Option_Index_Put_Long", BigInteger, nullable=True),
    Column("Option_Index_Call_Short", BigInteger, nullable=True),
    Column("Option_Index_Put_Short", BigInteger, nullable=True),
    Column("Option_Stock_Call_Long", BigInteger, nullable=True),
    Column("Option_Stock_Put_Long", BigInteger, nullable=True),
    Column("Option_Stock_Call_Short", BigInteger, nullable=True),
    Column("Option_Stock_Put_Short", BigInteger, nullable=True),
    Column("Total_Long_Contracts", BigInteger, nullable=True),
    Column("Total_Short_Contracts", BigInteger, nullable=True),
)

trade_events_ledger = Table(
    "trade_events_ledger",
    metadata,
    # --- SURROGATE PRIMARY KEY ---
    Column("EventID", Integer, primary_key=True, autoincrement=True),
    # --- EVENT METADATA ---
    Column("ReportDate", TIMESTAMP, index=True, nullable=False),
    Column("Ticker", String(50), index=True, nullable=False),
    Column("EventType", String(50), nullable=False),
    # --- TRANSACTION DETAILS ---
    Column("SecurityName", String(255), nullable=True),
    Column("ClientName", String(255), nullable=False),
    Column("TransactionType", String(20), nullable=False),
    Column("Quantity", BigInteger, nullable=False),
    Column("TradePrice", Float, nullable=False),
    Column("Remarks", String(255), nullable=True),
    # --- THE FIX: ENFORCE UNIQUE TRADES FOR UPSERTS ---
    UniqueConstraint(
        "ReportDate",
        "Ticker",
        "ClientName",
        "TransactionType",
        "Quantity",
        "TradePrice",
        name="unique_trade_event",
    ),
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
