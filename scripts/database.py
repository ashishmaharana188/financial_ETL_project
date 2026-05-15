from sqlalchemy import (
    create_engine,
    Column,
    String,
    Date,
    Numeric,
    MetaData,
    Table,
    Boolean,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy import Column, String, Date, MetaData, Table

# Setup Engine
engine = create_engine("postgresql+psycopg2://postgres:123456@localhost:5432/postgres")

# Setup Metadata
metadata = MetaData(schema="public")


company_profiles = Table(
    "company_profiles",
    metadata,
    Column("Ticker", String(50), primary_key=True),
    Column("CompanyName", String(200)),
    Column("Sector", String(100)),
    Column("Industry", String(100)),
    Column("valid_data_since", Date),
)

macro_indicators = Table(
    "macro_indicators",
    metadata,
    Column("IndicatorName", String(100), primary_key=True),
    Column("ReportDate", String(50), primary_key=True),
    Column("Value", Numeric),
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
