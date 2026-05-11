import pandas as pd
from sqlalchemy import text
from scripts.database import engine
import yfinance as yf

# PHASE 1: STRUCTURAL ANCHORS (YEARLY TABLES)


def fetch_ccc(ticker: str) -> pd.DataFrame:
    query = text("""
        WITH ccc_data AS (
            SELECT 
                i."Ticker", i."ReportDate",
                i."TotalRevenue", i."CostOfRevenue",
                b."Receivables", b."Inventory", b."PayablesAndAccruedExpenses"
            FROM yearly_income_statement i
            JOIN yearly_balance_sheet b ON i."Ticker" = b."Ticker" AND i."ReportDate" = b."ReportDate"
            WHERE i."Ticker" = :ticker
        )
        SELECT 
            "Ticker", "ReportDate",
            CASE WHEN "TotalRevenue" > 0 THEN ROUND(("Receivables" / "TotalRevenue") * 365, 2) ELSE NULL END AS dso,
            CASE WHEN "CostOfRevenue" > 0 THEN ROUND(("Inventory" / "CostOfRevenue") * 365, 2) ELSE NULL END AS dio,
            CASE WHEN "CostOfRevenue" > 0 THEN ROUND(("PayablesAndAccruedExpenses" / "CostOfRevenue") * 365, 2) ELSE NULL END AS dpo,
            CASE 
                WHEN "TotalRevenue" > 0 AND "CostOfRevenue" > 0 
                THEN ROUND((("Receivables" / "TotalRevenue") * 365) + (("Inventory" / "CostOfRevenue") * 365) - (("PayablesAndAccruedExpenses" / "CostOfRevenue") * 365), 2)
                ELSE NULL 
            END AS cash_conversion_cycle
        FROM ccc_data
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})


def fetch_debt_to_equity(ticker: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate",
            (COALESCE("CurrentDebtAndCapitalLeaseObligation", 0) + 
             COALESCE("LongTermDebtAndCapitalLeaseObligation", 0)) AS total_debt,
            "StockholdersEquity",
            CASE WHEN "StockholdersEquity" IS NULL OR "StockholdersEquity" <= 0 THEN NULL 
                 ELSE ROUND((COALESCE("CurrentDebtAndCapitalLeaseObligation", 0) + COALESCE("LongTermDebtAndCapitalLeaseObligation", 0)) / "StockholdersEquity", 2) 
                 END AS debt_to_equity,
            CASE WHEN "StockholdersEquity" IS NULL OR "StockholdersEquity" <= 0 THEN FALSE 
                 WHEN ((COALESCE("CurrentDebtAndCapitalLeaseObligation", 0) + COALESCE("LongTermDebtAndCapitalLeaseObligation", 0)) / "StockholdersEquity") < 2.0 THEN TRUE 
                 ELSE FALSE END AS swarm_pass_leverage
        FROM yearly_balance_sheet
        WHERE "Ticker" = :ticker
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})


def fetch_roic(ticker: str) -> pd.DataFrame:
    query = text("""
        WITH roic_components AS (
            SELECT 
                i."Ticker", i."ReportDate", i."OperatingIncome", i."TaxProvision", i."NetIncome",
                b."StockholdersEquity",
                (COALESCE(b."CurrentDebtAndCapitalLeaseObligation", 0) + COALESCE(b."LongTermDebtAndCapitalLeaseObligation", 0)) AS total_debt,
                COALESCE(b."CashCashEquivalentsAndShortTermInvestments", 0) AS cash
            FROM yearly_income_statement i
            JOIN yearly_balance_sheet b ON i."Ticker" = b."Ticker" AND i."ReportDate" = b."ReportDate"
            WHERE i."Ticker" = :ticker
        )
        SELECT 
            "Ticker", "ReportDate",
            ROUND(CASE WHEN ("NetIncome" + "TaxProvision") > 0 THEN "OperatingIncome" * (1 - ("TaxProvision" / ("NetIncome" + "TaxProvision"))) ELSE "OperatingIncome" * 0.75 END, 2) AS nopat,
            ROUND((total_debt + "StockholdersEquity" - cash), 2) AS invested_capital,
            CASE WHEN (total_debt + "StockholdersEquity" - cash) <= 0 THEN NULL 
                 ELSE ROUND((CASE WHEN ("NetIncome" + "TaxProvision") > 0 THEN "OperatingIncome" * (1 - ("TaxProvision" / ("NetIncome" + "TaxProvision"))) ELSE "OperatingIncome" * 0.75 END) / (total_debt + "StockholdersEquity" - cash), 4) END AS roic,
            CASE WHEN (total_debt + "StockholdersEquity" - cash) <= 0 THEN FALSE 
                 WHEN ((CASE WHEN ("NetIncome" + "TaxProvision") > 0 THEN "OperatingIncome" * (1 - ("TaxProvision" / ("NetIncome" + "TaxProvision"))) ELSE "OperatingIncome" * 0.75 END) / (total_debt + "StockholdersEquity" - cash)) > 0.10 THEN TRUE 
                 ELSE FALSE END AS swarm_pass_roic
        FROM roic_components
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})


def fetch_fcf_yield(ticker: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate", "Currency",
            "TotalOperatingCashFlow", "CapExPurchaseOfPPE",
            ("TotalOperatingCashFlow" - ABS(COALESCE("CapExPurchaseOfPPE", 0))) AS free_cash_flow,
            CASE WHEN ("TotalOperatingCashFlow" - ABS(COALESCE("CapExPurchaseOfPPE", 0))) > 0 THEN TRUE ELSE FALSE END AS swarm_pass_positive_fcf
        FROM yearly_indirect_cash_flow
        WHERE "Ticker" = :ticker
        ORDER BY "ReportDate" DESC;
    """)
    df = pd.read_sql(query, engine, params={"ticker": ticker})

    if not df.empty:
        try:
            # FIX 1: Smart Suffix - If the exact ticker fails, try appending .NS for Indian Screener stocks
            ticker_info = yf.Ticker(ticker).info
            if not ticker_info.get("marketCap") or ticker_info.get("marketCap") <= 1:
                ticker_info = yf.Ticker(f"{ticker}.NS").info

            # YOUR ORIGINAL WORKING LOGIC RESTORED
            live_market_cap = ticker_info.get("marketCap", 1) / 1000000.0
            market_cap_currency = ticker_info.get("currency", "Unknown")
        except Exception:
            live_market_cap = None
            market_cap_currency = "Unknown"

        df["LiveMarketCap"] = live_market_cap

        def calc_yield(row):
            # THE FIX: Allow negative FCF to calculate! Only abort if market cap is missing entirely.
            if (
                not live_market_cap
                or live_market_cap <= 1
                or row["free_cash_flow"] is None
            ):
                return None

            cash_flow = float(row["free_cash_flow"])
            stmt_currency = str(row["Currency"]).upper()

            # FIX 2: The Scale Matcher - If the database cash flow is massive (Raw Screener Data),
            # scale it down by 1,000,000 so it perfectly matches your market cap scale above!
            if abs(cash_flow) > 1000000:
                cash_flow = cash_flow / 1000000.0

            # Currency Normalization
            if stmt_currency == "USD" and market_cap_currency == "INR":
                cash_flow = cash_flow * 83.50
            elif stmt_currency == "INR" and market_cap_currency == "USD":
                cash_flow = cash_flow / 83.50

            # True Yield Calculation (Raw FCF / Raw Market Cap)
            return cash_flow / live_market_cap

        df["FCF_Yield"] = df.apply(calc_yield, axis=1)

        # Calculate Swarm Pass for FCF Yield (> 5%)
        df["swarm_pass_cheap"] = df["FCF_Yield"].apply(
            lambda x: x > 0.05 if pd.notnull(x) else False
        )

    return df


def fetch_dol(ticker: str) -> pd.DataFrame:
    query = text("""
        WITH lag_data AS (
            SELECT 
                "Ticker", "ReportDate", "TotalRevenue",
                LAG("TotalRevenue") OVER (PARTITION BY "Ticker" ORDER BY "ReportDate" ASC) AS prev_revenue,
                "OperatingIncome",
                LAG("OperatingIncome") OVER (PARTITION BY "Ticker" ORDER BY "ReportDate" ASC) AS prev_operating_income
            FROM yearly_income_statement
            WHERE "Ticker" = :ticker
        ),
        pct_changes AS (
            SELECT 
                "Ticker", "ReportDate", "TotalRevenue", "OperatingIncome",
                CASE WHEN prev_revenue IS NULL OR prev_revenue = 0 THEN NULL ELSE ("TotalRevenue" - prev_revenue) / ABS(prev_revenue) END AS pct_change_revenue,
                CASE WHEN prev_operating_income IS NULL OR prev_operating_income = 0 THEN NULL ELSE ("OperatingIncome" - prev_operating_income) / ABS(prev_operating_income) END AS pct_change_ebit
            FROM lag_data
        )
        SELECT 
            "Ticker", "ReportDate",
            ROUND(pct_change_revenue * 100, 2) AS rev_growth_pct,
            ROUND(pct_change_ebit * 100, 2) AS ebit_growth_pct,
            CASE WHEN pct_change_revenue IS NULL OR pct_change_revenue = 0 THEN NULL ELSE ROUND(pct_change_ebit / pct_change_revenue, 2) END AS degree_of_operating_leverage,
            CASE WHEN pct_change_revenue > 0 AND pct_change_ebit > pct_change_revenue THEN TRUE ELSE FALSE END AS swarm_pass_positive_leverage
        FROM pct_changes
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})


# PHASE 2: TACTICAL RESPONDERS (QUARTERLY TABLES)


def fetch_cfo_to_pat(ticker: str) -> pd.DataFrame:
    query = text("""
        WITH cfo_pat_data AS (
            SELECT 
                i."Ticker", i."ReportDate", 
                i."NetIncome", 
                c."TotalOperatingCashFlow"
            FROM quarterly_income_statement i
            JOIN yearly_indirect_cash_flow c 
            ON i."Ticker" = c."Ticker" AND i."ReportDate" = c."ReportDate"
            WHERE i."Ticker" = :ticker
        )
        SELECT 
            "Ticker", "ReportDate",
            "NetIncome", "TotalOperatingCashFlow" AS cfo,
            CASE 
                WHEN "NetIncome" IS NULL OR "NetIncome" = 0 THEN NULL
                ELSE ROUND("TotalOperatingCashFlow" / "NetIncome", 2) 
            END AS cfo_to_pat,
            CASE 
                WHEN "NetIncome" <= 0 THEN FALSE
                WHEN ("TotalOperatingCashFlow" / "NetIncome") >= 0.80 THEN TRUE
                ELSE FALSE 
            END AS swarm_pass_quality_of_earnings
        FROM cfo_pat_data
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})


def fetch_operating_margin(ticker: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate",
            CASE WHEN "TotalRevenue" = 0 OR "TotalRevenue" IS NULL THEN NULL 
                 ELSE ROUND("OperatingIncome" / "TotalRevenue", 4) END AS operating_margin,
            CASE WHEN "TotalRevenue" = 0 OR "TotalRevenue" IS NULL THEN FALSE 
                 WHEN "OperatingIncome" > 0 THEN TRUE ELSE FALSE END AS swarm_pass_operating_margin
        FROM quarterly_income_statement
        WHERE "Ticker" = :ticker
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})


def fetch_gross_margin(ticker: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate",
            CASE WHEN "TotalRevenue" = 0 OR "TotalRevenue" IS NULL THEN NULL 
                 ELSE ROUND("GrossProfit" / "TotalRevenue", 4) END AS gross_margin,
            CASE WHEN "TotalRevenue" = 0 OR "TotalRevenue" IS NULL THEN FALSE 
                 WHEN "GrossProfit" > 0 THEN TRUE ELSE FALSE END AS swarm_pass_gross_margin
        FROM quarterly_income_statement
        WHERE "Ticker" = :ticker
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})


def fetch_interest_coverage(ticker: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate",
            "OperatingIncome", "NetInterestIncome",
            CASE WHEN "NetInterestIncome" IS NULL OR "NetInterestIncome" = 0 THEN NULL
                 ELSE ROUND("OperatingIncome" / ABS("NetInterestIncome"), 2) END AS interest_coverage,
            CASE WHEN "NetInterestIncome" IS NULL OR "NetInterestIncome" = 0 THEN FALSE
                 WHEN ("OperatingIncome" / ABS("NetInterestIncome")) > 1.5 THEN TRUE 
                 ELSE FALSE END AS swarm_pass_solvency
        FROM quarterly_income_statement
        WHERE "Ticker" = :ticker
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})


def fetch_asset_turnover(ticker: str) -> pd.DataFrame:
    query = text("""
        WITH at_data AS (
            SELECT 
                i."Ticker", i."ReportDate", 
                i."TotalRevenue", 
                b."TotalAssets"
            FROM quarterly_income_statement i
            JOIN quarterly_balance_sheet b 
            ON i."Ticker" = b."Ticker" AND i."ReportDate" = b."ReportDate"
            WHERE i."Ticker" = :ticker
        )
        SELECT 
            "Ticker", "ReportDate",
            "TotalRevenue", "TotalAssets",
            CASE WHEN "TotalAssets" IS NULL OR "TotalAssets" = 0 THEN NULL
                 ELSE ROUND("TotalRevenue" / "TotalAssets", 2) END AS asset_turnover
        FROM at_data
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(query, engine, params={"ticker": ticker})
