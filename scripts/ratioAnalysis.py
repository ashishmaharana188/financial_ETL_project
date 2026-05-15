import pandas as pd
from sqlalchemy import text
from scripts.database import engine
import yfinance as yf

# PHASE 1: STRUCTURAL ANCHORS (YEARLY TABLES)


def fetch_ccc(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        WITH ccc_data AS (
            SELECT 
                i."Ticker", i."ReportDate",
                i."TotalRevenue", i."CostOfRevenue",
                b."Receivables", b."Inventory", b."PayablesAndAccruedExpenses"
            FROM yearly_income_statement i
            JOIN yearly_balance_sheet b ON i."Ticker" = b."Ticker" AND i."ReportDate" = b."ReportDate" AND i."DataSource" = b."DataSource"
            WHERE i."Ticker" = :ticker AND i."DataSource" = :data_source
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
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


def fetch_debt_to_equity(ticker: str, data_source: str) -> pd.DataFrame:
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
        WHERE "Ticker" = :ticker AND "DataSource" = :data_source
          
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


def fetch_roic(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        WITH roic_components AS (
            SELECT 
                i."Ticker", i."ReportDate", i."OperatingIncome", i."TaxProvision", i."NetIncome",
                b."StockholdersEquity",
                (COALESCE(b."CurrentDebtAndCapitalLeaseObligation", 0) + COALESCE(b."LongTermDebtAndCapitalLeaseObligation", 0)) AS total_debt,
                COALESCE(b."CashCashEquivalentsAndShortTermInvestments", 0) AS cash
            FROM yearly_income_statement i
            JOIN yearly_balance_sheet b ON i."Ticker" = b."Ticker" AND i."ReportDate" = b."ReportDate" AND i."DataSource" = b."DataSource"
            WHERE i."Ticker" = :ticker AND i."DataSource" = :data_source
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
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


def fetch_fcf_yield(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate", "Currency",
            "TotalOperatingCashFlow", "CapExPurchaseOfPPE",
            ("TotalOperatingCashFlow" - ABS(COALESCE("CapExPurchaseOfPPE", 0))) AS free_cash_flow,
            CASE WHEN ("TotalOperatingCashFlow" - ABS(COALESCE("CapExPurchaseOfPPE", 0))) > 0 THEN TRUE ELSE FALSE END AS swarm_pass_positive_fcf
        FROM yearly_indirect_cash_flow
        WHERE "Ticker" = :ticker AND "DataSource" = :data_source
          
        ORDER BY "ReportDate" DESC;
    """)
    df = pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )

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


def fetch_dol(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        WITH lag_data AS (
            SELECT 
                "Ticker", "ReportDate", "TotalRevenue",
                LAG("TotalRevenue") OVER (PARTITION BY "Ticker" ORDER BY "ReportDate" ASC) AS prev_revenue,
                "OperatingIncome",
                LAG("OperatingIncome") OVER (PARTITION BY "Ticker" ORDER BY "ReportDate" ASC) AS prev_operating_income
            FROM yearly_income_statement
            WHERE "Ticker" = :ticker AND "DataSource" = :data_source
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
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


# PHASE 2: TACTICAL RESPONDERS (QUARTERLY TABLES)


def fetch_cfo_to_pat(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        WITH cfo_pat_data AS (
            SELECT 
                i."Ticker", i."ReportDate", 
                i."NetIncome", 
                c."TotalOperatingCashFlow"
            FROM quarterly_income_statement i
            JOIN yearly_indirect_cash_flow c 
            ON i."Ticker" = c."Ticker" AND i."ReportDate" = c."ReportDate" AND i."DataSource" = c."DataSource"
            WHERE i."Ticker" = :ticker AND i."DataSource" = :data_source
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
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


def fetch_operating_margin(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate",
            CASE WHEN "TotalRevenue" = 0 OR "TotalRevenue" IS NULL THEN NULL 
                 ELSE ROUND("OperatingIncome" / "TotalRevenue", 4) END AS operating_margin,
            CASE WHEN "TotalRevenue" = 0 OR "TotalRevenue" IS NULL THEN FALSE 
                 WHEN "OperatingIncome" > 0 THEN TRUE ELSE FALSE END AS swarm_pass_operating_margin
        FROM quarterly_income_statement
        WHERE "Ticker" = :ticker AND "DataSource" = :data_source
          
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


def fetch_gross_margin(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate",
            CASE WHEN "TotalRevenue" = 0 OR "TotalRevenue" IS NULL THEN NULL 
                 ELSE ROUND("GrossProfit" / "TotalRevenue", 4) END AS gross_margin,
            CASE WHEN "TotalRevenue" = 0 OR "TotalRevenue" IS NULL THEN FALSE 
                 WHEN "GrossProfit" > 0 THEN TRUE ELSE FALSE END AS swarm_pass_gross_margin
        FROM quarterly_income_statement
        WHERE "Ticker" = :ticker AND "DataSource" = :data_source
          
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


def fetch_interest_coverage(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        SELECT 
            "Ticker", "ReportDate",
            "OperatingIncome", "NetInterestIncome",
            
            -- FIX: Winsorize (Cap) the Coverage Math at 99.99x to protect regression models
            CASE 
                WHEN "NetInterestIncome" IS NULL OR "NetInterestIncome" = 0 THEN 99.99
                WHEN ("OperatingIncome" / NULLIF(ABS("NetInterestIncome"), 0)) > 99.99 THEN 99.99
                ELSE ROUND("OperatingIncome" / NULLIF(ABS("NetInterestIncome"), 0), 2) 
            END AS interest_coverage,
            
            -- Solvency Triage
            CASE 
                WHEN "NetInterestIncome" IS NULL THEN FALSE
                WHEN "NetInterestIncome" = 0 THEN 
                    CASE WHEN "OperatingIncome" > 0 THEN TRUE ELSE FALSE END
                WHEN ("OperatingIncome" / NULLIF(ABS("NetInterestIncome"), 0)) > 1.5 THEN TRUE 
                ELSE FALSE 
            END AS swarm_pass_solvency
            
        FROM quarterly_income_statement
        WHERE "Ticker" = :ticker AND "DataSource" = :data_source
          
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


def fetch_asset_turnover(ticker: str, data_source: str) -> pd.DataFrame:
    query = text("""
        WITH at_data AS (
            SELECT 
                i."Ticker", i."ReportDate", 
                i."TotalRevenue", 
                b."TotalAssets"
            FROM yearly_income_statement i
            JOIN yearly_balance_sheet b 
            ON i."Ticker" = b."Ticker" AND i."ReportDate" = b."ReportDate" AND i."DataSource" = b."DataSource"
            WHERE i."Ticker" = :ticker AND i."DataSource" = :data_source
        )
        SELECT 
            "Ticker", "ReportDate",
            "TotalRevenue", "TotalAssets",
            CASE WHEN "TotalAssets" IS NULL OR "TotalAssets" = 0 THEN NULL
                 ELSE ROUND(("TotalRevenue" * 1.0) / "TotalAssets", 2) END AS asset_turnover
        FROM at_data
        ORDER BY "ReportDate" DESC;
    """)
    return pd.read_sql(
        query, engine, params={"ticker": ticker, "data_source": data_source}
    )


def fetch_piotroski_f_score(ticker: str, engine) -> pd.DataFrame:
    """
    Calculates the 9-point Piotroski F-Score using YEARLY data for Screener.
    Compares current year to the previous year (Lag 1).
    """
    query = text("""
        WITH UnifiedData AS (
            SELECT 
                i."ReportDate",
                i."NetIncome",
                i."GrossProfit",
                i."TotalRevenue",
                b."TotalAssets",
                b."LongTermDebtAndCapitalLeaseObligation" AS long_term_debt,
                b."CurrentAssets",
                b."CurrentLiabilities",
                b."CapitalStock" AS shares_proxy,
                c."TotalOperatingCashFlow" AS ocf
            FROM yearly_income_statement i
            JOIN yearly_balance_sheet b 
                ON i."Ticker" = b."Ticker" AND i."ReportDate" = b."ReportDate"
            JOIN yearly_indirect_cash_flow c 
                ON i."Ticker" = c."Ticker" AND i."ReportDate" = c."ReportDate"
            WHERE i."Ticker" = :ticker
              AND c."IsSectionValid" = TRUE  -- THE GATEKEEPER LEAK FILTER
        ),
        LaggedData AS (
            SELECT 
                "ReportDate",
                "NetIncome", "TotalAssets", "ocf", "long_term_debt", 
                "CurrentAssets", "CurrentLiabilities", "shares_proxy",
                "GrossProfit", "TotalRevenue",
                
                -- Lag 1 gets the previous year's data
                LAG("TotalAssets", 1) OVER (ORDER BY "ReportDate") AS prev_assets,
                LAG("NetIncome", 1) OVER (ORDER BY "ReportDate") AS prev_ni,
                LAG("long_term_debt", 1) OVER (ORDER BY "ReportDate") AS prev_debt,
                LAG("CurrentAssets", 1) OVER (ORDER BY "ReportDate") AS prev_ca,
                LAG("CurrentLiabilities", 1) OVER (ORDER BY "ReportDate") AS prev_cl,
                LAG("shares_proxy", 1) OVER (ORDER BY "ReportDate") AS prev_shares,
                LAG("GrossProfit", 1) OVER (ORDER BY "ReportDate") AS prev_gp,
                LAG("TotalRevenue", 1) OVER (ORDER BY "ReportDate") AS prev_rev
            FROM UnifiedData
        )
        SELECT 
            "ReportDate",
            
            -- PROFITABILITY
            CASE WHEN ("NetIncome" / NULLIF(prev_assets, 0)) > 0 THEN 1 ELSE 0 END AS f_roa,
            CASE WHEN "ocf" > 0 THEN 1 ELSE 0 END AS f_cfo,
            CASE WHEN ("NetIncome" / NULLIF(prev_assets, 0)) > ("prev_ni" / NULLIF(LAG(prev_assets, 1) OVER (ORDER BY "ReportDate"), 0)) THEN 1 ELSE 0 END AS f_droa,
            CASE WHEN "ocf" > "NetIncome" THEN 1 ELSE 0 END AS f_accrual,
            
            -- LEVERAGE & LIQUIDITY
            CASE WHEN ("long_term_debt" / NULLIF("TotalAssets", 0)) < ("prev_debt" / NULLIF("prev_assets", 0)) THEN 1 ELSE 0 END AS f_leverage,
            CASE WHEN ("CurrentAssets" / NULLIF("CurrentLiabilities", 0)) > ("prev_ca" / NULLIF("prev_cl", 0)) THEN 1 ELSE 0 END AS f_liquidity,
            CASE WHEN "shares_proxy" <= "prev_shares" THEN 1 ELSE 0 END AS f_dilution,
            
            -- EFFICIENCY
            CASE WHEN ("GrossProfit" / NULLIF("TotalRevenue", 0)) > ("prev_gp" / NULLIF("prev_rev", 0)) THEN 1 ELSE 0 END AS f_margin,
            CASE WHEN ("TotalRevenue" / NULLIF("TotalAssets", 0)) > ("prev_rev" / NULLIF("prev_assets", 0)) THEN 1 ELSE 0 END AS f_turnover
            
        FROM LaggedData
        ORDER BY "ReportDate" DESC;
    """)

    df = pd.read_sql(query, engine, params={"ticker": ticker})
    if df.empty:
        return pd.DataFrame(columns=["ReportDate", "Piotroski_F_Score"])

    score_cols = [
        "f_roa",
        "f_cfo",
        "f_droa",
        "f_accrual",
        "f_leverage",
        "f_liquidity",
        "f_dilution",
        "f_margin",
        "f_turnover",
    ]
    df["Piotroski_F_Score"] = df[score_cols].sum(axis=1)
    return df[["ReportDate", "Piotroski_F_Score"]]


def fetch_beneish_m_score(ticker: str, engine) -> pd.DataFrame:
    """
    Calculates the 8-variable Beneish M-Score using YEARLY data for Screener.
    Compares current year to the previous year (Lag 1).
    """
    query = text("""
        WITH UnifiedData AS (
            SELECT 
                i."ReportDate",
                i."TotalRevenue" AS rev,
                i."GrossProfit" AS gp,
                i."OperatingExpense" AS sga,
                i."NetIncome" AS ni,
                b."Receivables" AS rec,
                b."CurrentAssets" AS ca,
                b."CurrentLiabilities" AS cl,
                b."TotalAssets" AS ta,
                b."NetPPE" AS ppe,
                b."LongTermDebtAndCapitalLeaseObligation" AS ltd,
                c."DepreciationAndAmortization" AS dep,
                c."TotalOperatingCashFlow" AS ocf
            FROM yearly_income_statement i
            JOIN yearly_balance_sheet b 
                ON i."Ticker" = b."Ticker" AND i."ReportDate" = b."ReportDate"
            JOIN yearly_indirect_cash_flow c 
                ON i."Ticker" = c."Ticker" AND i."ReportDate" = c."ReportDate"
            WHERE i."Ticker" = :ticker
              AND c."IsSectionValid" = TRUE  -- THE LEAK FILTER
        ),
        LaggedData AS (
            SELECT 
                "ReportDate", rev, gp, sga, ni, rec, ca, cl, ta, ppe, ltd, dep, ocf,
                
                -- Lag 1 for Year-over-Year Comparisons
                LAG(rev, 1) OVER (ORDER BY "ReportDate") AS prev_rev,
                LAG(gp, 1) OVER (ORDER BY "ReportDate") AS prev_gp,
                LAG(sga, 1) OVER (ORDER BY "ReportDate") AS prev_sga,
                LAG(rec, 1) OVER (ORDER BY "ReportDate") AS prev_rec,
                LAG(ca, 1) OVER (ORDER BY "ReportDate") AS prev_ca,
                LAG(cl, 1) OVER (ORDER BY "ReportDate") AS prev_cl,
                LAG(ta, 1) OVER (ORDER BY "ReportDate") AS prev_ta,
                LAG(ppe, 1) OVER (ORDER BY "ReportDate") AS prev_ppe,
                LAG(ltd, 1) OVER (ORDER BY "ReportDate") AS prev_ltd,
                LAG(dep, 1) OVER (ORDER BY "ReportDate") AS prev_dep
            FROM UnifiedData
        ),
        Indices AS (
            SELECT 
                "ReportDate",
                -- 1. Days Sales in Receivables Index (DSRI)
                COALESCE((rec / NULLIF(rev, 0)) / NULLIF((prev_rec / NULLIF(prev_rev, 0)), 0), 1) AS dsri,
                
                -- 2. Gross Margin Index (GMI)
                COALESCE((prev_gp / NULLIF(prev_rev, 0)) / NULLIF((gp / NULLIF(rev, 0)), 0), 1) AS gmi,
                
                -- 3. Asset Quality Index (AQI)
                COALESCE((1 - ((ca + ppe) / NULLIF(ta, 0))) / NULLIF((1 - ((prev_ca + prev_ppe) / NULLIF(prev_ta, 0))), 0), 1) AS aqi,
                
                -- 4. Sales Growth Index (SGI)
                COALESCE(rev / NULLIF(prev_rev, 0), 1) AS sgi,
                
                -- 5. Depreciation Index (DEPI)
                COALESCE((prev_dep / NULLIF(prev_dep + prev_ppe, 0)) / NULLIF((dep / NULLIF(dep + ppe, 0)), 0), 1) AS depi,
                
                -- 6. SGA Expenses Index (SGAI)
                COALESCE((sga / NULLIF(rev, 0)) / NULLIF((prev_sga / NULLIF(prev_rev, 0)), 0), 1) AS sgai,
                
                -- 7. Leverage Index (LVGI)
                COALESCE(((cl + ltd) / NULLIF(ta, 0)) / NULLIF(((prev_cl + prev_ltd) / NULLIF(prev_ta, 0)), 0), 1) AS lvgi,
                
                -- 8. Total Accruals to Total Assets (TATA)
                COALESCE((ni - ocf) / NULLIF(ta, 0), 0) AS tata
                
            FROM LaggedData
        )
        SELECT 
            "ReportDate",
            ROUND(CAST(
                -4.84 
                + (0.920 * dsri) 
                + (0.528 * gmi) 
                + (0.404 * aqi) 
                + (0.892 * sgi) 
                + (0.115 * depi) 
                - (0.172 * sgai) 
                + (4.679 * tata) 
                - (0.327 * lvgi) 
            AS NUMERIC), 3) AS "Beneish_M_Score"
        FROM Indices
        ORDER BY "ReportDate" DESC;
    """)

    return pd.read_sql(query, engine, params={"ticker": ticker})
