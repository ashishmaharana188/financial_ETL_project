import pandas as pd
import numpy as np
import statsmodels.api as sm
import json
from datetime import datetime
from scripts.database import engine


class OLSMicrostructureEngine:
    def __init__(self, engine_name="1_OLS_Microstructure"):
        self.engine_name = engine_name

    def fetch_historical_matrix(self, ticker: str, asof_date: str) -> pd.DataFrame:
        """
        Fetches historical daily matrix containing price, volume, and lagged bulk metrics
        (delivery, open interest, options PCR, futures basis) up to the asof_date.
        """
        # Native DuckDB syntax using positional parameter binding
        query = """
            SELECT date, close, volume, delivery_percentage, oi_pcr, futures_basis
            FROM unified_market_matrix
            WHERE ticker = $ticker AND date <= CAST($asof_date AS DATE)
            ORDER BY date ASC
            LIMIT 500
        """
        # Zero-copy Arrow extraction directly into Pandas
        df = engine.execute(query, {"ticker": ticker, "asof_date": asof_date}).df()

        numeric_cols = [
            "close",
            "volume",
            "delivery_percentage",
            "oi_pcr",
            "futures_basis",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
            df.sort_index(inplace=True)

        return df

    def calculate_institutional_footprint(self, ticker: str, asof_date: str) -> float:
        """
        Estimates macro-level FII/DII involvement on the date of execution.
        """
        query = """
            SELECT "ReportDate", "Volume"
            FROM macro_daily_ledger
            WHERE "IndicatorName" = 'FII_DII_Net' AND "ReportDate" <= CAST($asof_date AS DATE)
            ORDER BY "ReportDate" DESC
            LIMIT 10
        """
        df = engine.execute(query, {"asof_date": asof_date}).df()

        if df.empty:
            return 0.0

        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
        return float(df["Volume"].mean())

    def fetch_macro_regime(self, asof_date: str) -> dict:
        """
        Identifies broader market conditions on the target execution date.
        """
        vix_query = """
            SELECT "ReportDate", "Close_Value"
            FROM macro_daily_ledger
            WHERE "IndicatorName" = 'INDIAVIX' AND "ReportDate" <= CAST($asof_date AS DATE)
            ORDER BY "ReportDate" DESC LIMIT 252
        """
        breadth_query = """
            SELECT "Ticker", "Close", "Open"
            FROM global_assets_daily
            WHERE "ReportDate" = CAST($asof_date AS DATE) AND "AssetClass" = 'Equity'
        """
        vix_df = engine.execute(vix_query, {"asof_date": asof_date}).df()
        breadth_df = engine.execute(breadth_query, {"asof_date": asof_date}).df()

        regime = {"vix_percentile": 0.5, "advances_declines_ratio": 1.0}

        if not vix_df.empty:
            current_vix = vix_df.iloc[0]["Close_Value"]
            vix_percentile = (vix_df["Close_Value"] < current_vix).mean()
            regime["vix_percentile"] = float(vix_percentile)

        if not breadth_df.empty:
            advances = (breadth_df["Close"] > breadth_df["Open"]).sum()
            declines = (breadth_df["Close"] < breadth_df["Open"]).sum()
            if declines > 0:
                regime["advances_declines_ratio"] = float(advances / declines)

        return regime

    def train_and_score(self, df: pd.DataFrame) -> dict:
        """
        Builds the localized regression model.
        """
        if len(df) < 50:
            return None

        # 1. Feature Engineering
        df["log_ret"] = np.log(df["close"] / df["close"].shift(1))
        df["target_2d"] = df["close"].shift(-2) / df["close"] - 1
        df["target_5d"] = df["close"].shift(-5) / df["close"] - 1

        df["vol_ratio"] = df["volume"] / df["volume"].rolling(20).mean()
        df["del_ma"] = df["delivery_percentage"].rolling(10).mean()
        df["del_spike"] = df["delivery_percentage"] / df["del_ma"]

        df["pcr_change"] = df["oi_pcr"].diff(3)
        df["basis_trend"] = df["futures_basis"].rolling(5).mean()

        df.dropna(inplace=True)
        if len(df) < 40:
            return None

        # Extract features for the most recent day (T=0)
        current_features = df.iloc[-1].to_dict()

        # 2. OLS Modeling targeting T+2
        features = ["log_ret", "vol_ratio", "del_spike", "pcr_change", "basis_trend"]
        X = df[features]
        X = sm.add_constant(X)
        y = df["target_2d"]

        try:
            model = sm.OLS(y, X).fit()
            X_latest = pd.DataFrame([df.iloc[-1][features]])
            X_latest = sm.add_constant(X_latest, has_constant="add")

            # Ensure constant aligns with model params
            missing_cols = set(model.params.index) - set(X_latest.columns)
            for col in missing_cols:
                X_latest[col] = 1.0

            predicted_return_2d = float(model.predict(X_latest.iloc[0])[0])
            r_squared = float(model.rsquared)

            # Map mathematical return to normalized [-1, 1] score
            score = max(min(predicted_return_2d / 0.05, 1.0), -1.0)
            confidence = r_squared

            return {
                "score": score,
                "confidence": confidence,
                "expected_return": predicted_return_2d,
                "latest_features": current_features,
            }
        except Exception:
            return None

    def execute_pipeline(self, ticker: str, asof_date: str = None):
        """
        Main orchestrator.
        """
        if not asof_date:
            asof_date = datetime.now().strftime("%Y-%m-%d")

        df = self.fetch_historical_matrix(ticker, asof_date)
        if df is None or df.empty:
            return

        model_results = self.train_and_score(df)
        if not model_results:
            return

        inst_flow = self.calculate_institutional_footprint(ticker, asof_date)
        macro_regime = self.fetch_macro_regime(asof_date)

        # Merge isolated and macro features for JSON logging
        final_features = model_results["latest_features"]
        final_features["institutional_flow"] = inst_flow
        final_features["vix_percentile"] = macro_regime["vix_percentile"]

        # Veto & Penalty System
        veto = False
        penalty = 0.0
        reasons = []

        if macro_regime["vix_percentile"] > 0.85:
            penalty -= 0.2
            reasons.append("Extreme Market Volatility (VIX > 85th Percentile)")

        if final_features["pcr_change"] < -0.2:
            penalty -= 0.3
            reasons.append("Aggressive Put unwinding / Call writing detected")

        if final_features["del_spike"] < 0.5:
            penalty -= 0.1
            reasons.append("Severely low delivery footprint vs historical average")

        final_score = model_results["score"] + penalty

        # Discretize the signal based on adjusted scores
        signal = "WATCH"
        if final_score > 0.4:
            signal = "BUY"
        elif final_score < -0.4:
            signal = "SHORT-BIAS"
            veto = True
            reasons.append("Systemic VETO: Short-Bias override.")
        elif -0.4 <= final_score <= 0.1:
            signal = "AVOID"

        # Horizon Map Output
        predictions = [
            {
                "horizon": "2D",
                "signal": signal,
                "score": final_score,
                "confidence": model_results["confidence"],
                "expected_return": model_results["expected_return"],
                "veto": veto,
                "penalty": penalty,
                "reasons": reasons,
            }
        ]

        self.store_predictions(ticker, asof_date, predictions, final_features)

    def store_predictions(
        self, ticker: str, asof_date: str, predictions: list, feature_json: dict
    ):
        """
        Native DuckDB Upsert logging the model's output and explicit internal state.
        """
        for pred in predictions:
            engine.execute(
                """
                INSERT INTO prediction_ledger 
                ("engine_name", "ticker", "asof_date", "horizon", "signal", "score", 
                 "confidence", "veto_flag", "penalty", "target_metric", "reason_json", "feature_json")
                VALUES ($1, $2, CAST($3 AS DATE), $4, $5, $6, $7, CAST($8 AS BOOLEAN), $9, $10, CAST($11 AS JSON), CAST($12 AS JSON))
                ON CONFLICT ("engine_name", "ticker", "asof_date", "horizon")
                DO UPDATE SET
                    "signal" = EXCLUDED."signal",
                    "score" = EXCLUDED."score",
                    "confidence" = EXCLUDED."confidence",
                    "veto_flag" = EXCLUDED."veto_flag",
                    "penalty" = EXCLUDED."penalty",
                    "target_metric" = EXCLUDED."target_metric",
                    "reason_json" = EXCLUDED."reason_json",
                    "feature_json" = EXCLUDED."feature_json";
            """,
                [
                    self.engine_name,
                    ticker,
                    asof_date,
                    pred["horizon"],
                    pred["signal"],
                    pred["score"],
                    pred["confidence"],
                    pred["veto"],
                    pred["penalty"],
                    json.dumps({"expected_return": pred["expected_return"]}),
                    json.dumps(pred["reasons"]),
                    json.dumps(feature_json),
                ],
            )


def run_mass_historical_backfill(days_depth=60):
    """
    Used only once during setup to populate the prediction_ledger backward in time
    so the dashboard has a historical timeline to map against realized returns.
    """
    # 1. Fetch active targets directly via DuckDB
    tickers_df = engine.execute(
        'SELECT "Ticker" FROM market_metadata WHERE "IsActive" = true'
    ).df()
    tickers = tickers_df["Ticker"].tolist()

    if not tickers:
        print("[ERROR] No active tickers discovered in market_metadata.")
        return

    # 2. Generate date array moving backward from today
    today = datetime.now()
    date_list = [
        (today - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, days_depth + 1)
    ]

    print(
        f"[START] Initiating mass backfill for {len(tickers)} stocks over {days_depth} days ({len(date_list)} execution dates)..."
    )

    pipeline = OLSMicrostructureEngine()

    for target_date in date_list:
        day_of_week = datetime.strptime(target_date, "%Y-%m-%d").weekday()
        if day_of_week >= 5:  # Skip weekends
            continue

        print(f"\\n--- Processing Timeline Snapshot: {target_date} ---")
        for ticker in tickers:
            try:
                pipeline.execute_pipeline(ticker=ticker, asof_date=target_date)
            except Exception as e:
                print(f"[SKIP] Pipeline error for {ticker} on {target_date}: {e}")


if __name__ == "__main__":
    run_mass_historical_backfill(days_depth=60)
