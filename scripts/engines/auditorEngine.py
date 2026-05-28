import pandas as pd
from datetime import datetime
import json

# strictly import the native DuckDB connection
from scripts.database import engine


class SystemicAuditor:
    def __init__(self):
        # 1. Native DuckDB DDL: Replaces SQLAlchemy MetaData and Table definitions.
        # Enforces the physical table structure for the validation ledger natively.
        engine.execute("""
            CREATE TABLE IF NOT EXISTS validation_ledger (
                "engine_name" VARCHAR,
                "ticker" VARCHAR,
                "prediction_date" DATE,
                "horizon" VARCHAR,
                "signal" VARCHAR,
                "expected_return" DOUBLE,
                "realized_return" DOUBLE,
                "variance_error" DOUBLE,
                "is_directional_hit" BOOLEAN,
                "veto_flagged" BOOLEAN,
                "veto_success" BOOLEAN,
                "audit_date" DATE,
                PRIMARY KEY ("engine_name", "ticker", "prediction_date", "horizon")
            );
        """)

        # Mapping horizon string representations to actual trading days
        self.horizon_map = {"2D": 2, "5D": 5, "20D": 20}

    def fetch_unvalidated_predictions(self) -> pd.DataFrame:
        """
        Fetches predictions from the ledger where enough time has passed
        for the horizon to mature, but no validation record exists yet.
        """
        # 2. Replaced text() wrapper with raw SQL string.
        query = """
            SELECT p.engine_name, p.ticker, p.asof_date AS prediction_date, 
                   p.horizon, p.signal, p.target_metric, p.veto_flag
            FROM prediction_ledger p
            LEFT JOIN validation_ledger v
              ON p.engine_name = v.engine_name
             AND p.ticker = v.ticker
             AND p.asof_date = v.prediction_date
             AND p.horizon = v.horizon
            WHERE v.ticker IS NULL
        """
        # 3. Native zero-copy Arrow extraction
        return engine.execute(query).df()

    def fetch_realized_price_path(
        self, ticker: str, start_date: str, days_forward: int
    ) -> pd.DataFrame:
        """
        Extracts the actual closing price sequence for the specific number
        of trading days following a prediction.
        """
        # 4. Switched :named_params to native DuckDB $positional_params
        query = """
            SELECT date, close
            FROM unified_market_matrix
            WHERE ticker = $ticker AND date >= CAST($start_date AS DATE)
            ORDER BY date ASC
            LIMIT $limit
        """

        # limit is days_forward + 1 because the first row is T+0 (the prediction date)
        return engine.execute(
            query,
            {"ticker": ticker, "start_date": start_date, "limit": days_forward + 1},
        ).df()

    def run_audit_cycle(self):
        """
        Main execution loop. Identifies mature predictions, fetches the
        realized reality, grades the engine's performance, and stores the audit.
        """
        print("[*] Auditor: Initiating Systemic Validation Cycle...")
        pending_df = self.fetch_unvalidated_predictions()

        if pending_df.empty:
            print("  [-] No mature predictions require auditing at this time.")
            return

        print(
            f"  [+] Found {len(pending_df)} un-audited prediction(s). Verifying reality paths..."
        )

        for _, row in pending_df.iterrows():
            ticker = row["ticker"]
            horizon_label = row["horizon"]
            days_required = self.horizon_map.get(horizon_label, 5)

            # Fetch the actual market reality
            reality_df = self.fetch_realized_price_path(
                ticker, str(row["prediction_date"]), days_required
            )

            # Only grade the prediction if the full horizon has mathematically elapsed
            if len(reality_df) >= days_required + 1:
                p_0 = reality_df.iloc[0]["close"]
                p_n = reality_df.iloc[-1]["close"]

                # 1. The Realized Math
                realized_return = (p_n - p_0) / p_0

                # 2. Extracting Expected Reality from the JSON storage
                try:
                    # DuckDB returns native JSON as python dicts when using .df()
                    target_data = (
                        json.loads(row["target_metric"])
                        if isinstance(row["target_metric"], str)
                        else row["target_metric"]
                    )
                    expected_return = float(target_data.get("expected_return", 0.0))
                except Exception:
                    expected_return = 0.0

                variance_error = realized_return - expected_return

                # 3. Grading Directional Logic
                is_hit = False
                if row["signal"] == "BUY" and realized_return > 0:
                    is_hit = True
                elif row["signal"] == "SHORT-BIAS" and realized_return < 0:
                    is_hit = True
                elif row["signal"] == "AVOID" and abs(realized_return) < 0.02:
                    # AVOID is considered a hit if the stock basically chopped around
                    is_hit = True

                # Grading the Veto System: If the AI Vetoed a BUY, was that the right call?
                veto_success = None
                if row["veto_flag"]:
                    veto_success = bool(realized_return <= 0.0)

                # 5. Native DuckDB UPSERT: Replacing SQLAlchemy insert().on_conflict()
                engine.execute(
                    """
                    INSERT INTO validation_ledger (
                        "engine_name", "ticker", "prediction_date", "horizon", "signal", 
                        "expected_return", "realized_return", "variance_error", 
                        "is_directional_hit", "veto_flagged", "veto_success", "audit_date"
                    )
                    VALUES ($1, $2, CAST($3 AS DATE), $4, $5, $6, $7, $8, CAST($9 AS BOOLEAN), CAST($10 AS BOOLEAN), CAST($11 AS BOOLEAN), CAST($12 AS DATE))
                    ON CONFLICT ("engine_name", "ticker", "prediction_date", "horizon")
                    DO UPDATE SET
                        "realized_return" = EXCLUDED."realized_return",
                        "variance_error" = EXCLUDED."variance_error",
                        "is_directional_hit" = EXCLUDED."is_directional_hit",
                        "veto_success" = EXCLUDED."veto_success",
                        "audit_date" = EXCLUDED."audit_date";
                """,
                    [
                        row["engine_name"],
                        ticker,
                        str(row["prediction_date"]),
                        horizon_label,
                        row["signal"],
                        expected_return,
                        realized_return,
                        variance_error,
                        is_hit,
                        row["veto_flag"],
                        veto_success,
                        str(datetime.now().date()),
                    ],
                )

        print("[+] Auditor: Validation cycle complete. Ledger updated.")


if __name__ == "__main__":
    auditor = SystemicAuditor()
    auditor.run_audit_cycle()
