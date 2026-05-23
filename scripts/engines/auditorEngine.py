import pandas as pd
from datetime import datetime
from sqlalchemy import text, MetaData, Table, Column, String, Date, Float, Boolean
from sqlalchemy.dialects.postgresql import insert
from scripts.database import engine

# -----------------------------------------------------------------
# 1. DATABASE SCHEMA CONTRACT
# -----------------------------------------------------------------
metadata = MetaData()
validation_ledger = Table(
    "validation_ledger",
    metadata,
    Column("engine_name", String(50), primary_key=True),
    Column("ticker", String(50), primary_key=True),
    Column("prediction_date", Date, primary_key=True),
    Column("horizon", String(10), primary_key=True),
    Column("signal", String(20)),
    Column("expected_return", Float),
    Column("realized_return", Float),
    Column("variance_error", Float),
    Column("is_directional_hit", Boolean),
    Column("veto_flagged", Boolean),
    Column("veto_success", Boolean),
    Column("audit_date", Date),
)


class SystemicAuditor:
    def __init__(self):
        # Ensure the table exists before writing
        metadata.create_all(engine, tables=[validation_ledger])
        self.horizon_map = {"2D": 2, "5D": 5, "20D": 20}

    def fetch_unvalidated_predictions(self) -> pd.DataFrame:
        """
        Fetches predictions from the ledger where enough time has passed
        for the horizon to mature, but no validation record exists yet.
        """
        query = text("""
            SELECT p.engine_name, p.ticker, p.asof_date AS prediction_date, 
                   p.horizon, p.signal, p.score AS expected_return, p.veto_flag
            FROM prediction_ledger p
            LEFT JOIN validation_ledger v 
              ON p.engine_name = v.engine_name 
             AND p.ticker = v.ticker 
             AND p.asof_date = v.prediction_date 
             AND p.horizon = v.horizon
            WHERE v.ticker IS NULL
              AND p.asof_date <= CURRENT_DATE - INTERVAL '30 days' -- Buffer for maximum horizon
        """)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)

    def fetch_realized_price_path(
        self, ticker: str, start_date: str, days: int
    ) -> pd.DataFrame:
        """
        Queries the unified matrix strictly for the actual trading days following a prediction.
        """
        query = text("""
            SELECT date, close
            FROM unified_market_matrix
            WHERE ticker = :ticker AND date >= :start_date
            ORDER BY date ASC
            LIMIT :limit
        """)
        with engine.connect() as conn:
            # We fetch days + 1 because the first row is T+0 (the prediction date)
            return pd.read_sql(
                query,
                conn,
                params={"ticker": ticker, "start_date": start_date, "limit": days + 1},
            )

    def run_audit_cycle(self):
        """
        Main execution loop. Processes open predictions and writes the variance logic.
        """
        df_pending = self.fetch_unvalidated_predictions()
        if df_pending.empty:
            print("[*] Auditor: No pending mature predictions found.")
            return

        print(
            f"[*] Auditor: Processing {len(df_pending)} pending historical signals..."
        )

        with engine.begin() as conn:
            for _, row in df_pending.iterrows():
                ticker = row["ticker"]
                p_date = row["prediction_date"].strftime("%Y-%m-%d")
                horizon_label = row["horizon"]
                target_days = self.horizon_map.get(horizon_label, 0)

                # 1. Fetch exact trading day path
                path_df = self.fetch_realized_price_path(ticker, p_date, target_days)

                # Check if the horizon has actually completed in market-days
                if len(path_df) < target_days + 1:
                    continue

                # 2. Extract Prices
                p0 = path_df.iloc[0]["close"]
                pT = path_df.iloc[-1]["close"]

                if p0 == 0:
                    continue

                # 3. Calculate Variance and Hit Rates
                # 3. Calculate Variance and Hit Rates (Casted strictly to Python native primitives)
                realized_return = float((pT - p0) / p0)
                expected_return = float(row["expected_return"])
                variance_error = float(realized_return - expected_return)

                # Directional Hit Logic: Did price move in the predicted direction?
                is_hit = False
                if expected_return > 0 and realized_return > 0:
                    is_hit = True
                elif expected_return < 0 and realized_return < 0:
                    is_hit = True
                elif row["signal"] == "WATCH" or row["signal"] == "AVOID":
                    is_hit = bool(realized_return <= 0.01)

                # Veto Success Logic: If a macro veto downgraded a BUY, was that the right call?
                veto_success = None
                if row["veto_flag"]:
                    veto_success = bool(realized_return <= 0.0)

                # 4. Construct Payload
                payload = {
                    "engine_name": row["engine_name"],
                    "ticker": ticker,
                    "prediction_date": row["prediction_date"],
                    "horizon": horizon_label,
                    "signal": row["signal"],
                    "expected_return": expected_return,
                    "realized_return": realized_return,
                    "variance_error": variance_error,
                    "is_directional_hit": is_hit,
                    "veto_flagged": row["veto_flag"],
                    "veto_success": veto_success,
                    "audit_date": datetime.now().date(),
                }

                # 5. Execute UPSERT
                stmt = insert(validation_ledger).values(**payload)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        "engine_name",
                        "ticker",
                        "prediction_date",
                        "horizon",
                    ],
                    set_=payload,
                )
                conn.execute(stmt)

        print("[+] Auditor: Validation cycle complete. Ledger updated.")


if __name__ == "__main__":
    auditor = SystemicAuditor()
    auditor.run_audit_cycle()
