import pandas as pd
from sqlalchemy import create_engine
from database import engine as duckdb_proxy

PG_URI = "postgresql://postgres:123456@localhost:5432/postgres"
pg_conn = create_engine(PG_URI)

migration_map = {
    # "global_assets_daily": ["Ticker", "ReportDate"],
    # "global_assets_intraday": ["Ticker", "ReportDate", "Timeframe"],
    # "macro_daily_ledger": ["IndicatorName", "ReportDate"],
    # "macro_intraday_ledger": ["IndicatorName", "ReportDate", "Timeframe"],
    # "market_metadata": ["Ticker"],
    "quarterly_income_statement": ["Ticker", "ReportDate"],
    "yearly_income_statement": ["Ticker", "ReportDate"],
    "quarterly_balance_sheet": ["Ticker", "ReportDate"],
    "yearly_balance_sheet": ["Ticker", "ReportDate"],
    "quarterly_cash_flow": ["Ticker", "ReportDate"],
    "yearly_cash_flow": ["Ticker", "ReportDate"],
    "yearly_indirect_cash_flow": ["Ticker", "ReportDate"],
}


def run_migration():
    print("[*] Initiating PostgreSQL -> DuckDB Migration...\n")

    for table, pk_list in migration_map.items():
        print(f"[*] Extracting {table} from PostgreSQL...")

        try:
            # 1. Read from Old DB
            df = pd.read_sql(f'SELECT * FROM "{table}"', pg_conn)

            if df.empty:
                print(f"  [-] Table is empty. Skipping.\n")
                continue

            print(f"  [+] Extracted {len(df)} rows. Pushing to DuckDB Arrow Memory...")

            # 2. Register Zero-Copy View
            view_name = f"temp_pg_{table}"
            duckdb_proxy.register(view_name, df)

            # 3. Dynamically construct exact column matches
            all_columns = df.columns.tolist()

            # Format PKs for the ON CONFLICT clause: ("Col1", "Col2")
            conflict_keys = "(" + ", ".join([f'"{pk}"' for pk in pk_list]) + ")"

            # Auto-generate UPDATE SET ignoring Primary Keys
            update_cols = [col for col in all_columns if col not in pk_list]
            set_clause = ", ".join([f'"{col}"=EXCLUDED."{col}"' for col in update_cols])

            # If the table ONLY has primary keys, handle gracefully
            if not update_cols:
                set_clause = f'"{pk_list[0]}"=EXCLUDED."{pk_list[0]}"'

            columns_str = ", ".join([f'"{col}"' for col in all_columns])

            query = f"""
                INSERT INTO {table} ({columns_str})
                SELECT * FROM {view_name}
                ON CONFLICT {conflict_keys} 
                DO UPDATE SET {set_clause};
            """

            duckdb_proxy.execute(query)
            duckdb_proxy.unregister(view_name)

            print(f"  [SUCCESS] {table} migration complete.\n")

        except Exception as e:
            print(f"  [ERROR] Failed to migrate {table}: {e}\n")
            if hasattr(duckdb_proxy, "_active_write_con"):
                try:
                    duckdb_proxy.unregister(f"temp_pg_{table}")
                except:
                    pass


if __name__ == "__main__":
    run_migration()
