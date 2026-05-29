import pandas as pd
from sqlalchemy import create_engine
from database import engine as duckdb_proxy

# --- CONFIGURATION ---
# Replace with your old PostgreSQL connection string
PG_URI = "postgresql://postgres:123456@localhost:5432/postgres"
pg_conn = create_engine(PG_URI)

# Exact mapping based on the provided SQLAlchemy tables
migration_map = {
    "global_assets_daily": {
        "conflict_keys": '("Ticker", "ReportDate")',
        "update_set": '"AssetClass"=EXCLUDED."AssetClass", "Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close"=EXCLUDED."Close", "Volume"=EXCLUDED."Volume"',
    },
    "global_assets_intraday": {
        "conflict_keys": '("Ticker", "ReportDate", "Timeframe")',
        "update_set": '"Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close"=EXCLUDED."Close", "Volume"=EXCLUDED."Volume"',
    },
    "macro_daily_ledger": {
        "conflict_keys": '("IndicatorName", "ReportDate")',
        "update_set": '"Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close_Value"=EXCLUDED."Close_Value", "Volume"=EXCLUDED."Volume"',
    },
    "macro_intraday_ledger": {
        "conflict_keys": '("IndicatorName", "ReportDate", "Timeframe")',
        "update_set": '"Open"=EXCLUDED."Open", "High"=EXCLUDED."High", "Low"=EXCLUDED."Low", "Close_Value"=EXCLUDED."Close_Value", "Volume"=EXCLUDED."Volume"',
    },
}


def run_migration():
    print("[*] Initiating PostgreSQL -> DuckDB Migration...\n")

    for table, config in migration_map.items():
        print(f"[*] Extracting {table} from PostgreSQL...")

        try:
            # 1. Read from Old DB
            df = pd.read_sql(f'SELECT * FROM "{table}"', pg_conn)

            if df.empty:
                print(f"  [-] Table is empty. Skipping.\n")
                continue

            print(f"  [+] Extracted {len(df)} rows. Pushing to DuckDB Arrow Memory...")

            # 2. Register Zero-Copy View via our Proxy
            view_name = f"temp_pg_{table}"
            duckdb_proxy.register(view_name, df)

            # 3. Native DuckDB Columnar Upsert
            # Explicitly wrapping columns in quotes to protect exact casing
            columns = ", ".join([f'"{col}"' for col in df.columns])

            query = f"""
                INSERT INTO {table} ({columns})
                SELECT * FROM {view_name}
                ON CONFLICT {config['conflict_keys']} 
                DO UPDATE SET {config['update_set']};
            """

            duckdb_proxy.execute(query)
            duckdb_proxy.unregister(view_name)

            print(f"  [SUCCESS] {table} migration complete.\n")

        except Exception as e:
            print(f"  [ERROR] Failed to migrate {table}: {e}\n")
            if hasattr(duckdb_proxy, "_active_write_con"):
                duckdb_proxy.unregister(f"temp_pg_{table}")


if __name__ == "__main__":
    run_migration()
