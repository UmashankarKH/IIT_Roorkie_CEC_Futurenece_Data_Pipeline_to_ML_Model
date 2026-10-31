"""ELT raw layer: exact copy of each CSV (all text) + _loaded_at, _source_file, _run_id.
Raw is append-only history. Re-running the SAME run_id replaces only that run's rows (safe retry)."""
import duckdb

from include.nammamart_config import DATA_DIR, WAREHOUSE

RAW_TABLES = {
    "raw_orders": "orders.csv",
    "raw_payments": "payments.csv",
    "raw_customers": "customers.csv",
    "raw_products": "products.csv",
    "raw_stores": "stores.csv",
}


def load_raw(table, csv_name, run_id):
    path = str(DATA_DIR / csv_name).replace("'", "''")
    rid = run_id.replace("'", "''")
    # nullstr is a sentinel so blank cells stay as '' exactly as received
    source = (f"SELECT *, now()::TIMESTAMP AS _loaded_at, '{csv_name}' AS _source_file, "
              f"'{rid}' AS _run_id FROM read_csv('{path}', header=true, all_varchar=true, "
              f"nullstr='__never_null__')")
    con = duckdb.connect(str(WAREHOUSE))
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS {source} LIMIT 0")
    con.execute(f"DELETE FROM {table} WHERE _run_id = '{rid}' AND _source_file = '{csv_name}'")
    con.execute(f"INSERT INTO {table} {source}")
    n = con.execute(f"SELECT COUNT(*) FROM {table} WHERE _run_id = '{rid}' "
                    f"AND _source_file = '{csv_name}'").fetchone()[0]
    con.close()
    return n