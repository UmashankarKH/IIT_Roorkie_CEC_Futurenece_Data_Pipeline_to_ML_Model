"""ETL Load: write clean tables into DuckDB. Safe to run many times (idempotent)."""
import duckdb

from include.nammamart_config import WAREHOUSE


def load_dim(df, table):
    """Dimensions are small: rebuild them every run."""
    con = duckdb.connect(str(WAREHOUSE))
    con.register("src", df)
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM src")
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.close()
    return n


def load_fact(df, table="etl_fact_orders"):
    """Fact: delete any rows with the same order_id, then insert. No duplicates, ever."""
    con = duckdb.connect(str(WAREHOUSE))
    con.register("src", df)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM src WHERE 1=0")
    con.execute(f"DELETE FROM {table} WHERE order_id IN (SELECT order_id FROM src)")
    con.execute(f"INSERT INTO {table} SELECT * FROM src")
    n = len(df)   # rows written by THIS run
    con.close()
    return n

def bad_order_ids(failed, passed):
    """Order ids that failed this run and have no passing copy (a duplicate keeps its first copy)."""
    if failed is None or len(failed) == 0 or "order_id" not in failed.columns:
        return set()
    ids = set(failed["order_id"].astype(str).str.strip())
    ids -= set(passed["order_id"].astype(str).str.strip())
    return ids - {""}


def purge_fact(order_ids, table="etl_fact_orders"):
    """Delete fact rows whose order no longer passes validation, so the fact only holds clean rows."""
    import pandas as pd
    if not order_ids:
        return 0
    con = duckdb.connect(str(WAREHOUSE))
    try:
        con.register("bad_ids", pd.DataFrame({"order_id": sorted(order_ids)}))
        where = "WHERE order_id IN (SELECT order_id FROM bad_ids)"
        n = con.execute(f"SELECT count(*) FROM {table} {where}").fetchone()[0]
        con.execute(f"DELETE FROM {table} {where}")
    finally:
        con.close()
    return n
