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
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.close()
    return n