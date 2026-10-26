from __future__ import annotations

import json
import os
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task

DATA_DIR = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_jayaprakash.duckdb"
SQL_DIR = "/usr/local/airflow/include/sql"


def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def _sql_file(name: str) -> str:
    with open(os.path.join(SQL_DIR, name), "r", encoding="utf-8") as f:
        return f.read()


@dag(
    dag_id="nammamart_elt_jayaprakash",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        "owner": "jayaprakash",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["nammamart", "elt", "assignment"],
)
def nammamart_elt_jayaprakash():

    @task
    def extract_raw_csvs():
        payload = {}
        for name in ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv"]:
            frame = _read_csv(os.path.join(DATA_DIR, name))
            payload[name] = frame.to_dict(orient="records")
        return json.dumps(payload)

    @task
    def load_raw_tables(payload_json):
        import duckdb
        con = duckdb.connect(WAREHOUSE_DB)
        payload = json.loads(payload_json)
        for name, rows in payload.items():
            table_name = "raw_" + name.replace(".csv", "")
            df = pd.DataFrame.from_records(rows)
            con.register("raw_df", df)
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT *, CURRENT_TIMESTAMP AS _loaded_at, '{name}' AS _source_file, 'elt_run' AS _run_id FROM raw_df")
        con.close()
        return "raw tables loaded"

    @task
    def run_staging_sql():
        import duckdb
        con = duckdb.connect(WAREHOUSE_DB)
        sql = _sql_file("nammamart_staging.sql")
        con.execute(sql)
        con.close()
        return "staging ready"

    @task
    def run_incremental_sql():
        import duckdb
        con = duckdb.connect(WAREHOUSE_DB)
        df = _read_csv(os.path.join(DATA_DIR, "orders_2026-10-01_increment.csv"))
        con.register("increment_df", df)
        con.execute("CREATE TABLE IF NOT EXISTS raw_orders_increment AS SELECT *, CURRENT_TIMESTAMP AS _loaded_at, 'orders_2026-10-01_increment.csv' AS _source_file, 'elt_run' AS _run_id FROM increment_df")
        con.execute("MERGE INTO stg_orders AS tgt USING raw_orders_increment AS src ON tgt.order_id = src.order_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        con.close()
        return "increment applied"

    @task
    def run_mart_sql():
        import duckdb
        con = duckdb.connect(WAREHOUSE_DB)
        sql = _sql_file("nammamart_marts.sql")
        con.execute(sql)
        con.close()
        return "mart tables built"

    raw_payload = extract_raw_csvs()
    raw_tables = load_raw_tables(raw_payload)
    staging = run_staging_sql()
    increment = run_incremental_sql()
    marts = run_mart_sql()

    raw_payload >> raw_tables >> staging >> increment >> marts


nammamart_elt_jayaprakash()
