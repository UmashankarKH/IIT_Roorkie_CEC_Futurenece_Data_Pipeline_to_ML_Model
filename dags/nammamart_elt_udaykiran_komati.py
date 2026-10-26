"""
NammaMart ELT pipeline — load raw CSVs, transform in DuckDB with SQL.
"""

import glob
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/usr/local/airflow/include")

import duckdb
import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from nammamart_audit import ensure_audit_tables, write_pipeline_run
from nammamart_config import (
    DATA_DIR,
    ELT_DAG_ID,
    INCREMENT_FILE,
    MASTER_FILES,
    SCHEDULE_CRON,
    SQL_DIR,
    TIMEZONE,
    WAREHOUSE_DB,
)
default_args = {
    "owner": "udaykiran.komati",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _run_id() -> str:
    return pendulum.now(TIMEZONE).format("YYYYMMDD_HHmmss")


def _run_sql_file(con: duckdb.DuckDBPyConnection, path: str) -> None:
    with open(path, "r", encoding="utf-8") as handle:
        sql = handle.read()
    print(f"[SQL] running {os.path.basename(path)}")
    con.execute(sql)


@dag(
    dag_id=ELT_DAG_ID,
    schedule=SCHEDULE_CRON,
    start_date=pendulum.datetime(2026, 9, 1, tz=TIMEZONE),
    catchup=False,
    default_args=default_args,
    tags=["fde", "nammamart", "elt"],
    doc_md="NammaMart ELT: extract → load raw → SQL staging → SQL marts.",
)
def nammamart_elt_udaykiran_komati():
    @task
    def load_raw_tables() -> str:
        run_id = _run_id()
        loaded_at = datetime.utcnow().isoformat()
        con = duckdb.connect(WAREHOUSE_DB)
        ensure_audit_tables(con)

        raw_specs = [
            ("raw_orders", MASTER_FILES["orders"]),
            ("raw_payments", MASTER_FILES["payments"]),
            ("raw_customers", MASTER_FILES["customers"]),
            ("raw_products", MASTER_FILES["products"]),
            ("raw_stores", MASTER_FILES["stores"]),
        ]

        rows_extracted = 0
        for table, filename in raw_specs:
            path = os.path.join(DATA_DIR, filename)
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} AS
                SELECT *, NULL::VARCHAR AS _loaded_at, NULL::VARCHAR AS _source_file, NULL::VARCHAR AS _run_id
                FROM read_csv_auto('{path}', all_varchar=true, header=true)
                WHERE 1=0
                """
            )
            con.execute(
                f"""
                INSERT INTO {table}
                SELECT *, ?, ?, ?
                FROM read_csv_auto('{path}', all_varchar=true, header=true)
                """,
                [loaded_at, filename, run_id],
            )
            count = con.execute(f"SELECT count(*) FROM read_csv_auto('{path}', header=true)").fetchone()[0]
            rows_extracted += count
            print(f"[Raw] {table}: appended {count} rows from {filename}")

        con.close()
        return f"{run_id}|{rows_extracted}"

    @task
    def load_increment_raw(meta: str) -> str:
        run_id = meta.split("|")[0]
        path = os.path.join(DATA_DIR, INCREMENT_FILE)
        if not os.path.exists(path):
            print("[Raw increment] file missing, skipping")
            return meta

        loaded_at = datetime.utcnow().isoformat()
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute(
            f"""
            INSERT INTO raw_orders
            SELECT *, ?, ?, ?
            FROM read_csv_auto('{path}', all_varchar=true, header=true)
            """,
            [loaded_at, INCREMENT_FILE, run_id],
        )
        count = con.execute(f"SELECT count(*) FROM read_csv_auto('{path}', header=true)").fetchone()[0]
        con.close()
        print(f"[Raw increment] appended {count} rows (MERGE handled in staging)")
        return meta

    @task
    def run_staging(meta: str) -> str:
        con = duckdb.connect(WAREHOUSE_DB)
        for name in ("stg_stores", "stg_products", "stg_customers", "stg_orders", "stg_payments"):
            _run_sql_file(con, os.path.join(SQL_DIR, f"{name}.sql"))
        con.close()
        return meta

    @task
    def run_marts(meta: str) -> str:
        con = duckdb.connect(WAREHOUSE_DB)
        for name in (
            "mart_daily_store_kpis",
            "mart_category_margin",
            "mart_delivery_sla",
            "mart_payment_reconciliation",
            "mart_top_customers",
        ):
            _run_sql_file(con, os.path.join(SQL_DIR, f"{name}.sql"))
        _run_sql_file(con, os.path.join(SQL_DIR, "elt_dq_results.sql"))

        sample = con.execute(
            "SELECT zone, round(sum(net_revenue),2) FROM mart_daily_store_kpis GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        print("[Mart] zone revenue:", sample)
        con.close()
        return meta

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def pipeline_run_audit(meta: str) -> None:
        run_id, rows_extracted = meta.split("|")
        con = duckdb.connect(WAREHOUSE_DB)
        rows_loaded = con.execute(
            "SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'"
        ).fetchone()[0]
        rows_quarantined = con.execute(
            "SELECT count(*) FROM stg_orders WHERE dq_status = 'FAIL'"
        ).fetchone()[0]
        end = datetime.utcnow()
        write_pipeline_run(
            con,
            run_id=run_id,
            dag_id=ELT_DAG_ID,
            start_time=end - timedelta(minutes=5),
            end_time=end,
            rows_extracted=int(rows_extracted),
            rows_loaded=int(rows_loaded),
            rows_quarantined=int(rows_quarantined),
            status="success",
        )
        con.close()

    meta = load_raw_tables()
    inc = load_increment_raw(meta)
    staging = run_staging(inc)
    marts = run_marts(staging)
    audit = pipeline_run_audit(marts)

    meta >> inc >> staging >> marts >> audit


nammamart_elt_udaykiran_komati()
