"""
dags/nammamart_elt_rajaselvan_thangaraj.py
-------------------------------------------
NammaMart Quick Commerce — ELT Pipeline
Author: rajaselvan_thangaraj
Client SLA: 9:00 AM IST, Monday to Saturday (cron: 0 9 * * 1-6)

Flow:
  load_raw_tables ──► run_staging_layer ──► load_and_merge_incremental ──► build_marts ──► verify_parity ──► run_audit
"""

import sys
import os

# Dynamic include path resolution for local dev & container runtime
for p in ["/usr/local/airflow/include", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "include"))]:
    if os.path.exists(p) and p not in sys.path:
        sys.path.insert(0, p)

from airflow.decorators import dag, task
from datetime import timedelta
import pendulum
import duckdb

from nammamart_etl import get_paths
from nammamart_elt import (
    load_raw_tables, run_staging_transforms,
    load_and_merge_increment, run_mart_transforms,
    verify_etl_elt_parity
)
from nammamart_observability import record_pipeline_run

default_args = {
    "owner": "rajaselvan_thangaraj",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id="nammamart_elt_rajaselvan_thangaraj",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fde", "nammamart", "elt", "duckdb_sql"],
    doc_md="""
    # NammaMart ELT Pipeline
    Loads every raw record untouched into DuckDB `raw_*` tables, then transforms and standardizes
    inside the warehouse using SQL (`stg_*` and `mart_*` layers).
    Includes automated MERGE for the next-day incremental feed (`orders_2026-10-01_increment.csv`).
    """,
)
def nammamart_elt_pipeline():

    # ── 1. LOAD RAW UNTOUCHED ─────────────────────────────────────
    @task
    def load_raw_data(**context) -> dict:
        run_id = context.get("run_id", f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        load_raw_tables(con, run_id)
        raw_count = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        con.close()

        return {"run_id": run_id, "raw_orders_count": raw_count}

    # ── 2. STAGING TRANSFORMS VIA SQL ─────────────────────────────
    @task
    def transform_staging_layer(prev_result: dict) -> dict:
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        run_staging_transforms(con)
        stg_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
        failed_count = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'FAILED'").fetchone()[0]
        con.close()

        print(f"[Staging] stg_orders created: {stg_count} rows ({failed_count} flagged as FAILED)")
        return {
            "run_id": prev_result["run_id"],
            "stg_orders_count": stg_count,
            "failed_count": failed_count,
        }

    # ── 3. INCREMENTAL LOAD & MERGE ───────────────────────────────
    @task
    def apply_incremental_merge(prev_result: dict) -> dict:
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        load_and_merge_increment(con, prev_result["run_id"])
        total_orders = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
        con.close()

        return {
            "run_id": prev_result["run_id"],
            "total_orders_after_merge": total_orders,
            "failed_count": prev_result["failed_count"],
        }

    # ── 4. BUILD MARTS VIA SQL ────────────────────────────────────
    @task
    def build_marts(prev_result: dict) -> dict:
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        run_mart_transforms(con)
        con.close()
        return prev_result

    # ── 5. VERIFY SEPTEMBER PARITY VS ETL ─────────────────────────
    @task
    def verify_parity(prev_result: dict) -> dict:
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        verify_etl_elt_parity(con)
        con.close()
        return prev_result

    # ── 6. RUN AUDIT (trigger_rule="all_done") ────────────────────
    @task(trigger_rule="all_done")
    def run_audit(prev_result: dict, **context):
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        start_time = context.get("data_interval_start", pendulum.now("Asia/Kolkata")).to_iso8601_string()
        end_time = pendulum.now("Asia/Kolkata").to_iso8601_string()
        run_id = prev_result.get("run_id", "unknown_run")
        loaded = prev_result.get("total_orders_after_merge", 0)
        quarantined = prev_result.get("failed_count", 0)

        record_pipeline_run(
            con=con,
            run_id=run_id,
            dag_id="nammamart_elt_rajaselvan_thangaraj",
            start_time=start_time,
            end_time=end_time,
            rows_extracted=loaded + quarantined,
            rows_loaded=loaded,
            rows_quarantined=quarantined,
            status="SUCCESS"
        )
        con.close()
        print("[Run Audit] Successfully recorded ELT run in pipeline_runs")

    # ── DAG WIRING ────────────────────────────────────────────────
    raw = load_raw_data()
    stg = transform_staging_layer(raw)
    merged = apply_incremental_merge(stg)
    marts = build_marts(merged)
    parity = verify_parity(marts)
    run_audit(parity)


elt_dag = nammamart_elt_pipeline()
