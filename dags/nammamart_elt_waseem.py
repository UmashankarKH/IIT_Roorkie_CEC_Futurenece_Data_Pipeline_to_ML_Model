"""
dags/nammamart_elt_waseem.py
----------------------------
NammaMart ELT pipeline - assignment Part B, scheduled per Part C.

**ELT** = Extract, Load, Transform.  Every row is loaded into DuckDB exactly as
received, and all cleaning happens afterwards in SQL.  Nothing is thrown away,
so analytics and compliance can audit, replay and re-ask questions later.

Flow::

    load_raw_orders -> load_raw_payments -> load_raw_customers
        -> load_raw_products -> load_raw_stores
        -> load_raw_increment (only when include_increment is passed)
        -> build_staging_masters        (SQL: stg_customers/products/stores)
        -> merge_orders_base            (SQL MERGE into stg_orders)
        -> merge_orders_increment       (SQL MERGE, 150 insert + 10 update)
        -> build_staging_payments       (SQL: needs the clean orders)
        -> run_quality_checks           (SQL: dq_results + elt_rule_failures)
        -> build_mart                   (SQL: the five mart tables)
        -> write_run_audit              (always runs)

Why the raw loads are chained rather than parallel
--------------------------------------------------
DuckDB is an embedded database: only one process may write to the file at a
time.  Airflow runs each task in its own process, so parallel raw loads would
fight over the file lock.  The ETL DAG can extract in parallel because those
tasks only touch CSV files, not the database.

Scheduling (Part C): identical to the ETL DAG - ``0 9 * * 1-6`` in
``Asia/Kolkata``, ``catchup=False``, retries and an execution timeout.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:  # Airflow 3 (Astro Runtime 3.x)
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2 fallback
    from airflow.decorators import dag, task

from include.nammamart import alerts, config, elt_pipeline, warehouse

DAG_ID = elt_pipeline.DAG_ID  # nammamart_elt_waseem

default_args = {
    "owner": "waseem",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alerts.task_failure_callback,
}


def _include_increment(context) -> bool:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return bool(conf.get("include_increment", False))


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: load every raw row into DuckDB, transform with SQL only",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "elt", "fde", "waseem"],
    doc_md=__doc__,
)
def nammamart_elt():
    """Extract -> Load raw -> Transform in the warehouse with SQL."""

    # -- LAYER 1: RAW (append only, nothing cleaned) -------------------------
    def _raw_task(dataset: str):
        @task(task_id=f"load_raw_{dataset}")
        def _run(**context) -> dict:
            with warehouse.connect() as con:
                appended = elt_pipeline.load_raw(con, dataset, context["run_id"])
                total = warehouse.row_count(con, f"raw_{dataset}")
            return {"dataset": dataset, "appended": appended, "raw_total": total}

        return _run()

    @task
    def load_raw_increment(**context) -> dict:
        """Append the next-day orders file (Part B.3). Skipped unless requested."""
        if not _include_increment(context):
            print("[ELT] include_increment not set - skipping the increment file")
            return {"appended": 0, "applied": False}
        with warehouse.connect() as con:
            appended = elt_pipeline.load_raw_increment(con, context["run_id"])
        return {"appended": appended, "applied": True}

    # -- LAYER 2: STAGING (SQL only) ----------------------------------------
    @task
    def build_staging_masters(increment_info: dict) -> dict:
        """Create the staging views and rebuild the customer/product/store tables."""
        with warehouse.connect() as con:
            counts = elt_pipeline.build_staging(con)
        return {"staging": counts, "increment": increment_info}

    @task
    def merge_orders_base(staging_info: dict, **context) -> dict:
        """Upsert the daily orders file into ``stg_orders`` (SQL MERGE)."""
        with warehouse.connect() as con:
            result = elt_pipeline.merge_orders(con, context["run_id"],
                                               config.SOURCE_FILES["orders"])
        return {**staging_info, "merge_base": result}

    @task
    def merge_orders_increment(merge_info: dict, **context) -> dict:
        """Upsert the next-day orders file: new orders insert, corrections update."""
        if not merge_info.get("increment", {}).get("applied"):
            print("[ELT] no increment loaded - nothing to merge")
            return {**merge_info, "merge_increment": {}}
        with warehouse.connect() as con:
            result = elt_pipeline.merge_orders_increment(con, context["run_id"])
        return {**merge_info, "merge_increment": result}

    @task
    def build_staging_payments(merge_info: dict) -> dict:
        """Build ``stg_payments`` last: its amount rule reads the clean orders."""
        with warehouse.connect() as con:
            rows = elt_pipeline.build_staging_payments(con)
        return {**merge_info, "stg_payments": rows}

    # -- DATA QUALITY (Part D, evaluated in SQL) ----------------------------
    @task
    def run_quality_checks(staging_info: dict, **context) -> dict:
        with warehouse.connect() as con:
            dq_summary = elt_pipeline.run_dq_checks(con, context["run_id"], DAG_ID)
            quarantine_file = elt_pipeline.export_quarantine(con, context["run_id"])
            rejected = elt_pipeline.quarantine_summary(con)
        print("[ELT] staging row status:\n" + rejected.to_string(index=False))
        return {**staging_info, "dq": dq_summary, "quarantine_file": quarantine_file,
                "rows_quarantined": int(rejected.loc[rejected["dq_status"] != "PASS", "rows"].sum())}

    # -- LAYER 3: MART (SQL only) -------------------------------------------
    @task
    def build_mart(dq_info: dict) -> dict:
        with warehouse.connect() as con:
            counts = elt_pipeline.build_mart(con)
            elt_pipeline.print_mart_preview(con)
            rows_loaded = int(con.execute(
                "SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'").fetchone()[0])
        return {**dq_info, "mart": counts, "rows_loaded": rows_loaded}

    # -- RUN AUDIT ----------------------------------------------------------
    @task(trigger_rule="all_done")
    def write_run_audit(mart_info: dict | None = None, **context) -> None:
        dag_run = context.get("dag_run")
        summary = mart_info or {}
        status = "SUCCESS" if mart_info else "FAILED"
        rows_extracted = 0
        with warehouse.connect() as con:
            for dataset in config.SOURCE_FILES:
                table = f"raw_{dataset}"
                if warehouse.table_exists(con, table):
                    rows_extracted += int(con.execute(
                        f'SELECT count(*) FROM "{table}" WHERE _run_id = ?',
                        [context["run_id"]]).fetchone()[0])
            warehouse.write_pipeline_run(
                con,
                run_id=context["run_id"],
                dag_id=DAG_ID,
                start_time=getattr(dag_run, "start_date", None) or pendulum.now("UTC"),
                end_time=pendulum.now("UTC"),
                rows_extracted=rows_extracted,
                rows_loaded=summary.get("rows_loaded", 0),
                rows_quarantined=summary.get("rows_quarantined", 0),
                status=status,
                notes=f"mart={list(summary.get('mart', {}).keys())}",
            )

    # Raw loads run one after another because DuckDB allows a single writer.
    raw_tasks = [_raw_task(name) for name in config.SOURCE_FILES]
    for upstream, downstream in zip(raw_tasks, raw_tasks[1:]):
        upstream >> downstream

    increment = load_raw_increment()
    raw_tasks[-1] >> increment

    staging = build_staging_masters(increment)
    merged_base = merge_orders_base(staging)
    merged_increment = merge_orders_increment(merged_base)
    payments = build_staging_payments(merged_increment)
    quality = run_quality_checks(payments)
    mart = build_mart(quality)
    write_run_audit(mart)


nammamart_elt()
