"""
dags/nammamart_elt_dinesh.py
----------------------------
NammaMart - ELT pipeline (Part B)

Extract -> Load raw (untouched) -> Transform in DuckDB with SQL

Every raw row is kept exactly as it arrived, including the bad ones, which is what
analytics and compliance asked for. The filtering happens in staging, in SQL, and the
rejected rows stay queryable instead of ending up in a CSV nobody opens.

    load_raw_orders --> load_raw_payments --> load_raw_customers
                    --> load_raw_products --> load_raw_stores
                            |
                            +--> build_staging --> record_quality --> build_mart --+
                                                                                   |
                                                                                   v
          load_raw_increment --> merge_staging --> merge_mart --> rebuild_reports
                                                                                |
                                                        compare_with_etl <------+
                                                        audit_run (always)

Layers:
    raw     include/sql/raw/        text only, append per source file
    staging include/sql/staging/    cast, standardise, dedupe, mask, quality flags
    mart    include/sql/mart/       star schema and the five reporting tables

No pandas anywhere in this pipeline. The tasks read a .sql file, substitute the run
details and run it; include/nammamart/elt.py is the whole of the Python.
"""

import sys
from datetime import timedelta

sys.path.insert(0, "/usr/local/airflow/include")

import pendulum
from airflow.decorators import dag, task

from nammamart import config as cfg
from nammamart import elt, warehouse as wh

# Same SLA as the ETL DAG: 9:00 AM IST, Monday to Saturday.
SCHEDULE = "0 9 * * 1-6"

default_args = {
    "owner": cfg.OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id=f"nammamart_elt_{cfg.OWNER}",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["fde", "nammamart", "elt", cfg.OWNER],
    doc_md=__doc__,
)
def nammamart_elt():

    # --------------------------------------------------------------- raw layer

    @task
    def load_raw(dataset: str, **context) -> int:
        return elt.load_raw(dataset, context["run_id"])

    raw_tasks = [load_raw.override(task_id=f"load_raw_{dataset}")(dataset)
                 for dataset in elt.RAW_TABLES]

    # The five raw loads are chained rather than parallel, and that is not an
    # oversight. DuckDB allows a single writing process at a time and Airflow gives
    # each task its own process, so running them in parallel fails on the file lock.
    # The ETL DAG can keep its five extracts parallel because they only read CSVs.
    for earlier, later in zip(raw_tasks, raw_tasks[1:]):
        earlier >> later

    # ----------------------------------------------------------- staging layer

    @task
    def build_staging() -> dict:
        return elt.build_staging()

    @task
    def record_quality(**context) -> dict:
        return elt.record_quality(context["run_id"])

    staged = build_staging()
    quality_recorded = record_quality()

    # -------------------------------------------------------------- mart layer

    @task
    def build_mart() -> dict:
        return elt.build_mart()

    marted = build_mart()

    # ------------------------------------------------------- incremental load

    @task
    def load_raw_increment(**context) -> int:
        """Raw: append the 1 Oct rows under their own _source_file. Nothing is
        overwritten, so the September file stays exactly as it was received."""
        return elt.load_raw_increment(f"{context['run_id']}_increment")

    @task
    def merge_staging(**context) -> dict:
        """Staging: MERGE on order_id. 150 inserts, 10 status corrections updated."""
        return elt.merge_staging_increment(f"{context['run_id']}_increment")

    @task
    def record_increment_quality(**context) -> dict:
        return elt.record_quality(f"{context['run_id']}_increment")

    @task
    def merge_mart(**context) -> dict:
        return elt.merge_increment(f"{context['run_id']}_increment")

    @task
    def rebuild_reports() -> dict:
        """The aggregates above the fact are stale after a merge, so rebuild them."""
        return elt.rebuild_reports()

    inc_raw = load_raw_increment()
    inc_staged = merge_staging()
    inc_quality = record_increment_quality()
    inc_merged = merge_mart()
    reports_rebuilt = rebuild_reports()

    # --------------------------------------------------------- ETL vs ELT check

    @task
    def compare_with_etl() -> dict:
        """Part B4 / Part G: the two pipelines must land on the same numbers."""
        return elt.compare_with_etl()

    compared = compare_with_etl()

    # -------------------------------------------------------------- run audit

    # retries=0: this task fails on purpose when something upstream broke, and
    # retrying that just wastes four minutes before the run is marked failed.
    @task(trigger_rule="all_done", retries=0)
    def audit_run(**context) -> dict:
        """Part F: one row per run in pipeline_runs, written even when a task failed.

        Same reasoning as the ETL DAG: the upstream results are pulled with
        xcom_pull inside a try/except, because a failed task publishes no XCom and
        the audit row has to be written anyway. The list of tasks with no result is
        the record of what broke.
        """
        ti = context["ti"]

        def pull(task_id):
            try:
                return ti.xcom_pull(task_ids=task_id)
            except Exception:                                    # noqa: BLE001
                return None

        # Every task that has to succeed for the mart to be current. A task that
        # failed published no XCom, so "no result" is the list of what went wrong.
        missing = [name for name in
                   [f"load_raw_{d}" for d in elt.RAW_TABLES]
                   + ["build_staging", "record_quality", "build_mart",
                      "load_raw_increment", "merge_staging",
                      "record_increment_quality", "merge_mart", "rebuild_reports",
                      "compare_with_etl"]
                   if pull(name) is None]

        extracted = sum(pull(f"load_raw_{d}") or 0 for d in elt.RAW_TABLES)
        staging = pull("build_staging") or {}
        quality_result = pull("record_quality") or {}
        increment = pull("merge_mart") or {}

        run = {
            "run_id": context["run_id"],
            "dag_id": elt.DAG_ID,
            "start_time": str(context["dag_run"].run_after
                              if hasattr(context["dag_run"], "run_after")
                              else context["logical_date"]),
            "end_time": wh.now(),
            "rows_extracted": extracted,
            "rows_loaded": increment.get("fact_rows", 0),
            "rows_quarantined": quality_result.get("rows_quarantined", 0),
            "status": "FAILED" if missing else "SUCCESS",
            "notes": f"no result from: {missing}" if missing
                     else f"staging: {staging} | increment: {increment}",
        }
        with wh.connect() as con:
            wh.record_run(con, run)

        # Fail the task once the evidence is safely written. Without this the DAG
        # run is marked SUCCESS even when a task failed: audit_run is the only leaf
        # task, trigger_rule="all_done" makes it run anyway, and Airflow takes the
        # run's state from its leaves. Part F experiment 4 is where I found that -
        # pipeline_runs said FAILED and the UI said green at the same time.
        if missing:
            raise RuntimeError(f"pipeline run {context['run_id']} failed; "
                               f"no result from: {missing}")
        return run

    audited = audit_run()

    # Staging reads every raw table, so the last raw load has to finish first.
    raw_tasks[-1] >> staged >> quality_recorded >> marted
    # The increment runs after the full load, otherwise the full rebuild of staging
    # would simply overwrite the merged rows.
    marted >> inc_raw >> inc_staged >> inc_quality >> inc_merged >> reports_rebuilt
    reports_rebuilt >> compared >> audited


nammamart_elt()
