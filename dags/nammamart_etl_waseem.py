"""
dags/nammamart_etl_waseem.py
----------------------------
NammaMart ETL pipeline - assignment Part A, scheduled per Part C.

**DAG** (Directed Acyclic Graph): an Airflow workflow - a set of tasks plus the
order in which they run.  Java comparison (learning aid): a job definition in a
scheduler, where each task is a class with one responsibility.

Flow::

    extract_orders    -+
    extract_payments  -|
    extract_customers -+-> validate_and_quarantine -> transform_star_schema
    extract_products  -|                                      |
    extract_stores    -+                                      v
                                                     load_star_schema
                                                              |
                                                              v
                                                     reconcile_payments
                                                              |
                                                              v
                                              build_reports -> write_run_audit

ETL means the data is cleaned **before** it reaches the warehouse: only rows
that passed every Critical quality rule are loaded, so the finance tables never
contain a bad row.

Scheduling (Part C)
-------------------
* cron ``0 9 * * 1-6``  - 09:00, Monday to Saturday
* timezone ``Asia/Kolkata`` - set on ``start_date`` with pendulum, otherwise
  Airflow assumes UTC and 09:00 UTC would be 14:30 IST, far too late for the
  09:30 IST leadership stand-up
* ``catchup=False`` - a missed day must not trigger a backfill storm; the
  client wants today's number, not a replay of last week
* retries, retry_delay and execution_timeout are set in ``default_args``
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum

# Make the project root importable so "include.nammamart..." works both inside
# the Astro container and on a laptop.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:  # Airflow 3 (Astro Runtime 3.x)
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2 fallback
    from airflow.decorators import dag, task

from include.nammamart import alerts, config, etl_pipeline, warehouse

DAG_ID = etl_pipeline.DAG_ID  # nammamart_etl_waseem

default_args = {
    "owner": "waseem",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alerts.task_failure_callback,
}


def _include_increment(context) -> bool:
    """Read the optional ``include_increment`` flag passed when triggering the DAG."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return bool(conf.get("include_increment", False))


@dag(
    dag_id=DAG_ID,
    description="NammaMart ETL: clean in pandas, then load a star schema into DuckDB",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "etl", "fde", "waseem"],
    doc_md=__doc__,
)
def nammamart_etl():
    """Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report."""

    # -- EXTRACT: five tasks, one per file, which Airflow runs in parallel ----
    # They only read CSVs and write run-scoped staging files, so there is no
    # database lock for them to fight over.
    def _extract_task(dataset: str):
        @task(task_id=f"extract_{dataset}")
        def _run(**context) -> dict:
            run_id = context["run_id"]
            frame = etl_pipeline.extract(dataset, include_increment=_include_increment(context))
            path = etl_pipeline.write_staging(run_id, f"raw_{dataset}", frame)
            return {"dataset": dataset, "rows": len(frame), "path": path}

        return _run()

    extracts = [_extract_task(name) for name in config.SOURCE_FILES]

    # -- VALIDATE + QUARANTINE ----------------------------------------------
    @task
    def validate_and_quarantine(extract_results: list[dict], **context) -> dict:
        """Apply the Part D rules and split each file into passed and failed rows."""
        run_id = context["run_id"]
        frames = {item["dataset"]: etl_pipeline.read_staging(run_id, f"raw_{item['dataset']}")
                  for item in extract_results}

        results = etl_pipeline.validate_all(frames, run_id, DAG_ID)
        quarantine_files = etl_pipeline.quarantine(results, run_id)

        for dataset, result in results.items():
            etl_pipeline.write_staging(run_id, f"clean_{dataset}", result.passed)

        with warehouse.connect() as con:
            warehouse.ensure_audit_tables(con)
            warehouse.write_dq_results(con, etl_pipeline.collect_dq_rows(results))
            quarantined = etl_pipeline.quarantine_frame(results)
            if not quarantined.empty:
                warehouse.append_table(con, config.QUARANTINE_TABLE, quarantined)

        summary = {
            "rows_extracted": sum(item["rows"] for item in extract_results),
            "rows_passed": {name: result.rows_passed for name, result in results.items()},
            "rows_quarantined": sum(result.rows_failed for result in results.values()),
            "quarantine_files": quarantine_files,
        }
        print(f"[Validate] {summary['rows_extracted']} rows in, "
              f"{summary['rows_quarantined']} quarantined")
        return summary

    # -- TRANSFORM -----------------------------------------------------------
    @task
    def transform_star_schema(validation_summary: dict, **context) -> dict:
        """Standardise, deduplicate, mask PII, join the dimensions and derive KPIs."""
        run_id = context["run_id"]
        clean = {name: etl_pipeline.read_staging(run_id, f"clean_{name}")
                 for name in config.SOURCE_FILES}
        tables = etl_pipeline.transform(clean, run_id)
        with warehouse.connect() as con:
            staged = etl_pipeline.stage_transformed(con, tables)
        return {"staged": staged,
                "rows_quarantined": validation_summary["rows_quarantined"],
                "rows_extracted": validation_summary["rows_extracted"]}

    # -- LOAD (idempotent) ---------------------------------------------------
    @task
    def load_star_schema(transform_summary: dict) -> dict:
        """Upsert the fact table on order_id and replace the three dimensions."""
        with warehouse.connect() as con:
            tables = etl_pipeline.read_staged(con)
            counts = etl_pipeline.load(con, tables)
        return {**transform_summary, "load_counts": counts}

    # -- RECONCILE orders against the payment gateway ------------------------
    @task
    def reconcile_payments(load_summary: dict, **context) -> dict:
        run_id = context["run_id"]
        payments = etl_pipeline.read_staging(run_id, "raw_payments")
        with warehouse.connect() as con:
            fact = warehouse.query(con, "SELECT * FROM etl_fact_orders")
            reconciliation = etl_pipeline.reconcile_payments(fact, payments, run_id)
            warehouse.replace_table(con, "etl_payment_reconciliation", reconciliation)
        return {**load_summary, "mismatches": len(reconciliation)}

    # -- REPORT --------------------------------------------------------------
    @task
    def build_reports(reconcile_summary: dict) -> dict:
        """Build the two summary tables and print them into the task log."""
        with warehouse.connect() as con:
            reports = etl_pipeline.build_reports(con)
        return {**reconcile_summary,
                "zone_rows": len(reports["etl_daily_zone_revenue"]),
                "category_rows": len(reports["etl_category_margin"])}

    # -- RUN AUDIT (runs even when an upstream task failed) ------------------
    @task(trigger_rule="all_done")
    def write_run_audit(report_summary: dict | None = None, **context) -> None:
        """Write one row per run to ``pipeline_runs`` (assignment Part F)."""
        dag_run = context.get("dag_run")
        summary = report_summary or {}
        status = "SUCCESS" if report_summary else "FAILED"
        with warehouse.connect() as con:
            warehouse.write_pipeline_run(
                con,
                run_id=context["run_id"],
                dag_id=DAG_ID,
                start_time=getattr(dag_run, "start_date", None) or pendulum.now("UTC"),
                end_time=pendulum.now("UTC"),
                rows_extracted=summary.get("rows_extracted", 0),
                rows_loaded=summary.get("load_counts", {}).get("etl_fact_orders", 0),
                rows_quarantined=summary.get("rows_quarantined", 0),
                status=status,
                notes=f"mismatches={summary.get('mismatches', 'n/a')}",
            )

    validated = validate_and_quarantine(extracts)
    transformed = transform_star_schema(validated)
    loaded = load_star_schema(transformed)
    reconciled = reconcile_payments(loaded)
    reported = build_reports(reconciled)
    write_run_audit(reported)


nammamart_etl()
