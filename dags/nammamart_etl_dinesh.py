"""
dags/nammamart_etl_dinesh.py
----------------------------
NammaMart - ETL pipeline (Part A)

Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report

Bad rows are stopped before the warehouse. The finance tables only ever contain
orders that passed every Critical check, which is what the CFO asked for.

    extract_orders    --> validate_orders    --+
    extract_payments  --> validate_payments  --+
    extract_customers --> validate_customers --+--> transform --> load_star --> report
    extract_products  --> validate_products  --+        |
    extract_stores    --> validate_stores    --+        +-> record_transform_dq
                                                              |
    (record_validation_dq runs before transform)        load_increment
                                                              |
                                                        reconcile --> report
    (always, even on failure)                                       audit_run

The real logic lives in include/nammamart/ so it can be unit tested and run from a
terminal (python -m nammamart.run_pipelines --pipeline etl). The tasks here are thin
on purpose - a DAG file full of pandas is a DAG file nobody can test.
"""

import sys
from datetime import timedelta

sys.path.insert(0, "/usr/local/airflow/include")

import pendulum
from airflow.decorators import dag, task

from nammamart import config as cfg
from nammamart import etl, warehouse as wh

# Part C: 9:00 AM IST, Monday to Saturday. The stand-up is at 9:30, so the run has
# to be finished before 9:00, not started at 9:30.
SCHEDULE = "0 9 * * 1-6"

default_args = {
    "owner": cfg.OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # A task that has not finished in 15 minutes is stuck, not slow: this dataset
    # takes seconds. Better to fail and alert than to drift past the stand-up.
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id=f"nammamart_etl_{cfg.OWNER}",
    schedule=SCHEDULE,
    # Timezone-aware start_date. Without tz="Asia/Kolkata" Airflow reads the cron as
    # UTC and the run lands at 2:30 PM IST, five hours after the stand-up.
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["fde", "nammamart", "etl", cfg.OWNER],
    doc_md=__doc__,
)
def nammamart_etl():

    # ---------------------------------------------------------------- extract

    @task
    def extract(dataset: str, **context) -> str:
        return etl.extract_to_stash(dataset, context["run_id"])

    @task
    def validate(dataset: str, raw_path: str, **context) -> dict:
        return etl.validate_to_stash(dataset, raw_path, context["run_id"])

    # Five parallel extract tasks, one per file, declared explicitly so the Graph
    # view shows which file is which instead of five identical mapped boxes.
    validated = {}
    for dataset in cfg.SOURCE_FILES:
        raw = extract.override(task_id=f"extract_{dataset}")(dataset)
        validated[dataset] = validate.override(task_id=f"validate_{dataset}")(dataset, raw)

    # -------------------------------------------------------------- transform

    @task
    def transform(validated_paths: dict, **context) -> dict:
        return etl.transform_from_stash(validated_paths, context["run_id"])

    transformed = transform(validated)

    # ------------------------------------------------------------------- load

    @task
    def load_star(paths: dict) -> dict:
        return etl.load_star(paths)

    @task
    def reconcile() -> int:
        return etl.load_reconciliation()

    @task
    def record_validation_dq(validated_paths: dict) -> int:
        """Part D: the per-file results, written before transform gets a chance to
        fail. Experiment 6 is the reason this is its own task."""
        return etl.record_validation_dq(validated_paths)

    @task
    def record_transform_dq(transformed_paths: dict, **context) -> int:
        """The cross-file checks: referential integrity, volume, freshness."""
        return etl.record_transform_dq(transformed_paths, context["run_id"])

    @task
    def load_increment(**context) -> dict:
        """Part B3 on the ETL side: new orders inserted, corrections overwritten."""
        return etl.load_increment(context["run_id"])

    validation_dq = record_validation_dq(validated)
    loaded = load_star(transformed)
    transform_dq = record_transform_dq(transformed)
    reconciled = reconcile()
    incremented = load_increment()

    # ----------------------------------------------------------------- report

    @task
    def report() -> dict:
        reports = etl.build_reports()
        return {name: len(df) for name, df in reports.items()}

    reported = report()

    # ------------------------------------------------------------- run audit

    # retries=0: this task fails on purpose when something upstream broke, and
    # retrying that just wastes four minutes before the run is marked failed.
    @task(trigger_rule="all_done", retries=0)
    def audit_run(**context) -> dict:
        """Part F: one row per run in pipeline_runs, written even when a task failed.

        trigger_rule="all_done" is the whole point. A run audit that only gets
        written on success tells you nothing on the morning something breaks.

        It pulls the upstream results with xcom_pull inside a try/except rather than
        taking them as arguments. A task that failed has published no XCom, and a
        task that takes it as an argument would either fail itself or get None - so
        the audit row would be missing exactly when it matters most. A missing XCom
        is also the signal for which task failed, which is what `notes` records.
        """
        ti = context["ti"]

        def pull(task_id):
            try:
                return ti.xcom_pull(task_ids=task_id)
            except Exception:                                    # noqa: BLE001
                return None

        missing = []
        extracted = quarantined = 0
        for dataset in cfg.SOURCE_FILES:
            item = pull(f"validate_{dataset}")
            if not item:
                missing.append(f"validate_{dataset}")
                continue
            extracted += item["rows_passed"] + item["rows_failed"]
            quarantined += item["rows_failed"]

        transformed_paths = pull("transform") or {}
        increment = pull("load_increment") or {}
        quarantined += transformed_paths.get("quarantined", 0)
        quarantined += increment.get("quarantined", 0)

        # Every task that has to succeed for the 9 AM number to exist. A task that
        # failed published no XCom, so "no result" is the list of what went wrong.
        missing += [name for name in ("transform", "record_validation_dq", "load_star",
                                     "record_transform_dq", "load_increment",
                                     "reconcile", "report")
                    if pull(name) is None]

        run = {
            "run_id": context["run_id"],
            "dag_id": etl.DAG_ID,
            "start_time": str(context["dag_run"].run_after
                              if hasattr(context["dag_run"], "run_after")
                              else context["logical_date"]),
            "end_time": wh.now(),
            "rows_extracted": extracted,
            "rows_loaded": transformed_paths.get("fact_rows", 0)
                           + increment.get("inserted", 0),
            "rows_quarantined": quarantined,
            "status": "FAILED" if missing else "SUCCESS",
            "notes": f"no result from: {missing}" if missing
                     else f"increment: {increment}",
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

    # Everything that writes to DuckDB is chained, deliberately. DuckDB allows one
    # writing process at a time, and Airflow runs each task in its own process, so two
    # parallel tasks writing to the warehouse fail on the file lock. The five extract
    # and validate tasks stay parallel because they only touch CSVs.
    #
    # record_validation_dq sits before transform so the quality evidence survives a
    # transform failure. The increment has to land after the full load or the
    # CREATE OR REPLACE would wipe it, and reconcile reads the fact table back, so it
    # runs after the increment - otherwise it reconciles yesterday's orders.
    validation_dq >> transformed
    loaded >> transform_dq >> incremented >> reconciled >> reported >> audited


nammamart_etl()
