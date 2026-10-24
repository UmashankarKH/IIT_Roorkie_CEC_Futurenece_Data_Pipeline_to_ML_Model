"""Airflow DAG wrapper for the NammaMart SQL-based ELT pipeline."""

from datetime import timedelta
import re
import sys

import duckdb
import pendulum
from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, "/usr/local/airflow/include")

from nammamart_elt import (
    DATA_DIR,
    DEFAULT_WAREHOUSE,
    MART_SQL_FILES,
    SOURCE_FILES,
    STAGING_SQL_FILES,
    append_incremental_orders,
    count_raw_rows,
    load_raw_tables,
    record_dq_results,
    run_sql_files,
    update_customer_history,
)
from alerts import notify_failure, notify_quality_warning
from observability import VOLUME_CHECK_NAME, record_pipeline_run

DAG_ID = "nammamart_elt_samy"


def _run_id() -> str:
    """Airflow's run_id, made safe to store and compare."""

    return re.sub(r"[^A-Za-z0-9_-]", "_", get_current_context()["run_id"])


@dag(
    dag_id="nammamart_elt_samy",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        "owner": "samy",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        # Slack alert (SLACK_WEBHOOK_URL Variable) once a task fails its last retry.
        "on_failure_callback": notify_failure,
    },
    tags=["nammamart", "elt", "assignment"],
)
def nammamart_elt_samy():
    """Define Extract/Load raw → Increment → Staging SQL → Mart SQL → DQ → Audit."""

    @task
    def load_raw() -> int:
        # Raw ingestion performs no pandas cleaning: every row is kept as
        # text with _loaded_at, _source_file and _run_id.
        load_raw_tables(run_id=_run_id())
        rows = count_raw_rows()
        print(f"[Raw] {rows} rows loaded into raw_* tables")
        return rows

    @task
    def load_increment(_raw_rows: int) -> dict:
        # Raw is append-only: the next-day feed is appended, never merged,
        # so both the original and corrected order versions are kept.
        if not (DATA_DIR / SOURCE_FILES["orders_increment"]).exists():
            print("[Increment] No increment file today; nothing appended.")
            return {"increment_rows_appended": 0, "increment_inserted": 0, "increment_updated": 0}
        metrics = append_incremental_orders(run_id=_run_id())
        print(f"[Increment] {metrics}")
        return metrics

    @task
    def build_staging(_increment: dict) -> dict:
        # stg_orders.sql performs the MERGE/upsert: latest row per order_id.
        run_sql_files(STAGING_SQL_FILES)
        con = duckdb.connect(str(DEFAULT_WAREHOUSE), read_only=True)
        try:
            counts = dict(con.execute("SELECT dq_status, count(*) FROM stg_orders GROUP BY 1").fetchall())
        finally:
            con.close()
        print(f"[Staging] stg_orders by dq_status: {counts}")
        return {"rows_loaded": counts.get("PASS", 0), "rows_quarantined": counts.get("FAIL", 0)}

    @task
    def build_marts(_history: dict) -> None:
        run_sql_files(MART_SQL_FILES)
        print(f"[Marts] built: {', '.join(name.removesuffix('.sql') for name in MART_SQL_FILES)}")

    @task
    def record_quality(_marts: None) -> int:
        scorecard = record_dq_results(run_id=_run_id(), dag_id=DAG_ID)
        failed = int((scorecard["status"] == "FAIL").sum())
        volume_failed = scorecard[(scorecard["check_name"] == VOLUME_CHECK_NAME) & (scorecard["status"] == "FAIL")]
        notify_quality_warning(DAG_ID, _run_id(), volume_failed["check_name"].tolist())
        print(f"[DQ] {len(scorecard)} checks, {failed} failed")
        return failed

    @task
    def update_customer_history_scd2(_staging: dict) -> dict:
        # dim_customer_history.sql closes changed tier versions and opens new ones.
        result = update_customer_history(run_id=_run_id())
        print(f"[SCD2] dim_customer_history: {result}")
        return result

    @task(trigger_rule="all_done")
    def audit_completion() -> None:
        """Write one pipeline_runs row whether the run succeeded or failed."""

        context = get_current_context()
        ti = context["ti"]
        raw_rows = ti.xcom_pull(task_ids="load_raw") or 0
        increment = ti.xcom_pull(task_ids="load_increment") or {}
        staging = ti.xcom_pull(task_ids="build_staging") or {}
        quality = ti.xcom_pull(task_ids="record_quality")
        history = ti.xcom_pull(task_ids="update_customer_history_scd2")
        # record_quality and the history update are the last business tasks:
        # a missing result means a failure upstream.
        status = "SUCCESS" if quality is not None and history is not None else "FAILED"
        start = getattr(context.get("dag_run"), "start_date", None) or pendulum.now("UTC")
        record_pipeline_run(
            DEFAULT_WAREHOUSE,
            run_id=_run_id(),
            dag_id=DAG_ID,
            start_time=pendulum.instance(start).to_iso8601_string(),
            end_time=pendulum.now("UTC").to_iso8601_string(),
            rows_extracted=raw_rows + increment.get("increment_rows_appended", 0),
            rows_loaded=staging.get("rows_loaded", 0),
            rows_quarantined=staging.get("rows_quarantined", 0),
            status=status,
        )
        print(f"[Audit] {DAG_ID} run {_run_id()} finished with status {status}")

    # Sequential, not parallel: DuckDB allows one writing process per file.
    history = update_customer_history_scd2(build_staging(load_increment(load_raw())))
    quality = record_quality(build_marts(history))
    quality >> audit_completion()


nammamart_elt_samy()
