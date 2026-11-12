"""NammaMart ETL pipeline: parallel source checks, Python validation, and run audit."""
import pendulum
from datetime import timedelta
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from include.nammamart_pipeline import run_etl, ensure_run_audit

@dag(
    dag_id="nammamart_etl_surbhi",
    description="Validate and transform NammaMart exports before loading finance tables",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={"owner":"surbhi","retries":2,"retry_delay":timedelta(minutes=3),"execution_timeout":timedelta(minutes=30)},
    tags=["nammamart","etl","assignment"],
)
def nammamart_etl_surbhi():
    def make_extract_task(task_id, filename, required_columns):
        @task(task_id=task_id)
        def extract_and_profile():
            from include.nammamart_pipeline import data_dir
            path = data_dir() / filename
            if not path.exists():
                raise FileNotFoundError(f"Required source file not found: {path}")
            frame = pd.read_csv(path, low_memory=False)
            missing = sorted(set(required_columns) - set(frame.columns))
            if missing:
                raise ValueError(f"Schema check failed for {filename}: missing required columns {missing}")
            return {"file": filename, "rows": len(frame), "columns": len(frame.columns)}
        return extract_and_profile()

    from include.nammamart_pipeline import REQUIRED_COLUMNS, FILES, INCREMENT
    extract_tasks = [
        make_extract_task("extract_orders", FILES["orders"], REQUIRED_COLUMNS[FILES["orders"]]),
        make_extract_task("extract_payments", FILES["payments"], REQUIRED_COLUMNS[FILES["payments"]]),
        make_extract_task("extract_customers", FILES["customers"], REQUIRED_COLUMNS[FILES["customers"]]),
        make_extract_task("extract_products", FILES["products"], REQUIRED_COLUMNS[FILES["products"]]),
        make_extract_task("extract_stores", FILES["stores"], REQUIRED_COLUMNS[FILES["stores"]]),
        make_extract_task("extract_increment", INCREMENT, REQUIRED_COLUMNS[INCREMENT]),
    ]

    @task
    def validate_transform_load(_source_profiles):
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        run_etl(str(ctx["run_id"]))
        return "ETL completed; inspect DuckDB tables, quarantine, and reports."

    load = validate_transform_load(extract_tasks)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def final_run_audit():
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        ti = ctx["ti"]
        outcome = "success" if ti.xcom_pull(task_ids="validate_transform_load") else "failed"
        ensure_run_audit(str(ctx["run_id"]), "nammamart_etl_surbhi", outcome)

    audit = final_run_audit()
    load >> audit

nammamart_etl_surbhi()
