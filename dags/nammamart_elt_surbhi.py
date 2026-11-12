"""NammaMart ELT pipeline: parallel source checks, raw preservation, SQL marts, and run audit."""
import pendulum
from datetime import timedelta
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from include.nammamart_pipeline import run_elt, ensure_run_audit

@dag(
    dag_id="nammamart_elt_surbhi",
    description="Preserve raw NammaMart exports and transform with DuckDB SQL",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={"owner":"surbhi","retries":2,"retry_delay":timedelta(minutes=3),"execution_timeout":timedelta(minutes=30)},
    tags=["nammamart","elt","assignment"],
)
def nammamart_elt_surbhi():
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
    def load_raw_transform_sql(_source_profiles):
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        run_elt(str(ctx["run_id"]))
        return "ELT completed; raw/staging/mart tables are in DuckDB."

    load = load_raw_transform_sql(extract_tasks)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def final_run_audit():
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        ti = ctx["ti"]
        outcome = "success" if ti.xcom_pull(task_ids="load_raw_transform_sql") else "failed"
        ensure_run_audit(str(ctx["run_id"]), "nammamart_elt_surbhi", outcome)

    audit = final_run_audit()
    load >> audit

nammamart_elt_surbhi()
