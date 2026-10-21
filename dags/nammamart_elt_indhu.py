import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from nammamart_common import elt_load_raw, elt_run_sql, conn

LOCAL_TZ = pendulum.timezone("Asia/Kolkata")

@dag(
    dag_id="nammamart_elt_indhu",
    description="NammaMart ELT: load raw CSV snapshots as text, then transform in DuckDB SQL",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 20, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "indhu",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["nammamart", "elt", "assignment"],
)
def nammamart_elt():
    @task
    def load_raw():
        ctx=get_current_context()
        return elt_load_raw(ctx["run_id"])

    @task
    def transform_sql(_raw_rows):
        return elt_run_sql(get_current_context()["run_id"])

    @task
    def audit(_mart_counts):
        ctx=get_current_context()
        con=conn()
        try:
            raw_count=con.execute("SELECT (SELECT count(*) FROM raw_orders) + (SELECT count(*) FROM raw_customers) + (SELECT count(*) FROM raw_payments) + (SELECT count(*) FROM raw_products) + (SELECT count(*) FROM raw_stores)").fetchone()[0]
            stg_count=con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            quarantined=con.execute("SELECT count(*) FROM stg_orders WHERE dq_status='FAIL'").fetchone()[0]
            con.execute(
                "INSERT INTO pipeline_runs VALUES (?, ?, current_timestamp, current_timestamp, ?, ?, ?, 'SUCCESS')",
                [str(ctx["run_id"]),"nammamart_elt_indhu",int(raw_count),int(stg_count),int(quarantined)]
            )
        finally:
            con.close()

    raw=load_raw()
    marts=transform_sql(raw)
    audit(marts)

nammamart_elt()
