import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from nammamart_common import extract_file, etl_validate, etl_transform, etl_load

LOCAL_TZ = pendulum.timezone("Asia/Kolkata")

@dag(
    dag_id="nammamart_etl_indhu",
    description="NammaMart ETL: extract, validate/quarantine, transform, and load DuckDB star schema",
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
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl():
    @task
    def extract_customers():
        return extract_file("customers")

    @task
    def extract_orders_and_increment():
        a=extract_file("orders")
        b=extract_file("increment")
        return {"base_orders":a,"increment":b}

    @task
    def extract_payments():
        return extract_file("payments")

    @task
    def extract_products():
        return extract_file("products")

    @task
    def extract_stores():
        return extract_file("stores")

    @task
    def validate_and_quarantine(_customers, _orders, _payments, _products, _stores):
        ctx=get_current_context()
        return etl_validate(ctx["run_id"])

    @task
    def transform(validation_result):
        ctx=get_current_context()
        return etl_transform(ctx["run_id"], validation_result)

    @task
    def load(transform_result):
        ctx=get_current_context()
        return etl_load(transform_result, ctx["run_id"])

    c=extract_customers()
    o=extract_orders_and_increment()
    p=extract_payments()
    pr=extract_products()
    s=extract_stores()
    v=validate_and_quarantine(c,o,p,pr,s)
    t=transform(v)
    load(t)

nammamart_etl()
