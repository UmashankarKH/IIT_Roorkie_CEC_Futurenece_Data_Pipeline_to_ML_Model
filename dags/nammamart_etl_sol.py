"""Validated ETL pipeline for NammaMart's September order exports."""

import sys
from datetime import timedelta

sys.path.insert(0, "/usr/local/airflow/include")

import pendulum
from airflow.decorators import dag, task
from airflow.sdk import get_current_context

from nammamart_etl import (
    build_reports,
    extract_file,
    load_star_schema,
    quarantine_files,
    reconcile_payments,
    transform_files,
    validate_file,
)


DEFAULT_ARGS = {
    "owner": "fde-sol",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
}


@dag(
    dag_id="nammamart_etl_sol",
    description="Validate, quarantine, transform and load NammaMart orders into DuckDB",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl_sol():
    @task
    def extract_orders():
        return extract_file("orders.csv")

    @task
    def extract_payments():
        return extract_file("payments.csv")

    @task
    def extract_customers():
        return extract_file("customers.csv")

    @task
    def extract_products():
        return extract_file("products.csv")

    @task
    def extract_stores():
        return extract_file("stores.csv")

    @task
    def validate_customers_task(manifest):
        context = get_current_context()
        return validate_file(manifest, context["run_id"])

    @task
    def validate_products_task(manifest):
        context = get_current_context()
        return validate_file(manifest, context["run_id"])

    @task
    def validate_stores_task(manifest):
        context = get_current_context()
        return validate_file(manifest, context["run_id"])

    @task
    def validate_payments_task(manifest, order_result):
        context = get_current_context()
        return validate_file(
            manifest,
            context["run_id"],
            {"orders.csv": order_result},
        )

    @task
    def validate_orders_task(manifest, customer_result, product_result, store_result):
        context = get_current_context()
        masters = {
            "customers.csv": customer_result,
            "products.csv": product_result,
            "stores.csv": store_result,
        }
        return validate_file(manifest, context["run_id"], masters)

    @task
    def quarantine_task(order_result, payment_result, customer_result, product_result, store_result):
        context = get_current_context()
        results = [order_result, payment_result, customer_result, product_result, store_result]
        return quarantine_files(results, context["run_id"])

    @task
    def transform_task(order_result, customer_result, product_result, store_result):
        context = get_current_context()
        return transform_files(
            order_result,
            customer_result,
            product_result,
            store_result,
            context["run_id"],
        )

    @task
    def load_task(transformed):
        return load_star_schema(transformed)

    @task
    def reconcile_task(load_result, payment_manifest):
        return reconcile_payments(load_result, payment_manifest)

    @task
    def report_task(reconciliation, order_result, payment_result, customer_result, product_result, store_result):
        results = [order_result, payment_result, customer_result, product_result, store_result]
        return build_reports(reconciliation, results)

    # Extract tasks fan out independently. Validation is also parallel except
    # orders, which waits for clean dimension keys to enforce referential integrity.
    order_source = extract_orders()
    payment_source = extract_payments()
    customer_source = extract_customers()
    product_source = extract_products()
    store_source = extract_stores()

    clean_customers = validate_customers_task(customer_source)
    clean_products = validate_products_task(product_source)
    clean_stores = validate_stores_task(store_source)
    clean_orders = validate_orders_task(
        order_source, clean_customers, clean_products, clean_stores
    )
    clean_payments = validate_payments_task(payment_source, clean_orders)

    quarantine = quarantine_task(
        clean_orders, clean_payments, clean_customers, clean_products, clean_stores
    )
    transformed = transform_task(clean_orders, clean_customers, clean_products, clean_stores)
    loaded = load_task(transformed)
    reconciled = reconcile_task(loaded, payment_source)
    report = report_task(
        reconciled,
        clean_orders,
        clean_payments,
        clean_customers,
        clean_products,
        clean_stores,
    )

    # Do not load a successful fact snapshot until rejected rows have been
    # published to the run-specific quarantine files.
    quarantine >> transformed


nammamart_etl_sol()
