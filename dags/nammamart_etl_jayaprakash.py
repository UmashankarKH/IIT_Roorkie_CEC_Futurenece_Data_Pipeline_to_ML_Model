from __future__ import annotations

import json
import os
import sys
from datetime import timedelta

sys.path.insert(0, "/usr/local/airflow")

import pandas as pd
import pendulum
from airflow.decorators import dag, task

from include.nammamart_etl_utils import (
    build_fact_orders,
    ensure_duckdb,
    reconcile_payments,
    standardize_loyalty,
    standardize_mode,
    validate_dataframe,
)

DATA_DIR = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_jayaprakash.duckdb"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/jayaprakash"


def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def _safe_json(value):
    return json.dumps(value, default=str)


@dag(
    dag_id="nammamart_etl_jayaprakash",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        "owner": "jayaprakash",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl_jayaprakash():

    @task
    def extract_orders():
        df = _read_csv(os.path.join(DATA_DIR, "orders.csv"))
        return _safe_json(df.to_dict(orient="records"))

    @task
    def extract_payments():
        df = _read_csv(os.path.join(DATA_DIR, "payments.csv"))
        return _safe_json(df.to_dict(orient="records"))

    @task
    def extract_customers():
        df = _read_csv(os.path.join(DATA_DIR, "customers.csv"))
        return _safe_json(df.to_dict(orient="records"))

    @task
    def extract_products():
        df = _read_csv(os.path.join(DATA_DIR, "products.csv"))
        return _safe_json(df.to_dict(orient="records"))

    @task
    def extract_stores():
        df = _read_csv(os.path.join(DATA_DIR, "stores.csv"))
        return _safe_json(df.to_dict(orient="records"))

    @task
    def validate_and_quarantine(orders_json, payments_json, customers_json, products_json, stores_json):
        data = {
            "orders.csv": pd.DataFrame.from_records(json.loads(orders_json)),
            "payments.csv": pd.DataFrame.from_records(json.loads(payments_json)),
            "customers.csv": pd.DataFrame.from_records(json.loads(customers_json)),
            "products.csv": pd.DataFrame.from_records(json.loads(products_json)),
            "stores.csv": pd.DataFrame.from_records(json.loads(stores_json)),
        }
        valid_maps = {}
        failed_maps = {}
        os.makedirs(QUARANTINE_DIR, exist_ok=True)

        for name, frame in data.items():
            clean_df, failed_df = validate_dataframe(frame, name)
            valid_maps[name] = clean_df.to_dict(orient="records")
            # Keep only failed rows with a reason in the quarantine output
            if not failed_df.empty:
                failed_df["file_name"] = name
                failed_df["failure_reason"] = failed_df.get("failure_reason", "")
                failed_frame = frame.iloc[failed_df["row_index"].tolist()].copy()
                failed_frame["failure_reason"] = failed_df["failure_reason"].values
                out_path = os.path.join(QUARANTINE_DIR, f"{name.replace('.csv', '')}_failed_{pendulum.now('UTC').strftime('%Y%m%d%H%M%S')}.csv")
                failed_frame.to_csv(out_path, index=False)
                failed_maps[name] = failed_frame.to_dict(orient="records")

        return {
            "valid": valid_maps,
            "failed": failed_maps,
            "summary": {key: len(value) for key, value in failed_maps.items()},
        }

    @task
    def transform_and_load(validated_json):
        ensure_duckdb(WAREHOUSE_DB)
        validated = validated_json["valid"]
        orders = pd.DataFrame.from_records(validated.get("orders.csv", []))
        customers = pd.DataFrame.from_records(validated.get("customers.csv", []))
        products = pd.DataFrame.from_records(validated.get("products.csv", []))
        stores = pd.DataFrame.from_records(validated.get("stores.csv", []))
        payments = pd.DataFrame.from_records(validated.get("payments.csv", []))

        fact = build_fact_orders(orders, customers, products, stores, "etl_run")
        if not fact.empty:
            import duckdb
            con = duckdb.connect(WAREHOUSE_DB)
            con.register("fact_df", fact)
            con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM fact_df LIMIT 0")
            con.execute("INSERT OR REPLACE INTO etl_fact_orders SELECT * FROM fact_df")
            con.execute("CREATE TABLE IF NOT EXISTS etl_dim_customer AS SELECT DISTINCT customer_id, full_name, email, phone, loyalty_tier FROM fact_df")
            con.execute("CREATE TABLE IF NOT EXISTS etl_dim_product AS SELECT DISTINCT product_id, category, brand, cost_price FROM fact_df")
            con.execute("CREATE TABLE IF NOT EXISTS etl_dim_store AS SELECT DISTINCT store_id, store_name, zone, manager_name FROM fact_df")
            con.execute("CREATE TABLE IF NOT EXISTS dq_results (run_id TEXT, dag_id TEXT, check_name TEXT, dimension TEXT, severity TEXT, rows_checked INT, rows_failed INT, status TEXT, checked_at TIMESTAMP)")
            con.execute("INSERT INTO dq_results VALUES ('etl_run', 'nammamart_etl_jayaprakash', 'orders_validation', 'validity', 'Critical', ?, ?, 'passed', CURRENT_TIMESTAMP)", [len(orders), 0])
            con.close()

        return {
            "fact_rows": len(fact),
            "orders_rows": len(orders),
            "quarantine_rows": sum(len(v) for v in validated_json["failed"].values()),
        }

    @task
    def reconcile_payments_task(validated_json):
        data = {
            "orders.csv": pd.DataFrame.from_records(validated_json["valid"].get("orders.csv", [])),
            "payments.csv": pd.DataFrame.from_records(validated_json["valid"].get("payments.csv", [])),
        }
        reconciled = reconcile_payments(data["orders.csv"], data["payments.csv"])
        import duckdb
        con = duckdb.connect(WAREHOUSE_DB)
        con.register("reconciled_df", reconciled)
        con.execute("CREATE TABLE IF NOT EXISTS etl_payment_reconciliation AS SELECT * FROM reconciled_df WHERE 1=0")
        con.execute("INSERT OR REPLACE INTO etl_payment_reconciliation SELECT * FROM reconciled_df")
        con.close()
        return len(reconciled)

    @task
    def final_report():
        import duckdb
        con = duckdb.connect(WAREHOUSE_DB)
        zone = con.execute(
            "SELECT zone, ROUND(SUM(net_revenue), 2) AS net_revenue FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY zone ORDER BY zone"
        ).fetchdf()
        margin = con.execute(
            "SELECT category, ROUND(SUM(gross_margin), 2) AS gross_margin FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY category ORDER BY category"
        ).fetchdf()
        print("Zone revenue:\n", zone.to_string(index=False))
        print("Category margin:\n", margin.to_string(index=False))
        con.close()

    @task(trigger_rule="all_done")
    def audit_run(transformed_json, reconciliation_count):
        import duckdb
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("CREATE TABLE IF NOT EXISTS pipeline_runs (run_id TEXT, dag_id TEXT, start_time TIMESTAMP, end_time TIMESTAMP, rows_extracted INT, rows_loaded INT, rows_quarantined INT, status TEXT)")
        run_id = f"etl_{pendulum.now('UTC').strftime('%Y%m%d%H%M%S')}"
        rows_extracted = transformed_json.get("orders_rows", 0)
        rows_loaded = transformed_json.get("fact_rows", 0)
        rows_quarantined = transformed_json.get("quarantine_rows", 0)
        now = pendulum.now('UTC').to_iso8601_string()
        con.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [run_id, "nammamart_etl_jayaprakash", now, now, rows_extracted, rows_loaded, rows_quarantined, "success"],
        )
        print(f"[Audit] pipeline_runs row inserted for {run_id}")
        con.close()

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()

    validated = validate_and_quarantine(orders, payments, customers, products, stores)
    transformed = transform_and_load(validated)
    reconciliation = reconcile_payments_task(validated)
    final = final_report()
    audit = audit_run(transformed, reconciliation)

    transformed.set_downstream(reconciliation)
    reconciliation.set_downstream(final)
    final.set_downstream(audit)


nammamart_etl_jayaprakash()
