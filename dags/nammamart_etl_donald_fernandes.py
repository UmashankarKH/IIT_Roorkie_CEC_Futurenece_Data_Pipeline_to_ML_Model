"""NammaMart ETL pipeline.

Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report -> Audit
This file is intended for dags/nammamart_etl_donald_fernandes.py.
"""
from __future__ import annotations

import json
import os
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

DATA_DIR = "/usr/local/airflow/include/data/nammamart"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/donald_fernandes"
DB_PATH = "/usr/local/airflow/include/warehouse_donald_fernandes.duckdb"

DEFAULT_ARGS = {
    "owner": "donald-fernandes",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

STATUS_MAP = {"delivered": "DELIVERED", "cancelled": "CANCELLED", "returned": "RETURNED"}
MODE_MAP = {
    "upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery", "wallet": "Wallet",
}
VALID_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}


def _read(name: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(DATA_DIR, name), dtype=str, keep_default_na=False)


def _strip(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = (
        out.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    for col in out.columns:
        out[col] = out[col].astype(str).str.strip()
    return out


def _mask_email(value: str) -> str:
    if not value or "@" not in value:
        return ""
    local, domain = value.split("@", 1)
    return (local[:2] + "***@" + domain) if len(local) > 2 else (local + "***@" + domain)


def _mask_phone(value: str) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits[:2] + "******" + digits[-2:] if len(digits) >= 4 else ""


def _quarantine(df: pd.DataFrame, reasons: dict[int, list[str]], source: str, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    failed_mask = pd.Series(False, index=df.index)
    for idx in reasons:
        failed_mask.loc[idx] = True
    failed = df.loc[failed_mask].copy()
    clean = df.loc[~failed_mask].copy()
    if not failed.empty:
        failed.insert(0, "failure_reason", ["; ".join(reasons[i]) for i in failed.index])
        failed.insert(0, "run_id", run_id)
        failed.insert(0, "source_file", source)
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        failed.to_csv(os.path.join(QUARANTINE_DIR, f"{source.replace('.csv','')}_failed_{run_id}.csv"), index=False)
    return clean, failed


def _add(reasons, mask, reason):
    for idx in mask[mask].index:
        reasons.setdefault(idx, []).append(reason)


@dag(
    dag_id="nammamart_etl_donald_fernandes",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "etl", "donald-fernandes"],
)
def nammamart_etl():
    @task
    def extract_orders() -> str:
        path = os.path.join(DATA_DIR, "orders.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        return path

    @task
    def extract_payments() -> str:
        path = os.path.join(DATA_DIR, "payments.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        return path

    @task
    def extract_customers() -> str:
        path = os.path.join(DATA_DIR, "customers.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        return path

    @task
    def extract_products() -> str:
        path = os.path.join(DATA_DIR, "products.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        return path

    @task
    def extract_stores() -> str:
        path = os.path.join(DATA_DIR, "stores.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        return path

    @task
    def validate_and_transform(order_path, payment_path, customer_path, product_path, store_path) -> dict:
        import duckdb
        context = get_current_context()
        run_id = context["run_id"].replace(":", "_")
        orders = _strip(pd.read_csv(order_path, dtype=str, keep_default_na=False))
        payments = _strip(pd.read_csv(payment_path, dtype=str, keep_default_na=False))
        customers = _strip(pd.read_csv(customer_path, dtype=str, keep_default_na=False))
        products = _strip(pd.read_csv(product_path, dtype=str, keep_default_na=False))
        stores = _strip(pd.read_csv(store_path, dtype=str, keep_default_na=False))
        results = []

        # Dimensions.
        customer_reasons = {}
        _add(customer_reasons, customers.customer_id.eq(""), "missing customer_id")
        _add(customer_reasons, customers.customer_id.duplicated(keep="last"), "duplicate customer_id")
        customers_clean, customers_failed = _quarantine(customers, customer_reasons, "customers.csv", run_id)
        customers_clean["loyalty_tier"] = customers_clean["loyalty_tier"].str.title()
        customers_clean["email_masked"] = customers_clean["email"].map(_mask_email)
        customers_clean["phone_masked"] = customers_clean["phone"].map(_mask_phone)
        results.append({"file": "customers.csv", "checked": len(customers), "failed": len(customers_failed)})

        product_reasons = {}
        products["mrp_num"] = pd.to_numeric(products["mrp"], errors="coerce")
        products["cost_num"] = pd.to_numeric(products["cost_price"], errors="coerce")
        _add(product_reasons, products.product_id.eq(""), "missing product_id")
        _add(product_reasons, products.product_id.duplicated(keep="last"), "duplicate product_id")
        _add(product_reasons, products.category.eq(""), "missing category")
        _add(product_reasons, products.mrp_num.le(0), "mrp must be positive")
        _add(product_reasons, products.cost_num.gt(products.mrp_num), "cost_price greater than mrp")
        products_clean, products_failed = _quarantine(products, product_reasons, "products.csv", run_id)
        products_clean["mrp"] = products_clean.pop("mrp_num")
        products_clean["cost_price"] = products_clean.pop("cost_num")
        results.append({"file": "products.csv", "checked": len(products), "failed": len(products_failed)})

        store_reasons = {}
        _add(store_reasons, stores.store_id.eq(""), "missing store_id")
        _add(store_reasons, stores.zone.eq(""), "missing zone")
        stores_clean, stores_failed = _quarantine(stores, store_reasons, "stores.csv", run_id)
        results.append({"file": "stores.csv", "checked": len(stores), "failed": len(stores_failed)})

        # Orders.
        orders["order_status"] = orders["order_status"].str.lower().map(STATUS_MAP).fillna(orders["order_status"])
        orders["payment_mode"] = orders["payment_mode"].str.lower().map(MODE_MAP).fillna(orders["payment_mode"])
        for col in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
            orders[col] = pd.to_numeric(orders[col], errors="coerce")
        orders["order_ts_parsed"] = pd.to_datetime(orders["order_ts"], errors="coerce")
        order_reasons = {}
        _add(order_reasons, orders.order_id.eq(""), "missing order_id")
        _add(order_reasons, orders.order_id.duplicated(keep=False), "duplicate order_id")
        _add(order_reasons, orders.order_ts_parsed.isna(), "invalid or missing order_ts")
        _add(order_reasons, ~orders.order_status.isin(VALID_STATUSES), "invalid order_status")
        _add(order_reasons, ~orders.payment_mode.isin(VALID_MODES), "invalid payment_mode")
        _add(order_reasons, ~orders.customer_id.isin(set(customers_clean.customer_id)), "unknown customer_id")
        _add(order_reasons, ~orders.product_id.isin(set(products_clean.product_id)), "unknown product_id")
        _add(order_reasons, ~orders.store_id.isin(set(stores_clean.store_id)), "unknown store_id")
        _add(order_reasons, orders.quantity.le(0), "quantity must be greater than zero")
        _add(order_reasons, orders.discount_pct.lt(0) | orders.discount_pct.gt(100), "discount_pct outside 0-100")
        expected = (orders.quantity * orders.unit_price * (1 - orders.discount_pct / 100)).round(2)
        _add(order_reasons, (expected - orders.order_amount).abs().gt(0.01), "order_amount formula mismatch")
        delivered = orders.order_status.eq("DELIVERED")
        _add(order_reasons, delivered & orders.delivery_minutes.isna(), "delivered order missing delivery_minutes")
        _add(order_reasons, delivered & orders.delivery_minutes.gt(120), "delivery_minutes above 120")
        orders_clean, orders_failed = _quarantine(orders, order_reasons, "orders.csv", run_id)
        orders_clean["order_date"] = orders_clean.order_ts_parsed.dt.date.astype(str)
        orders_clean["net_revenue"] = orders_clean.order_amount.where(orders_clean.order_status.eq("DELIVERED"), 0.0).round(2)
        product_cost = products_clean.set_index("product_id")["cost_price"]
        orders_clean["gross_margin"] = (orders_clean.net_revenue - orders_clean.quantity * orders_clean.product_id.map(product_cost)).round(2)
        orders_clean["within_15_min"] = orders_clean.order_status.eq("DELIVERED") & orders_clean.delivery_minutes.le(15)
        orders_clean = orders_clean.drop(columns=["order_ts_parsed"])
        results.append({"file": "orders.csv", "checked": len(orders), "failed": len(orders_failed)})

        payment_reasons = {}
        payments["payment_mode"] = payments["payment_mode"].str.lower().map(MODE_MAP).fillna(payments["payment_mode"])
        payments["amount_paid"] = pd.to_numeric(payments["amount_paid"], errors="coerce")
        payments["payment_ts_parsed"] = pd.to_datetime(payments["payment_ts"], errors="coerce")
        _add(payment_reasons, payments.payment_id.duplicated(keep=False), "duplicate payment_id")
        _add(payment_reasons, payments.order_id.duplicated(keep=False), "duplicate payment order_id")
        _add(payment_reasons, payments.payment_ts_parsed.isna(), "invalid payment_ts")
        _add(payment_reasons, ~payments.payment_mode.isin(VALID_MODES), "invalid payment_mode")
        payments_clean, payments_failed = _quarantine(payments, payment_reasons, "payments.csv", run_id)
        results.append({"file": "payments.csv", "checked": len(payments), "failed": len(payments_failed)})

        # Persist cleaned tables and quality scorecard.
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        con = duckdb.connect(DB_PATH)
        for name, frame in {
            "etl_dim_customer": customers_clean,
            "etl_dim_product": products_clean,
            "etl_dim_store": stores_clean,
            "etl_fact_orders_stage": orders_clean,
            "etl_payments_stage": payments_clean,
        }.items():
            con.register(name + "_df", frame)
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM {name}_df")
            con.unregister(name + "_df")

        con.execute("""CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP
        )""")
        now = pendulum.now("UTC").to_datetime_string()
        dq_rows = []
        for result in results:
            dq_rows.append((run_id, "nammamart_etl_donald_fernandes", result["file"] + " validation", "multiple", "Critical", result["checked"], result["failed"], "PASS" if result["failed"] == 0 else "FAIL", now))
        con.executemany("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", dq_rows)
        con.close()
        return {"run_id": run_id, "rows_extracted": int(sum(r["checked"] for r in results)), "rows_loaded": len(orders_clean), "rows_quarantined": int(sum(r["failed"] for r in results))}

    @task
    def load_star_schema(result: dict):
        import duckdb
        con = duckdb.connect(DB_PATH)
        con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM etl_fact_orders_stage WHERE 1=0")
        con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM etl_fact_orders_stage)")
        con.execute("INSERT INTO etl_fact_orders SELECT * FROM etl_fact_orders_stage")
        con.close()
        return result

    @task
    def reconcile_and_report(result: dict):
        import duckdb
        con = duckdb.connect(DB_PATH)
        con.execute("""CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            SELECT p.order_id, p.payment_id, p.payment_status, p.amount_paid,
                   o.order_amount,
                   CASE WHEN o.order_id IS NULL THEN 'ORDER_NOT_FOUND'
                        WHEN p.payment_status='SUCCESS' AND ABS(p.amount_paid-o.order_amount)>0.01 THEN 'SUCCESS_AMOUNT_MISMATCH'
                        ELSE 'MATCH' END AS reconciliation_reason
            FROM etl_payments_stage p LEFT JOIN etl_fact_orders o USING(order_id)
            WHERE o.order_id IS NULL OR (p.payment_status='SUCCESS' AND ABS(p.amount_paid-o.order_amount)>0.01)
        """)
        con.execute("""CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, s.zone, s.store_name, ROUND(SUM(o.net_revenue),2) AS net_revenue
            FROM etl_fact_orders o JOIN etl_dim_store s USING(store_id)
            WHERE o.order_status='DELIVERED' GROUP BY 1,2,3 ORDER BY 1,2,3
        """)
        con.execute("""CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category, ROUND(SUM(o.net_revenue),2) AS revenue,
                   ROUND(SUM(o.gross_margin),2) AS gross_margin
            FROM etl_fact_orders o JOIN etl_dim_product p USING(product_id)
            GROUP BY 1 ORDER BY 1
        """)
        print(con.execute("SELECT * FROM etl_daily_zone_revenue LIMIT 20").fetchdf().to_string(index=False))
        con.close()
        return result

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run():
        import duckdb
        context = get_current_context()
        run_id = context["run_id"].replace(":", "_")
        dag_run = context["dag_run"]
        status = getattr(dag_run, "state", None) or "running"
        con = duckdb.connect(DB_PATH)
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
        )""")
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, COALESCE((SELECT SUM(rows_checked) FROM dq_results WHERE run_id=?),0), COALESCE((SELECT COUNT(*) FROM etl_fact_orders),0), COALESCE((SELECT SUM(rows_failed) FROM dq_results WHERE run_id=?),0), ?)", [run_id, context["dag"].dag_id, dag_run.start_date, pendulum.now("UTC"), run_id, run_id, status])
        con.close()

    extracted = [extract_orders(), extract_payments(), extract_customers(), extract_products(), extract_stores()]
    result = validate_and_transform(*extracted)
    loaded = load_star_schema(result)
    reported = reconcile_and_report(loaded)
    reported >> audit_run()


nammamart_etl()
