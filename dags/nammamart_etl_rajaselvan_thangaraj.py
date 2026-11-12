"""
dags/nammamart_etl_rajaselvan_thangaraj.py
-------------------------------------------
NammaMart Quick Commerce — ETL Pipeline
Author: rajaselvan_thangaraj
Client SLA: 9:00 AM IST, Monday to Saturday (cron: 0 9 * * 1-6)

Flow:
  extract_orders ─────┐
  extract_payments ───┼
  extract_customers ──┼──► validate_and_quarantine ──► transform_clean_data ──► load_star_schema ──► reconcile_payments ──► generate_daily_reports ──► run_audit
  extract_products ───┼                                      │
  extract_stores ─────┘                                      └── (quarantine CSV written with failure_reason)
"""

import sys
import os
import io

# Dynamic include path resolution for local dev & container runtime
for p in ["/usr/local/airflow/include", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "include"))]:
    if os.path.exists(p) and p not in sys.path:
        sys.path.insert(0, p)

from airflow.decorators import dag, task
from datetime import timedelta
import pendulum
import pandas as pd
import duckdb

from nammamart_etl import get_paths, standardize_payment_mode, standardize_loyalty_tier
from nammamart_quality import (
    validate_orders, validate_customers, validate_products,
    validate_stores, validate_payments
)
from nammamart_masking import mask_email, mask_phone
from nammamart_observability import record_pipeline_run

default_args = {
    "owner": "rajaselvan_thangaraj",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id="nammamart_etl_rajaselvan_thangaraj",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fde", "nammamart", "etl", "star_schema"],
    doc_md="""
    # NammaMart ETL Pipeline
    Delivers clean, reconciled reporting data to DuckDB before 9:00 AM IST Monday to Saturday.
    - Quarantines bad rows with explicit root cause reasons.
    - Masks customer PII (DPDP Act 2023).
    - Idempotent loading to `etl_fact_orders` and dimensions.
    - Automated payment gateway reconciliation.
    """,
)
def nammamart_etl_pipeline():

    # ── 1. EXTRACT TASKS (Run in parallel) ────────────────────────
    @task
    def extract_orders() -> str:
        _, _, data_dir = get_paths()
        df = pd.read_csv(os.path.join(data_dir, "orders.csv"))
        print(f"[Extract] orders.csv: {len(df)} rows")
        return df.to_json()

    @task
    def extract_payments() -> str:
        _, _, data_dir = get_paths()
        df = pd.read_csv(os.path.join(data_dir, "payments.csv"))
        print(f"[Extract] payments.csv: {len(df)} rows")
        return df.to_json()

    @task
    def extract_customers() -> str:
        _, _, data_dir = get_paths()
        df = pd.read_csv(os.path.join(data_dir, "customers.csv"))
        print(f"[Extract] customers.csv: {len(df)} rows")
        return df.to_json()

    @task
    def extract_products() -> str:
        _, _, data_dir = get_paths()
        df = pd.read_csv(os.path.join(data_dir, "products.csv"))
        print(f"[Extract] products.csv: {len(df)} rows")
        return df.to_json()

    @task
    def extract_stores() -> str:
        _, _, data_dir = get_paths()
        df = pd.read_csv(os.path.join(data_dir, "stores.csv"))
        print(f"[Extract] stores.csv: {len(df)} rows")
        return df.to_json()

    # ── 2. VALIDATE & QUARANTINE ──────────────────────────────────
    @task
    def validate_and_quarantine(orders_json: str, payments_json: str,
                                customers_json: str, products_json: str,
                                stores_json: str, **context) -> dict:
        run_id = context.get("run_id", f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}")
        dag_id = "nammamart_etl_rajaselvan_thangaraj"
        warehouse_db, quarantine_dir, _ = get_paths()
        os.makedirs(quarantine_dir, exist_ok=True)

        con = duckdb.connect(warehouse_db)

        orders_df = pd.read_json(io.StringIO(orders_json))
        payments_df = pd.read_json(io.StringIO(payments_json))
        customers_df = pd.read_json(io.StringIO(customers_json))
        products_df = pd.read_json(io.StringIO(products_json))
        stores_df = pd.read_json(io.StringIO(stores_json))

        # Validate dimensions & log checks
        val_cust = validate_customers(customers_df, run_id, dag_id, con)
        val_prod = validate_products(products_df, run_id, dag_id, con)
        val_stores = validate_stores(stores_df, run_id, dag_id, con)
        val_pay = validate_payments(payments_df, orders_df, run_id, dag_id, con)

        # Validate orders (splits into clean and quarantined)
        passed_orders, failed_orders = validate_orders(
            orders_df, val_cust, val_prod, val_stores, run_id, dag_id, con
        )

        quarantine_file = os.path.join(quarantine_dir, f"orders_failed_{run_id}.csv")
        if len(failed_orders) > 0:
            failed_orders.to_csv(quarantine_file, index=False)
            print(f"[Quarantine] Wrote {len(failed_orders)} rows to {quarantine_file}")

        con.close()

        total_extracted = len(orders_df) + len(payments_df) + len(customers_df) + len(products_df) + len(stores_df)

        return {
            "passed_orders": passed_orders.to_json(),
            "quarantined_count": len(failed_orders),
            "total_extracted": total_extracted,
            "payments_json": payments_json,
            "customers_json": val_cust.to_json(),
            "products_json": val_prod.to_json(),
            "stores_json": val_stores.to_json(),
            "run_id": run_id,
        }

    # ── 3. TRANSFORM ──────────────────────────────────────────────
    @task
    def transform_clean_data(val_result: dict) -> dict:
        passed_orders = pd.read_json(io.StringIO(val_result["passed_orders"]))
        customers = pd.read_json(io.StringIO(val_result["customers_json"]))
        products = pd.read_json(io.StringIO(val_result["products_json"]))
        stores = pd.read_json(io.StringIO(val_result["stores_json"]))

        # Deduplicate & mask dimensions
        dim_cust = customers.drop_duplicates(subset=['customer_id'], keep='first').copy()
        dim_cust['loyalty_tier'] = dim_cust['loyalty_tier'].apply(standardize_loyalty_tier)
        dim_cust['masked_email'] = dim_cust['email'].apply(mask_email)
        dim_cust['masked_phone'] = dim_cust['phone'].apply(mask_phone)

        dim_prod = products.drop_duplicates(subset=['product_id'], keep='first').copy()
        dim_prod['category'] = dim_prod['category'].fillna('Unknown')

        dim_store = stores.copy()
        dim_store.loc[dim_store['store_id'] == 'S07', 'zone'] = 'South'

        # Transform Fact Orders
        fact = passed_orders.copy()
        fact['order_status'] = fact['order_status'].astype(str).str.strip().str.upper()
        fact['payment_mode'] = fact['payment_mode'].apply(standardize_payment_mode)
        fact['order_date'] = pd.to_datetime(fact['order_ts']).dt.date.astype(str)

        cost_map = dim_prod.set_index('product_id')['cost_price'].to_dict()
        fact['cost_price'] = fact['product_id'].map(cost_map).fillna(0.0)

        fact['net_revenue'] = fact.apply(
            lambda r: r['order_amount'] if r['order_status'] == 'DELIVERED' else 0.0, axis=1
        ).round(2)

        fact['gross_margin'] = fact.apply(
            lambda r: round(r['net_revenue'] - (r['quantity'] * r['cost_price']), 2) if r['order_status'] == 'DELIVERED' else 0.0,
            axis=1
        )

        fact['within_15_min'] = fact.apply(
            lambda r: bool(r['delivery_minutes'] <= 15.0) if r['order_status'] == 'DELIVERED' else False,
            axis=1
        )

        fact['etl_loaded_at'] = pendulum.now("Asia/Kolkata").to_iso8601_string()
        fact['run_id'] = val_result["run_id"]

        print(f"[Transform Complete] {len(fact)} clean orders prepared for warehouse")

        return {
            "fact_orders": fact.to_json(),
            "dim_cust": dim_cust.to_json(),
            "dim_prod": dim_prod.to_json(),
            "dim_store": dim_store.to_json(),
            "payments_json": val_result["payments_json"],
            "total_extracted": val_result["total_extracted"],
            "quarantined_count": val_result["quarantined_count"],
            "run_id": val_result["run_id"],
        }

    # ── 4. LOAD TO WAREHOUSE (IDEMPOTENT) ──────────────────────────
    @task
    def load_star_schema(transform_data: dict) -> dict:
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        fact_df = pd.read_json(io.StringIO(transform_data["fact_orders"]))
        dim_cust = pd.read_json(io.StringIO(transform_data["dim_cust"]))
        dim_prod = pd.read_json(io.StringIO(transform_data["dim_prod"]))
        dim_store = pd.read_json(io.StringIO(transform_data["dim_store"]))

        # Dimensions: idempotent replacement
        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_customer AS SELECT * FROM dim_cust WHERE 1=0")
        con.execute("DELETE FROM etl_dim_customer")
        con.register("v_cust", dim_cust)
        con.execute("""
            INSERT INTO etl_dim_customer
            SELECT customer_id, full_name, masked_email, masked_phone, city, area, pincode, signup_date, loyalty_tier, is_active
            FROM v_cust
        """)

        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_product AS SELECT * FROM dim_prod WHERE 1=0")
        con.execute("DELETE FROM etl_dim_product")
        con.register("v_prod", dim_prod)
        con.execute("""
            INSERT INTO etl_dim_product
            SELECT product_id, product_name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date
            FROM v_prod
        """)

        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_store AS SELECT * FROM dim_store WHERE 1=0")
        con.execute("DELETE FROM etl_dim_store")
        con.register("v_store", dim_store)
        con.execute("""
            INSERT INTO etl_dim_store
            SELECT store_id, store_name, zone, area, pincode, open_date, manager_name, capacity_orders_per_day
            FROM v_store
        """)

        # Fact: idempotent upsert by order_id
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_fact_orders (
                order_id VARCHAR PRIMARY KEY,
                order_ts VARCHAR,
                order_date VARCHAR,
                customer_id VARCHAR,
                store_id VARCHAR,
                product_id VARCHAR,
                quantity INTEGER,
                unit_price DOUBLE,
                discount_pct INTEGER,
                order_amount DOUBLE,
                net_revenue DOUBLE,
                gross_margin DOUBLE,
                payment_mode VARCHAR,
                order_status VARCHAR,
                channel VARCHAR,
                delivery_minutes DOUBLE,
                within_15_min BOOLEAN,
                etl_loaded_at VARCHAR,
                run_id VARCHAR
            )
        """)

        cols = [
            'order_id', 'order_ts', 'order_date', 'customer_id', 'store_id', 'product_id',
            'quantity', 'unit_price', 'discount_pct', 'order_amount', 'net_revenue', 'gross_margin',
            'payment_mode', 'order_status', 'channel', 'delivery_minutes', 'within_15_min',
            'etl_loaded_at', 'run_id'
        ]
        final_fact = fact_df[cols]
        con.register("v_fact", final_fact)
        con.execute("""
            DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM v_fact);
            INSERT INTO etl_fact_orders SELECT * FROM v_fact;
        """)

        loaded_count = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.close()
        print(f"[Load Complete] etl_fact_orders rows in warehouse: {loaded_count}")

        return {
            "loaded_count": len(final_fact),
            "payments_json": transform_data["payments_json"],
            "total_extracted": transform_data["total_extracted"],
            "quarantined_count": transform_data["quarantined_count"],
            "run_id": transform_data["run_id"],
        }

    # ── 5. RECONCILE PAYMENTS ─────────────────────────────────────
    @task
    def reconcile_payments(load_result: dict) -> dict:
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        payments_df = pd.read_json(io.StringIO(load_result["payments_json"]))
        con.register("v_payments", payments_df)

        con.execute("""
            CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            WITH
            pay_clean AS (
                SELECT
                    payment_id,
                    order_id,
                    payment_ts,
                    TRIM(payment_mode) AS payment_mode,
                    amount_paid,
                    payment_status,
                    gateway_ref
                FROM v_payments
            ),
            ord_clean AS (
                SELECT
                    order_id,
                    order_amount,
                    net_revenue,
                    order_status
                FROM etl_fact_orders
            )
            SELECT
                o.order_id,
                p.payment_id,
                o.order_amount,
                p.amount_paid,
                'Delivered order has no SUCCESS payment' AS mismatch_reason,
                o.order_status,
                p.payment_status
            FROM ord_clean o
            LEFT JOIN pay_clean p ON o.order_id = p.order_id AND p.payment_status = 'SUCCESS'
            WHERE o.order_status = 'DELIVERED' AND p.payment_id IS NULL

            UNION ALL

            SELECT
                o.order_id,
                p.payment_id,
                o.order_amount,
                p.amount_paid,
                'Amount mismatch between order and payment' AS mismatch_reason,
                o.order_status,
                p.payment_status
            FROM ord_clean o
            JOIN pay_clean p ON o.order_id = p.order_id
            WHERE p.payment_status = 'SUCCESS' AND ROUND(o.order_amount, 2) != ROUND(p.amount_paid, 2)

            UNION ALL

            SELECT
                o.order_id,
                p.payment_id,
                o.order_amount,
                p.amount_paid,
                'SUCCESS payment exists for non-delivered order' AS mismatch_reason,
                o.order_status,
                p.payment_status
            FROM ord_clean o
            JOIN pay_clean p ON o.order_id = p.order_id
            WHERE o.order_status IN ('CANCELLED', 'RETURNED') AND p.payment_status = 'SUCCESS'

            UNION ALL

            SELECT
                p.order_id,
                p.payment_id,
                NULL AS order_amount,
                p.amount_paid,
                'Payment without valid order in fact table' AS mismatch_reason,
                NULL AS order_status,
                p.payment_status
            FROM pay_clean p
            LEFT JOIN ord_clean o ON p.order_id = o.order_id
            WHERE o.order_id IS NULL;
        """)

        reconcile_count = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
        con.close()
        print(f"[Reconciliation Complete] etl_payment_reconciliation has {reconcile_count} rows")

        return load_result

    # ── 6. GENERATE REPORTS ───────────────────────────────────────
    @task
    def generate_daily_reports(reconcile_result: dict) -> dict:
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT
                s.zone,
                f.order_date,
                COUNT(f.order_id) AS delivered_orders,
                ROUND(SUM(f.net_revenue), 2) AS net_revenue
            FROM etl_fact_orders f
            JOIN etl_dim_store s ON f.store_id = s.store_id
            WHERE f.order_status = 'DELIVERED'
            GROUP BY s.zone, f.order_date
            ORDER BY f.order_date, s.zone;
        """)

        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT
                p.category,
                ROUND(SUM(f.net_revenue), 2) AS total_revenue,
                ROUND(SUM(f.gross_margin), 2) AS total_gross_margin,
                ROUND(SUM(f.gross_margin) * 100.0 / NULLIF(SUM(f.net_revenue), 0), 2) AS margin_pct
            FROM etl_fact_orders f
            JOIN etl_dim_product p ON f.product_id = p.product_id
            WHERE f.order_status = 'DELIVERED'
            GROUP BY p.category
            ORDER BY total_revenue DESC;
        """)

        cat_df = con.execute("SELECT * FROM etl_category_margin").fetchdf()
        print("\n=== [ETL Morning Review Report: Category Margin] ===")
        print(cat_df.to_string(index=False))

        con.close()
        return reconcile_result

    # ── 7. RUN AUDIT (trigger_rule="all_done") ────────────────────
    @task(trigger_rule="all_done")
    def run_audit(report_data: dict, **context):
        warehouse_db, _, _ = get_paths()
        con = duckdb.connect(warehouse_db)

        start_time = context.get("data_interval_start", pendulum.now("Asia/Kolkata")).to_iso8601_string()
        end_time = pendulum.now("Asia/Kolkata").to_iso8601_string()
        run_id = report_data.get("run_id", "unknown_run")
        extracted = report_data.get("total_extracted", 0)
        loaded = report_data.get("loaded_count", 0)
        quarantined = report_data.get("quarantined_count", 0)

        record_pipeline_run(
            con=con,
            run_id=run_id,
            dag_id="nammamart_etl_rajaselvan_thangaraj",
            start_time=start_time,
            end_time=end_time,
            rows_extracted=extracted,
            rows_loaded=loaded,
            rows_quarantined=quarantined,
            status="SUCCESS"
        )
        con.close()
        print("[Run Audit] Successfully recorded run in pipeline_runs")

    # ── DAG WIRING ────────────────────────────────────────────────
    orders_data = extract_orders()
    payments_data = extract_payments()
    customers_data = extract_customers()
    products_data = extract_products()
    stores_data = extract_stores()

    val = validate_and_quarantine(
        orders_data, payments_data, customers_data, products_data, stores_data
    )
    transformed = transform_clean_data(val)
    loaded = load_star_schema(transformed)
    reconciled = reconcile_payments(loaded)
    reported = generate_daily_reports(reconciled)
    run_audit(reported)


etl_dag = nammamart_etl_pipeline()
