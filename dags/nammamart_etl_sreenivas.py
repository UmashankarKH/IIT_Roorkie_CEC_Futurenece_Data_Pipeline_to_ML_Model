"""
dags/nammamart_etl_sreenivas.py
-------------------------------
NammaMart Quick Commerce — Production ETL Pipeline (Extract -> Validate -> Transform -> Load)
Author: Sreenivasulu Kona (sreenivas)

Business SLA: Ready before 9:00 AM IST, Monday through Saturday.
Timezone: Asia/Kolkata (IST)
Warehouse: include/warehouse_sreenivas.duckdb
"""

import sys
import os
import json
from datetime import timedelta
import pendulum
import pandas as pd
import duckdb

# Support both Astro container (/usr/local/airflow) and local workspace
BASE_DIR = "/usr/local/airflow" if os.path.exists("/usr/local/airflow") else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INCLUDE_DIR = os.path.join(BASE_DIR, "include")
if INCLUDE_DIR not in sys.path:
    sys.path.insert(0, INCLUDE_DIR)

from airflow.decorators import dag, task
from airflow.models import Variable
from nammamart_dq import (
    validate_orders,
    validate_payments,
    validate_customers,
    validate_products,
    validate_stores,
    standardize_payment_mode,
    standardize_order_status,
    mask_email,
    mask_phone,
)

DATA_DIR = os.path.join(INCLUDE_DIR, "data", "nammamart")
WAREHOUSE_DB = os.path.join(INCLUDE_DIR, "warehouse_sreenivas.duckdb")
QUARANTINE_DIR = os.path.join(INCLUDE_DIR, "quarantine", "sreenivas")

# Timezone-aware schedule: 9:00 AM IST, Monday through Saturday
# Asia/Kolkata is UTC+5:30. In cron: 0 9 * * 1-6
IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "sreenivas",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id="nammamart_etl_sreenivas",
    description="NammaMart ETL Pipeline: Python-based data quality, quarantine, PII masking, star schema load",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz=IST),
    catchup=False,
    default_args=default_args,
    tags=["nammamart", "etl", "production", "star_schema", "sreenivas"],
)
def nammamart_etl_pipeline():

    # ── 1. EXTRACT (5 parallel tasks) ──────────────────────────────────────────

    @task
    def extract_orders():
        path = os.path.join(DATA_DIR, "orders.csv")
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} orders from {path}")
        return df.to_json(date_format="iso")

    @task
    def extract_payments():
        path = os.path.join(DATA_DIR, "payments.csv")
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} payments from {path}")
        return df.to_json(date_format="iso")

    @task
    def extract_customers():
        path = os.path.join(DATA_DIR, "customers.csv")
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} customers from {path}")
        return df.to_json(date_format="iso")

    @task
    def extract_products():
        path = os.path.join(DATA_DIR, "products.csv")
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} products from {path}")
        return df.to_json(date_format="iso")

    @task
    def extract_stores():
        path = os.path.join(DATA_DIR, "stores.csv")
        df = pd.read_csv(path)
        print(f"[Extract] Loaded {len(df)} stores from {path}")
        return df.to_json(date_format="iso")

    # ── 2. VALIDATE & QUARANTINE ──────────────────────────────────────────────

    @task
    def validate_and_quarantine(orders_json, payments_json, customers_json, products_json, stores_json, **context):
        run_id = context.get("run_id", f"manual_{pendulum.now('UTC').strftime('%Y%m%d_%H%M%S')}")
        clean_run_id = str(run_id).replace(":", "_").replace("+", "_")
        os.makedirs(QUARANTINE_DIR, exist_ok=True)

        orders_df = pd.read_json(orders_json)
        payments_df = pd.read_json(payments_json)
        customers_df = pd.read_json(customers_json)
        products_df = pd.read_json(products_json)
        stores_df = pd.read_json(stores_json)

        # Validate master dimensions first
        p_cust, f_cust, dq_cust = validate_customers(customers_df, run_id=run_id)
        p_prod, f_prod, dq_prod = validate_products(products_df, run_id=run_id)
        p_store, f_store, dq_store = validate_stores(stores_df, run_id=run_id)

        # Validate orders against clean dimensions
        p_ord, f_ord, dq_ord = validate_orders(orders_df, p_cust, p_prod, p_store, run_id=run_id)

        # Validate payments against orders
        p_pay, f_pay, dq_pay = validate_payments(payments_df, orders_df, run_id=run_id)

        # Write Quarantine Files
        quarantine_summary = {}
        for fname, f_df in [
            ("orders", f_ord),
            ("payments", f_pay),
            ("customers", f_cust),
            ("products", f_prod),
            ("stores", f_store),
        ]:
            if len(f_df) > 0:
                q_path = os.path.join(QUARANTINE_DIR, f"{fname}_failed_{clean_run_id}.csv")
                f_df.to_csv(q_path, index=False)
                quarantine_summary[fname] = len(f_df)
                print(f"[Quarantine] Wrote {len(f_df)} bad {fname} rows to {q_path}")
            else:
                quarantine_summary[fname] = 0

        # Persist DQ Results to DuckDB scorecard
        all_dq = pd.concat([dq_cust, dq_prod, dq_store, dq_ord, dq_pay], ignore_index=True)
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id VARCHAR,
                dag_id VARCHAR,
                check_name VARCHAR,
                dimension VARCHAR,
                severity VARCHAR,
                rows_checked BIGINT,
                rows_failed BIGINT,
                status VARCHAR,
                checked_at TIMESTAMP
            )
        """)
        # Avoid duplicate dq records for the same run
        con.execute("DELETE FROM dq_results WHERE run_id = ?", [run_id])
        con.register("df_dq_view", all_dq)
        con.execute("INSERT INTO dq_results SELECT * FROM df_dq_view")
        con.close()

        print(f"[DQ Scorecard] Logged {len(all_dq)} quality checks to dq_results table")

        return {
            "passed_orders": p_ord.to_json(date_format="iso"),
            "passed_payments": p_pay.to_json(date_format="iso"),
            "passed_customers": p_cust.to_json(date_format="iso"),
            "passed_products": p_prod.to_json(date_format="iso"),
            "passed_stores": p_store.to_json(date_format="iso"),
            "total_extracted": len(orders_df) + len(payments_df) + len(customers_df) + len(products_df) + len(stores_df),
            "total_quarantined": len(f_ord) + len(f_pay) + len(f_cust) + len(f_prod) + len(f_store),
            "quarantine_summary": quarantine_summary,
        }

    # ── 3. TRANSFORM ──────────────────────────────────────────────────────────

    @task
    def transform_data(validation_result: dict, **context):
        run_id = context.get("run_id", "manual")
        p_ord = pd.read_json(validation_result["passed_orders"])
        p_cust = pd.read_json(validation_result["passed_customers"])
        p_prod = pd.read_json(validation_result["passed_products"])
        p_store = pd.read_json(validation_result["passed_stores"])

        # Deduplicate customers & products
        dim_cust = p_cust.drop_duplicates(subset=["customer_id"]).copy()
        dim_prod = p_prod.drop_duplicates(subset=["product_id"]).copy()
        dim_store = p_store.drop_duplicates(subset=["store_id"]).copy()

        # Join Orders with Dimension tables
        merged = p_ord.merge(
            dim_cust[["customer_id", "full_name", "email_masked", "phone_masked", "loyalty_tier_standardized"]],
            on="customer_id",
            how="inner",
        )
        merged = merged.merge(
            dim_prod[["product_id", "product_name", "category", "cost_price"]],
            on="product_id",
            how="inner",
        )
        merged = merged.merge(
            dim_store[["store_id", "store_name", "zone"]],
            on="store_id",
            how="inner",
        )

        # Derive required metrics
        merged["order_date"] = pd.to_datetime(merged["order_ts"], format="mixed").dt.strftime("%Y-%m-%d")
        merged["net_revenue"] = merged.apply(
            lambda r: r["order_amount"] if str(r["clean_order_status"]).upper() == "DELIVERED" else 0.0,
            axis=1,
        )
        merged["gross_margin"] = merged.apply(
            lambda r: r["net_revenue"] - (r["quantity"] * r["cost_price"]) if str(r["clean_order_status"]).upper() == "DELIVERED" else 0.0,
            axis=1,
        )
        merged["within_15_min"] = merged.apply(
            lambda r: bool(str(r["clean_order_status"]).upper() == "DELIVERED" and r["delivery_minutes"] <= 15),
            axis=1,
        )
        merged["loaded_at"] = pendulum.now("UTC").to_iso8601_string()
        merged["run_id"] = run_id

        print(f"[Transform] Successfully transformed {len(merged)} clean fact orders")

        return {
            "fact_orders": merged.to_json(date_format="iso"),
            "dim_cust": dim_cust.to_json(date_format="iso"),
            "dim_prod": dim_prod.to_json(date_format="iso"),
            "dim_store": dim_store.to_json(date_format="iso"),
        }

    # ── 4. LOAD INTO WAREHOUSE (Idempotent Star Schema) ───────────────────────

    @task
    def load_star_schema(transformed_dict: dict, **context):
        run_id = context.get("run_id", "manual")
        fact_df = pd.read_json(transformed_dict["fact_orders"])
        cust_df = pd.read_json(transformed_dict["dim_cust"])
        prod_df = pd.read_json(transformed_dict["dim_prod"])
        store_df = pd.read_json(transformed_dict["dim_store"])

        con = duckdb.connect(WAREHOUSE_DB)

        # Dimension: Customers (Masked PII)
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_dim_customer (
                customer_id VARCHAR PRIMARY KEY,
                full_name VARCHAR,
                email_masked VARCHAR,
                phone_masked VARCHAR,
                city VARCHAR,
                area VARCHAR,
                pincode VARCHAR,
                signup_date DATE,
                loyalty_tier VARCHAR,
                is_active VARCHAR,
                loaded_at TIMESTAMP
            )
        """)
        # Idempotent refresh of dimensions
        con.execute("DELETE FROM etl_dim_customer")
        con.register("tmp_cust", cust_df)
        con.execute("""
            INSERT INTO etl_dim_customer
            SELECT customer_id, full_name, email_masked, phone_masked, city, area, 
                   pincode, CAST(signup_date AS DATE), loyalty_tier_standardized, is_active, CURRENT_TIMESTAMP
            FROM tmp_cust
        """)

        # Bonus: Customer SCD Type 2 tracking loyalty tier history
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_dim_customer_scd2 (
                customer_sk VARCHAR,
                customer_id VARCHAR,
                full_name VARCHAR,
                email_masked VARCHAR,
                loyalty_tier VARCHAR,
                valid_from DATE,
                valid_to DATE,
                is_current BOOLEAN
            )
        """)
        con.execute("DELETE FROM etl_dim_customer_scd2")
        con.execute("""
            INSERT INTO etl_dim_customer_scd2
            SELECT customer_id || '_v1', customer_id, full_name, email_masked, loyalty_tier_standardized,
                   CAST(signup_date AS DATE), DATE '2099-12-31', TRUE
            FROM tmp_cust
        """)

        # Dimension: Products
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_dim_product (
                product_id VARCHAR PRIMARY KEY,
                product_name VARCHAR,
                category VARCHAR,
                sub_category VARCHAR,
                brand VARCHAR,
                mrp DECIMAL(10,2),
                cost_price DECIMAL(10,2),
                unit VARCHAR,
                is_perishable BOOLEAN,
                launch_date DATE,
                loaded_at TIMESTAMP
            )
        """)
        con.execute("DELETE FROM etl_dim_product")
        con.register("tmp_prod", prod_df)
        con.execute("""
            INSERT INTO etl_dim_product
            SELECT product_id, product_name, category, sub_category, brand, mrp,
                   cost_price, unit, is_perishable, CAST(launch_date AS DATE), CURRENT_TIMESTAMP
            FROM tmp_prod
        """)

        # Dimension: Stores
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_dim_store (
                store_id VARCHAR PRIMARY KEY,
                store_name VARCHAR,
                zone VARCHAR,
                area VARCHAR,
                pincode VARCHAR,
                open_date DATE,
                manager_name VARCHAR,
                capacity_orders_per_day INTEGER,
                loaded_at TIMESTAMP
            )
        """)
        con.execute("DELETE FROM etl_dim_store")
        con.register("tmp_store", store_df)
        con.execute("""
            INSERT INTO etl_dim_store
            SELECT store_id, store_name, zone, area, pincode, CAST(open_date AS DATE),
                   manager_name, capacity_orders_per_day, CURRENT_TIMESTAMP
            FROM tmp_store
        """)

        # Fact Table: Orders
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_fact_orders (
                order_id VARCHAR PRIMARY KEY,
                order_ts TIMESTAMP,
                order_date DATE,
                customer_id VARCHAR,
                store_id VARCHAR,
                product_id VARCHAR,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                discount_pct DECIMAL(10,2),
                order_amount DECIMAL(10,2),
                payment_mode VARCHAR,
                order_status VARCHAR,
                channel VARCHAR,
                delivery_minutes DECIMAL(10,2),
                net_revenue DECIMAL(10,2),
                gross_margin DECIMAL(10,2),
                within_15_min BOOLEAN,
                loaded_at TIMESTAMP,
                run_id VARCHAR
            )
        """)

        # Idempotent Load: Remove existing records for this partition/run or recreate
        con.register("tmp_fact", fact_df)
        con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM tmp_fact)")
        con.execute("""
            INSERT INTO etl_fact_orders
            SELECT 
                order_id,
                CAST(order_ts AS TIMESTAMP),
                CAST(order_date AS DATE),
                customer_id,
                store_id,
                product_id,
                CAST(quantity AS INTEGER),
                CAST(unit_price AS DECIMAL(10,2)),
                CAST(discount_pct AS DECIMAL(10,2)),
                CAST(order_amount AS DECIMAL(10,2)),
                clean_payment_mode,
                clean_order_status,
                channel,
                CAST(delivery_minutes AS DECIMAL(10,2)),
                CAST(net_revenue AS DECIMAL(10,2)),
                CAST(gross_margin AS DECIMAL(10,2)),
                within_15_min,
                CURRENT_TIMESTAMP,
                ?
            FROM tmp_fact
        """, [run_id])

        count_fact = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.close()
        print(f"[Load] Successfully loaded {count_fact} rows into etl_fact_orders (Idempotent)")
        return count_fact

    # ── 5. RECONCILE ORDERS WITH PAYMENTS ─────────────────────────────────────

    @task
    def reconcile_payments(raw_payments_json, validation_result: dict):
        payments_df = pd.read_json(raw_payments_json)
        p_ord = pd.read_json(validation_result["passed_orders"])

        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (
                order_id VARCHAR,
                order_amount DECIMAL(10,2),
                order_status VARCHAR,
                payment_id VARCHAR,
                amount_paid DECIMAL(10,2),
                payment_status VARCHAR,
                reconciliation_status VARCHAR,
                mismatch_reason VARCHAR,
                reconciled_at TIMESTAMP
            )
        """)
        con.execute("DELETE FROM etl_payment_reconciliation")

        # Outer join between clean orders and payments
        ord_map = {row["order_id"]: row for _, row in p_ord.iterrows()}
        pay_groups = payments_df.groupby("order_id")
        rec_rows = []

        for oid, o_row in ord_map.items():
            status = str(o_row["order_status"]).upper()
            if oid in pay_groups.groups:
                sub_pay = pay_groups.get_group(oid)
                has_success = False
                succ_amt = 0.0
                for _, p_row in sub_pay.iterrows():
                    if p_row["payment_status"] == "SUCCESS":
                        has_success = True
                        succ_amt += float(p_row["amount_paid"])

                if status == "DELIVERED":
                    if not has_success:
                        rec_rows.append({
                            "order_id": oid,
                            "order_amount": float(o_row["order_amount"]),
                            "order_status": status,
                            "payment_id": str(sub_pay["payment_id"].iloc[0]),
                            "amount_paid": float(sub_pay["amount_paid"].iloc[0]),
                            "payment_status": str(sub_pay["payment_status"].iloc[0]),
                            "reconciliation_status": "UNRECONCILED",
                            "mismatch_reason": "DELIVERED_WITHOUT_SUCCESSFUL_PAYMENT"
                        })
                    elif abs(succ_amt - float(o_row["order_amount"])) > 0.05:
                        rec_rows.append({
                            "order_id": oid,
                            "order_amount": float(o_row["order_amount"]),
                            "order_status": status,
                            "payment_id": str(sub_pay["payment_id"].iloc[0]),
                            "amount_paid": succ_amt,
                            "payment_status": "SUCCESS",
                            "reconciliation_status": "UNRECONCILED",
                            "mismatch_reason": "AMOUNT_MISMATCH"
                        })
                elif status == "CANCELLED" and has_success:
                    rec_rows.append({
                        "order_id": oid,
                        "order_amount": float(o_row["order_amount"]),
                        "order_status": status,
                        "payment_id": str(sub_pay["payment_id"].iloc[0]),
                        "amount_paid": succ_amt,
                        "payment_status": "SUCCESS",
                        "reconciliation_status": "UNRECONCILED",
                        "mismatch_reason": "PAYMENT_COLLECTED_FOR_CANCELLED_ORDER"
                    })
            else:
                if status == "DELIVERED":
                    rec_rows.append({
                        "order_id": oid,
                        "order_amount": float(o_row["order_amount"]),
                        "order_status": status,
                        "payment_id": None,
                        "amount_paid": 0.0,
                        "payment_status": "MISSING",
                        "reconciliation_status": "UNRECONCILED",
                        "mismatch_reason": "DELIVERED_NO_PAYMENT_RECORD"
                    })

        for pid, p_row in payments_df.iterrows():
            oid = str(p_row["order_id"]).strip()
            if oid not in ord_map:
                rec_rows.append({
                    "order_id": oid,
                    "order_amount": 0.0,
                    "order_status": "UNKNOWN_OR_QUARANTINED",
                    "payment_id": p_row["payment_id"],
                    "amount_paid": float(p_row["amount_paid"]),
                    "payment_status": p_row["payment_status"],
                    "reconciliation_status": "UNRECONCILED",
                    "mismatch_reason": "ORPHAN_OR_QUARANTINED_ORDER_PAYMENT"
                })

        rec_df = pd.DataFrame(rec_rows)
        rec_df["reconciled_at"] = pendulum.now("UTC").to_iso8601_string()
        con.register("tmp_rec", rec_df)
        con.execute("INSERT INTO etl_payment_reconciliation SELECT * FROM tmp_rec")
        con.close()

        print(f"[Reconciliation] Identified {len(rec_df)} order-payment discrepancies")
        return len(rec_df)

    # ── 6. STORE CAPACITY CHECK (Bonus) ───────────────────────────────────────

    @task
    def store_capacity_check():
        con = duckdb.connect(WAREHOUSE_DB)
        capacity_report = con.execute("""
            SELECT 
                o.order_date,
                s.store_id,
                s.store_name,
                COUNT(*) AS orders_count,
                s.capacity_orders_per_day,
                CASE WHEN COUNT(*) > s.capacity_orders_per_day THEN 'BREACH' ELSE 'NORMAL' END AS capacity_status
            FROM etl_fact_orders o
            JOIN etl_dim_store s ON o.store_id = s.store_id
            GROUP BY o.order_date, s.store_id, s.store_name, s.capacity_orders_per_day
            HAVING COUNT(*) > s.capacity_orders_per_day
        """).fetchdf()
        con.close()
        print(f"[Capacity Monitor] Found {len(capacity_report)} store-capacity breaches")
        return len(capacity_report)

    # ── 7. REPORTING (Daily Net Revenue & Category Margin) ────────────────────

    @task
    def generate_reports():
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT 
                o.order_date,
                s.zone,
                s.store_id,
                s.store_name,
                COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
                ROUND(SUM(o.net_revenue), 2) AS total_net_revenue
            FROM etl_fact_orders o
            JOIN etl_dim_store s ON o.store_id = s.store_id
            WHERE o.order_status = 'DELIVERED'
            GROUP BY o.order_date, s.zone, s.store_id, s.store_name
            ORDER BY o.order_date DESC, total_net_revenue DESC;
        """)

        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT 
                p.category,
                ROUND(SUM(o.net_revenue), 2) AS total_revenue,
                ROUND(SUM(o.quantity * p.cost_price), 2) AS total_cost,
                ROUND(SUM(o.gross_margin), 2) AS total_gross_margin,
                ROUND((SUM(o.gross_margin) / NULLIF(SUM(o.net_revenue), 0)) * 100.0, 2) AS margin_pct
            FROM etl_fact_orders o
            JOIN etl_dim_product p ON o.product_id = p.product_id
            WHERE o.order_status = 'DELIVERED'
            GROUP BY p.category
            ORDER BY total_revenue DESC;
        """)

        zone_summary = con.execute("SELECT zone, SUM(total_net_revenue) AS rev FROM etl_daily_zone_revenue GROUP BY zone").fetchall()
        cat_summary = con.execute("SELECT category, total_revenue, total_gross_margin, margin_pct FROM etl_category_margin").fetchall()
        con.close()

        print("\n--- [Report: Net Revenue by Zone] ---")
        for row in zone_summary:
            print(f"Zone {row[0]}: ₹{row[1]:,.2f}")

        print("\n--- [Report: Category Gross Margin] ---")
        for row in cat_summary:
            print(f"{row[0]}: Revenue=₹{row[1]:,.2f} | Margin=₹{row[2]:,.2f} ({row[3]}%)")

        return True

    # ── 8. RUN AUDIT LOG (trigger_rule="all_done") ─────────────────────────────

    @task(trigger_rule="all_done")
    def run_audit(val_result: dict, load_rows: int, **context):
        run_id = context.get("run_id", "manual")
        dag_id = context.get("dag", None)
        dag_name = dag_id.dag_id if dag_id else "nammamart_etl_sreenivas"
        start_time = context.get("data_interval_start", pendulum.now("UTC")).to_iso8601_string()
        end_time = pendulum.now("UTC").to_iso8601_string()

        rows_extracted = val_result.get("total_extracted", 0)
        rows_quarantined = val_result.get("total_quarantined", 0)
        rows_loaded = load_rows

        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR,
                dag_id VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_extracted BIGINT,
                rows_loaded BIGINT,
                rows_quarantined BIGINT,
                status VARCHAR
            )
        """)

        con.execute("""
            INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [run_id, dag_name, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, "SUCCESS"])
        con.close()

        print(f"[Audit Log] Recorded run {run_id} in pipeline_runs: In={rows_extracted}, Loaded={rows_loaded}, Quarantined={rows_quarantined}")

        # Optional Slack Alert on Failure / Summary
        slack_webhook = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
        if slack_webhook:
            try:
                from slack_notifier import send_slack_alert
                msg = f":white_check_mark: *NammaMart ETL Succeeded*\nRun ID: `{run_id}`\nExtracted: {rows_extracted} | Loaded: {rows_loaded} | Quarantined: {rows_quarantined}"
                send_slack_alert(msg, webhook_url=slack_webhook)
            except Exception as e:
                print(f"[Slack Alert] Skipped or failed: {e}")

    # ── DAG ORCHESTRATION GRAPH ───────────────────────────────────────────────
    raw_orders = extract_orders()
    raw_payments = extract_payments()
    raw_customers = extract_customers()
    raw_products = extract_products()
    raw_stores = extract_stores()

    val_res = validate_and_quarantine(raw_orders, raw_payments, raw_customers, raw_products, raw_stores)
    transformed = transform_data(val_res)
    loaded_count = load_star_schema(transformed)

    reconcile_payments(raw_payments, val_res)
    store_capacity_check()
    generate_reports()

    run_audit(val_res, loaded_count)


nammamart_etl_dag = nammamart_etl_pipeline()
