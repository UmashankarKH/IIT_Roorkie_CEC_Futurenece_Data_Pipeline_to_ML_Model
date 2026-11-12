"""
dags/nammamart_etl_chandan.py
-------------------------------
NammaMart Quick Commerce — ETL Pipeline
Author : Chandan
Schedule: 9:00 AM IST, Monday–Saturday (cron 30 3 * * 1-6  i.e. 03:30 UTC)

Flow:
  extract_orders ──┐
  extract_payments──┤
  extract_customers─┼──► validate_and_quarantine ──► transform ──► load_star_schema
  extract_products──┤                                              ──► reconcile_payments
  extract_stores ──┘                                              ──► build_reports
                                                                  ──► audit_run  (trigger_rule=all_done)

Design notes
─────────────
• ETL: bad rows NEVER reach the warehouse; they go to quarantine with failure_reason.
• Idempotent load: DELETE WHERE run_date = <today> before INSERT, so re-runs don't double-count.
• IST = UTC+5:30  →  9:00 AM IST = 03:30 UTC  →  cron "30 3 * * 1-6"
• catchup=False: we don't want to backfill historical runs when the DAG is first enabled;
  we only care about today's report, not every missed day.
"""

import sys
import os
sys.path.insert(0, "/usr/local/airflow/include")

from airflow.decorators import dag, task
import pendulum
from datetime import timedelta
import pandas as pd
import duckdb
import json
import re

# ─── paths ────────────────────────────────────────────────────────────────────
DATA_DIR       = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE_DB   = "/usr/local/airflow/include/warehouse_chandan.duckdb"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/chandan"

# ─── default_args ─────────────────────────────────────────────────────────────
default_args = {
    "owner":             "chandan",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id   = "nammamart_etl_chandan",
    # 9:00 AM IST = 03:30 UTC  (IST is UTC+5:30)
    schedule = "30 3 * * 1-6",
    start_date = pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup  = False,
    default_args = default_args,
    tags     = ["nammamart", "etl", "chandan"],
    doc_md   = __doc__,
)
def nammamart_etl_chandan():

    # ══════════════════════════════════════════════════════════════════════════
    # EXTRACT — 5 parallel tasks
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def extract_orders(**context):
        path = f"{DATA_DIR}/orders.csv"
        df = pd.read_csv(path, dtype=str)
        print(f"[Extract] orders: {len(df)} rows from {path}")
        return {"data": df.to_json(orient="split"), "source": "orders.csv", "rows": len(df)}

    @task
    def extract_payments(**context):
        path = f"{DATA_DIR}/payments.csv"
        df = pd.read_csv(path, dtype=str)
        print(f"[Extract] payments: {len(df)} rows from {path}")
        return {"data": df.to_json(orient="split"), "source": "payments.csv", "rows": len(df)}

    @task
    def extract_customers(**context):
        path = f"{DATA_DIR}/customers.csv"
        df = pd.read_csv(path, dtype=str)
        print(f"[Extract] customers: {len(df)} rows from {path}")
        return {"data": df.to_json(orient="split"), "source": "customers.csv", "rows": len(df)}

    @task
    def extract_products(**context):
        path = f"{DATA_DIR}/products.csv"
        df = pd.read_csv(path, dtype=str)
        print(f"[Extract] products: {len(df)} rows from {path}")
        return {"data": df.to_json(orient="split"), "source": "products.csv", "rows": len(df)}

    @task
    def extract_stores(**context):
        path = f"{DATA_DIR}/stores.csv"
        df = pd.read_csv(path, dtype=str)
        print(f"[Extract] stores: {len(df)} rows from {path}")
        return {"data": df.to_json(orient="split"), "source": "stores.csv", "rows": len(df)}

    # ══════════════════════════════════════════════════════════════════════════
    # VALIDATE & QUARANTINE
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def validate_and_quarantine(orders_raw, payments_raw, customers_raw,
                                products_raw, stores_raw, **context):
        from nammamart_quality import (
            run_orders_checks, get_critical_failed_index,
            build_failure_reason_map,
            normalise_payment_mode, normalise_order_status, normalise_loyalty_tier,
            # individual checks
            payments_completeness, payments_uniqueness, payments_validity_mode,
            payments_consistency_order_id,
            customers_completeness, customers_uniqueness,
            customers_validity_email, customers_validity_phone,
            customers_validity_loyalty_tier, customers_timeliness_signup,
            products_completeness, products_uniqueness,
            products_accuracy_mrp, products_accuracy_cost_vs_mrp,
            products_timeliness_launch,
            stores_completeness, stores_uniqueness,
            stores_validity_zone, stores_accuracy_capacity,
        )

        run_id   = context["run_id"]
        run_date = context["ds"]

        os.makedirs(QUARANTINE_DIR, exist_ok=True)

        # Load DataFrames
        orders_df    = pd.read_json(orders_raw["data"],    orient="split", dtype=str)
        payments_df  = pd.read_json(payments_raw["data"],  orient="split", dtype=str)
        customers_df = pd.read_json(customers_raw["data"], orient="split", dtype=str)
        products_df  = pd.read_json(products_raw["data"],  orient="split", dtype=str)
        stores_df    = pd.read_json(stores_raw["data"],    orient="split", dtype=str)

        # Normalise before checks so casing issues don't cause false failures
        orders_df["order_status"]  = orders_df["order_status"].apply(normalise_order_status)
        orders_df["payment_mode"]  = orders_df["payment_mode"].apply(normalise_payment_mode)
        customers_df["loyalty_tier"] = customers_df["loyalty_tier"].apply(normalise_loyalty_tier)
        payments_df["payment_mode"]  = payments_df["payment_mode"].apply(normalise_payment_mode)

        # ── Orders DQ ────────────────────────────────────────────────────────
        order_checks = run_orders_checks(orders_df, customers_df, products_df, stores_df)
        order_failed_idx     = get_critical_failed_index(order_checks)
        order_reason_map     = build_failure_reason_map(order_checks)

        orders_passed = orders_df[~orders_df.index.isin(order_failed_idx)].copy()
        orders_failed = orders_df[orders_df.index.isin(order_failed_idx)].copy()
        orders_failed["failure_reason"] = orders_failed.index.map(order_reason_map)

        if len(orders_failed):
            qpath = f"{QUARANTINE_DIR}/orders_failed_{run_id[:20]}.csv"
            orders_failed.to_csv(qpath, index=False)
            print(f"[Quarantine] {len(orders_failed)} order rows → {qpath}")

        # ── Payments DQ ──────────────────────────────────────────────────────
        pay_checks = [
            payments_completeness(payments_df),
            payments_uniqueness(payments_df),
            payments_validity_mode(payments_df),
            payments_consistency_order_id(payments_df, orders_df),
        ]
        pay_failed_idx  = get_critical_failed_index(pay_checks)
        pay_reason_map  = build_failure_reason_map(pay_checks)
        payments_passed = payments_df[~payments_df.index.isin(pay_failed_idx)].copy()
        payments_failed = payments_df[payments_df.index.isin(pay_failed_idx)].copy()
        payments_failed["failure_reason"] = payments_failed.index.map(pay_reason_map)
        if len(payments_failed):
            qpath = f"{QUARANTINE_DIR}/payments_failed_{run_id[:20]}.csv"
            payments_failed.to_csv(qpath, index=False)

        # ── Customers DQ ─────────────────────────────────────────────────────
        cust_checks = [
            customers_completeness(customers_df),
            customers_uniqueness(customers_df),
            customers_validity_email(customers_df),
            customers_validity_phone(customers_df),
            customers_validity_loyalty_tier(customers_df),
            customers_timeliness_signup(customers_df),
        ]
        cust_failed_idx  = get_critical_failed_index(cust_checks)
        cust_reason_map  = build_failure_reason_map(cust_checks)
        customers_passed = customers_df[~customers_df.index.isin(cust_failed_idx)].copy()
        customers_failed = customers_df[customers_df.index.isin(cust_failed_idx)].copy()
        customers_failed["failure_reason"] = customers_failed.index.map(cust_reason_map)
        if len(customers_failed):
            qpath = f"{QUARANTINE_DIR}/customers_failed_{run_id[:20]}.csv"
            customers_failed.to_csv(qpath, index=False)

        # ── Products DQ ──────────────────────────────────────────────────────
        prod_checks = [
            products_completeness(products_df),
            products_uniqueness(products_df),
            products_accuracy_mrp(products_df),
            products_accuracy_cost_vs_mrp(products_df),
            products_timeliness_launch(products_df),
        ]
        prod_failed_idx  = get_critical_failed_index(prod_checks)
        prod_reason_map  = build_failure_reason_map(prod_checks)
        products_passed  = products_df[~products_df.index.isin(prod_failed_idx)].copy()
        products_failed  = products_df[products_df.index.isin(prod_failed_idx)].copy()
        products_failed["failure_reason"] = products_failed.index.map(prod_reason_map)
        if len(products_failed):
            qpath = f"{QUARANTINE_DIR}/products_failed_{run_id[:20]}.csv"
            products_failed.to_csv(qpath, index=False)

        # ── Stores DQ ────────────────────────────────────────────────────────
        store_checks = [
            stores_completeness(stores_df),
            stores_uniqueness(stores_df),
            stores_validity_zone(stores_df),
            stores_accuracy_capacity(stores_df),
        ]
        store_failed_idx = get_critical_failed_index(store_checks)
        store_reason_map = build_failure_reason_map(store_checks)
        stores_passed    = stores_df[~stores_df.index.isin(store_failed_idx)].copy()
        stores_failed    = stores_df[stores_df.index.isin(store_failed_idx)].copy()
        stores_failed["failure_reason"] = stores_failed.index.map(store_reason_map)
        if len(stores_failed):
            qpath = f"{QUARANTINE_DIR}/stores_failed_{run_id[:20]}.csv"
            stores_failed.to_csv(qpath, index=False)

        # ── Write dq_results ─────────────────────────────────────────────────
        all_checks = (
            [(c, "orders.csv") for c in order_checks]
            + [(c, "payments.csv") for c in pay_checks]
            + [(c, "customers.csv") for c in cust_checks]
            + [(c, "products.csv") for c in prod_checks]
            + [(c, "stores.csv") for c in store_checks]
        )
        checked_at = pendulum.now("UTC").isoformat()
        dq_rows = []
        for chk, src in all_checks:
            dq_rows.append({
                "run_id":       run_id,
                "dag_id":       "nammamart_etl_chandan",
                "source_file":  src,
                "check_name":   chk["check_name"],
                "dimension":    chk["dimension"],
                "severity":     chk["severity"],
                "rows_checked": chk["rows_checked"],
                "rows_failed":  chk["rows_failed"],
                "status":       "FAIL" if chk["rows_failed"] > 0 else "PASS",
                "checked_at":   checked_at,
            })
        dq_df = pd.DataFrame(dq_rows)
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id        VARCHAR,
                dag_id        VARCHAR,
                source_file   VARCHAR,
                check_name    VARCHAR,
                dimension     VARCHAR,
                severity      VARCHAR,
                rows_checked  INTEGER,
                rows_failed   INTEGER,
                status        VARCHAR,
                checked_at    VARCHAR
            )
        """)
        con.execute("INSERT INTO dq_results SELECT * FROM dq_df")
        con.close()

        summary = {
            "run_id":              run_id,
            "run_date":            run_date,
            "orders_passed":       len(orders_passed),
            "orders_quarantined":  len(orders_failed),
            "payments_passed":     len(payments_passed),
            "customers_passed":    len(customers_passed),
            "products_passed":     len(products_passed),
            "stores_passed":       len(stores_passed),
            "orders_json":         orders_passed.to_json(orient="split"),
            "payments_json":       payments_passed.to_json(orient="split"),
            "customers_json":      customers_passed.to_json(orient="split"),
            "products_json":       products_passed.to_json(orient="split"),
            "stores_json":         stores_passed.to_json(orient="split"),
        }
        print(f"[Validate] orders passed={len(orders_passed)}, quarantined={len(orders_failed)}")
        return summary

    # ══════════════════════════════════════════════════════════════════════════
    # TRANSFORM
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def transform(validated: dict, **context):
        from nammamart_quality import mask_email, mask_phone

        orders_df    = pd.read_json(validated["orders_json"],    orient="split")
        payments_df  = pd.read_json(validated["payments_json"],  orient="split")
        customers_df = pd.read_json(validated["customers_json"], orient="split")
        products_df  = pd.read_json(validated["products_json"],  orient="split")
        stores_df    = pd.read_json(validated["stores_json"],    orient="split")

        # ── Deduplicate dimensions ────────────────────────────────────────────
        customers_df = customers_df.drop_duplicates(subset=["customer_id"], keep="first")
        products_df  = products_df.drop_duplicates(subset=["product_id"],  keep="first")
        stores_df    = stores_df.drop_duplicates(subset=["store_id"],   keep="first")

        # ── Mask PII ─────────────────────────────────────────────────────────
        customers_df["email"] = customers_df["email"].apply(mask_email)
        customers_df["phone"] = customers_df["phone"].apply(mask_phone)

        # ── Normalise stores zone (fill missing as "Unknown") ─────────────────
        stores_df["zone"] = stores_df["zone"].fillna("Unknown").str.strip().str.title()

        # ── Cast types ───────────────────────────────────────────────────────
        orders_df["order_ts"]        = pd.to_datetime(orders_df["order_ts"])
        orders_df["order_date"]      = orders_df["order_ts"].dt.date.astype(str)
        orders_df["quantity"]        = pd.to_numeric(orders_df["quantity"],    errors="coerce")
        orders_df["unit_price"]      = pd.to_numeric(orders_df["unit_price"],  errors="coerce")
        orders_df["discount_pct"]    = pd.to_numeric(orders_df["discount_pct"],errors="coerce").fillna(0)
        orders_df["order_amount"]    = pd.to_numeric(orders_df["order_amount"],errors="coerce")
        orders_df["delivery_minutes"]= pd.to_numeric(orders_df["delivery_minutes"], errors="coerce")
        products_df["mrp"]           = pd.to_numeric(products_df["mrp"],        errors="coerce")
        products_df["cost_price"]    = pd.to_numeric(products_df["cost_price"], errors="coerce")

        # ── Derived columns ──────────────────────────────────────────────────
        orders_df["net_revenue"] = (
            orders_df["quantity"]
            * orders_df["unit_price"]
            * (1 - orders_df["discount_pct"] / 100)
        ).round(2)

        # Join products to get cost_price
        orders_products = orders_df.merge(
            products_df[["product_id", "cost_price", "category"]],
            on="product_id", how="left"
        )
        orders_products["gross_margin"] = (
            orders_products["net_revenue"]
            - orders_products["quantity"] * orders_products["cost_price"]
        ).round(2)

        orders_products["within_15_min"] = (
            orders_products["delivery_minutes"].fillna(999) <= 15
        ).astype(int)

        # Full join with all dimensions
        fact = orders_products.merge(
            customers_df[["customer_id", "full_name", "email", "phone",
                          "city", "area", "pincode", "signup_date",
                          "loyalty_tier", "is_active"]],
            on="customer_id", how="left"
        ).merge(
            stores_df[["store_id", "store_name", "zone", "area",
                       "manager_name", "capacity_orders_per_day"]],
            on="store_id", how="left", suffixes=("", "_store")
        )

        print(f"[Transform] fact rows: {len(fact)}")

        return {
            "fact_json":      fact.to_json(orient="split"),
            "customers_json": customers_df.to_json(orient="split"),
            "products_json":  products_df.to_json(orient="split"),
            "stores_json":    stores_df.to_json(orient="split"),
            "payments_json":  payments_df.to_json(orient="split"),
            "run_id":         validated["run_id"],
            "run_date":       validated["run_date"],
        }

    # ══════════════════════════════════════════════════════════════════════════
    # LOAD — star schema, idempotent
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def load_star_schema(transformed: dict, **context):
        fact_df      = pd.read_json(transformed["fact_json"],      orient="split")
        customers_df = pd.read_json(transformed["customers_json"], orient="split")
        products_df  = pd.read_json(transformed["products_json"],  orient="split")
        stores_df    = pd.read_json(transformed["stores_json"],    orient="split")
        run_id       = transformed["run_id"]
        run_date     = transformed["run_date"]

        con = duckdb.connect(WAREHOUSE_DB)

        # ── dim_customer ─────────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_dim_customer (
                customer_id   VARCHAR PRIMARY KEY,
                full_name     VARCHAR,
                email         VARCHAR,
                phone         VARCHAR,
                city          VARCHAR,
                area          VARCHAR,
                pincode       VARCHAR,
                signup_date   VARCHAR,
                loyalty_tier  VARCHAR,
                is_active     VARCHAR,
                _loaded_at    VARCHAR
            )
        """)
        customers_df["_loaded_at"] = pendulum.now("UTC").isoformat()
        # Upsert customers
        con.execute("CREATE OR REPLACE TEMP TABLE tmp_cust AS SELECT * FROM customers_df")
        con.execute("""
            DELETE FROM etl_dim_customer
            WHERE customer_id IN (SELECT customer_id FROM tmp_cust)
        """)
        con.execute("INSERT INTO etl_dim_customer SELECT * FROM tmp_cust")

        # ── dim_product ──────────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_dim_product (
                product_id    VARCHAR PRIMARY KEY,
                product_name  VARCHAR,
                category      VARCHAR,
                sub_category  VARCHAR,
                brand         VARCHAR,
                mrp           DOUBLE,
                cost_price    DOUBLE,
                unit          VARCHAR,
                is_perishable VARCHAR,
                launch_date   VARCHAR,
                _loaded_at    VARCHAR
            )
        """)
        products_df["_loaded_at"] = pendulum.now("UTC").isoformat()
        con.execute("CREATE OR REPLACE TEMP TABLE tmp_prod AS SELECT * FROM products_df")
        con.execute("DELETE FROM etl_dim_product WHERE product_id IN (SELECT product_id FROM tmp_prod)")
        con.execute("INSERT INTO etl_dim_product SELECT * FROM tmp_prod")

        # ── dim_store ────────────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_dim_store (
                store_id                VARCHAR PRIMARY KEY,
                store_name              VARCHAR,
                zone                    VARCHAR,
                area                    VARCHAR,
                manager_name            VARCHAR,
                capacity_orders_per_day VARCHAR,
                _loaded_at              VARCHAR
            )
        """)
        stores_load = stores_df[["store_id","store_name","zone","area",
                                  "manager_name","capacity_orders_per_day"]].copy()
        stores_load["_loaded_at"] = pendulum.now("UTC").isoformat()
        con.execute("CREATE OR REPLACE TEMP TABLE tmp_store AS SELECT * FROM stores_load")
        con.execute("DELETE FROM etl_dim_store WHERE store_id IN (SELECT store_id FROM tmp_store)")
        con.execute("INSERT INTO etl_dim_store SELECT * FROM tmp_store")

        # ── fact_orders ──────────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_fact_orders (
                order_id         VARCHAR,
                order_ts         VARCHAR,
                order_date       VARCHAR,
                customer_id      VARCHAR,
                store_id         VARCHAR,
                product_id       VARCHAR,
                category         VARCHAR,
                quantity         DOUBLE,
                unit_price       DOUBLE,
                discount_pct     DOUBLE,
                order_amount     DOUBLE,
                net_revenue      DOUBLE,
                gross_margin     DOUBLE,
                payment_mode     VARCHAR,
                order_status     VARCHAR,
                channel          VARCHAR,
                delivery_minutes DOUBLE,
                within_15_min    INTEGER,
                loyalty_tier     VARCHAR,
                zone             VARCHAR,
                store_name       VARCHAR,
                _run_id          VARCHAR,
                _loaded_at       VARCHAR
            )
        """)
        # Idempotent: delete rows from today's run_date before reinserting
        con.execute(f"DELETE FROM etl_fact_orders WHERE order_date = '{run_date}'")

        load_cols = [
            "order_id","order_ts","order_date","customer_id","store_id","product_id",
            "category","quantity","unit_price","discount_pct","order_amount",
            "net_revenue","gross_margin","payment_mode","order_status","channel",
            "delivery_minutes","within_15_min","loyalty_tier","zone","store_name"
        ]
        # Fill any missing columns
        for c in load_cols:
            if c not in fact_df.columns:
                fact_df[c] = None

        fact_df["order_ts"] = fact_df["order_ts"].astype(str)
        fact_load = fact_df[load_cols].copy()
        fact_load["_run_id"]    = run_id
        fact_load["_loaded_at"] = pendulum.now("UTC").isoformat()

        con.execute("INSERT INTO etl_fact_orders SELECT * FROM fact_load")

        n = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        print(f"[Load] etl_fact_orders total rows: {n}")
        con.close()

        return transformed

    # ══════════════════════════════════════════════════════════════════════════
    # RECONCILE PAYMENTS
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def reconcile_payments(transformed: dict, **context):
        fact_df    = pd.read_json(transformed["fact_json"],    orient="split")
        payments_df= pd.read_json(transformed["payments_json"],orient="split")
        run_id     = transformed["run_id"]

        payments_df["amount_paid"] = pd.to_numeric(payments_df["amount_paid"], errors="coerce")

        # DELIVERED orders should have a SUCCESS payment
        delivered = fact_df[fact_df["order_status"] == "DELIVERED"][
            ["order_id","order_amount","payment_mode"]
        ].copy()
        delivered["order_amount"] = pd.to_numeric(delivered["order_amount"], errors="coerce")

        success_pay = payments_df[payments_df["payment_status"].str.strip().str.upper() == "SUCCESS"][
            ["order_id","amount_paid","payment_status","payment_mode"]
        ]

        merged = delivered.merge(success_pay, on="order_id", how="left",
                                  suffixes=("_order","_payment"))

        mismatches = []

        # 1. DELIVERED order with no payment
        no_pay = merged[merged["amount_paid"].isna()]
        for _, row in no_pay.iterrows():
            mismatches.append({
                "order_id":        row["order_id"],
                "order_amount":    row["order_amount"],
                "amount_paid":     None,
                "difference":      None,
                "mismatch_reason": "DELIVERED order has no SUCCESS payment",
                "run_id":          run_id,
            })

        # 2. Amount mismatch
        has_pay = merged[~merged["amount_paid"].isna()]
        diff = (has_pay["order_amount"] - has_pay["amount_paid"]).abs()
        amt_mismatch = has_pay[diff > 0.05]
        for _, row in amt_mismatch.iterrows():
            mismatches.append({
                "order_id":        row["order_id"],
                "order_amount":    row["order_amount"],
                "amount_paid":     row["amount_paid"],
                "difference":      round(row["order_amount"] - row["amount_paid"], 2),
                "mismatch_reason": "Amount mismatch between order and payment",
                "run_id":          run_id,
            })

        # 3. Orphan payments (payment with no matching order)
        orphan_pay = payments_df[~payments_df["order_id"].isin(fact_df["order_id"])]
        for _, row in orphan_pay.iterrows():
            mismatches.append({
                "order_id":        row["order_id"],
                "order_amount":    None,
                "amount_paid":     row["amount_paid"],
                "difference":      None,
                "mismatch_reason": "Payment has no matching order",
                "run_id":          run_id,
            })

        recon_df = pd.DataFrame(mismatches)

        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (
                order_id        VARCHAR,
                order_amount    DOUBLE,
                amount_paid     DOUBLE,
                difference      DOUBLE,
                mismatch_reason VARCHAR,
                run_id          VARCHAR
            )
        """)
        con.execute(f"DELETE FROM etl_payment_reconciliation WHERE run_id = '{run_id}'")
        if len(recon_df):
            con.execute("INSERT INTO etl_payment_reconciliation SELECT * FROM recon_df")

        total_mismatch = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
        con.close()
        print(f"[Reconcile] {len(mismatches)} mismatches logged (total: {total_mismatch})")
        return transformed

    # ══════════════════════════════════════════════════════════════════════════
    # REPORT
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def build_reports(**context):
        con = duckdb.connect(WAREHOUSE_DB)

        # ── Daily zone revenue ────────────────────────────────────────────────
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT
                order_date,
                zone,
                store_id,
                store_name,
                COUNT(*)                       AS order_count,
                ROUND(SUM(net_revenue), 2)     AS net_revenue,
                ROUND(SUM(gross_margin), 2)    AS gross_margin
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY order_date, zone, store_id, store_name
            ORDER BY order_date, zone, store_id
        """)

        # ── Category margin ───────────────────────────────────────────────────
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT
                category,
                COUNT(*)                       AS order_count,
                ROUND(SUM(net_revenue), 2)     AS net_revenue,
                ROUND(SUM(gross_margin), 2)    AS gross_margin
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY category
            ORDER BY net_revenue DESC
        """)

        # ── Delivery SLA ──────────────────────────────────────────────────────
        con.execute("""
            CREATE OR REPLACE TABLE etl_delivery_sla AS
            SELECT
                store_id,
                store_name,
                zone,
                COUNT(*)                                                AS delivered_orders,
                ROUND(AVG(delivery_minutes), 2)                        AS avg_delivery_min,
                ROUND(100.0 * SUM(within_15_min) / COUNT(*), 2)       AS pct_within_15_min
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY store_id, store_name, zone
            ORDER BY avg_delivery_min
        """)

        # ── Top customers ─────────────────────────────────────────────────────
        con.execute("""
            CREATE OR REPLACE TABLE etl_top_customers AS
            SELECT
                customer_id,
                loyalty_tier,
                COUNT(*)                   AS order_count,
                ROUND(SUM(net_revenue), 2) AS total_spend
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY customer_id, loyalty_tier
            ORDER BY total_spend DESC
            LIMIT 10
        """)

        zone_rev = con.execute("SELECT * FROM etl_daily_zone_revenue ORDER BY net_revenue DESC LIMIT 5").fetchdf()
        cat_marg = con.execute("SELECT * FROM etl_category_margin").fetchdf()
        print(f"\n[Report] Top Zone Revenue:\n{zone_rev.to_string(index=False)}")
        print(f"\n[Report] Category Margin:\n{cat_marg.to_string(index=False)}")
        con.close()

    # ══════════════════════════════════════════════════════════════════════════
    # INCREMENTAL LOAD (October increment file)
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def load_increment(**context):
        """Load orders_2026-10-01_increment.csv: append new, update changed."""
        from nammamart_quality import (
            normalise_payment_mode, normalise_order_status,
            mask_email, mask_phone,
        )

        inc_path = f"{DATA_DIR}/orders_2026-10-01_increment.csv"
        if not os.path.exists(inc_path):
            print("[Increment] No increment file found, skipping.")
            return

        inc_df = pd.read_csv(inc_path, dtype=str)
        inc_df["order_status"] = inc_df["order_status"].apply(normalise_order_status)
        inc_df["payment_mode"] = inc_df["payment_mode"].apply(normalise_payment_mode)
        inc_df["order_ts"]     = pd.to_datetime(inc_df["order_ts"]).astype(str)
        inc_df["order_date"]   = pd.to_datetime(inc_df["order_ts"]).dt.date.astype(str)
        inc_df["quantity"]     = pd.to_numeric(inc_df["quantity"],    errors="coerce")
        inc_df["unit_price"]   = pd.to_numeric(inc_df["unit_price"],  errors="coerce")
        inc_df["discount_pct"] = pd.to_numeric(inc_df["discount_pct"],errors="coerce").fillna(0)
        inc_df["net_revenue"]  = (inc_df["quantity"] * inc_df["unit_price"]
                                   * (1 - inc_df["discount_pct"] / 100)).round(2)
        inc_df["order_amount"] = pd.to_numeric(inc_df["order_amount"], errors="coerce")
        inc_df["delivery_minutes"] = pd.to_numeric(inc_df["delivery_minutes"], errors="coerce")
        inc_df["gross_margin"] = None  # no product cost in this simplified path
        inc_df["within_15_min"] = (inc_df["delivery_minutes"].fillna(999) <= 15).astype(int)
        inc_df["loyalty_tier"] = None
        inc_df["zone"]         = None
        inc_df["store_name"]   = None

        run_id = context["run_id"]
        inc_df["_run_id"]    = run_id
        inc_df["_loaded_at"] = pendulum.now("UTC").isoformat()

        load_cols = [
            "order_id","order_ts","order_date","customer_id","store_id","product_id",
            "category","quantity","unit_price","discount_pct","order_amount",
            "net_revenue","gross_margin","payment_mode","order_status","channel",
            "delivery_minutes","within_15_min","loyalty_tier","zone","store_name",
            "_run_id","_loaded_at"
        ]
        for c in load_cols:
            if c not in inc_df.columns:
                inc_df[c] = None

        inc_load = inc_df[load_cols]

        con = duckdb.connect(WAREHOUSE_DB)
        # Upsert: delete existing order_ids then insert
        existing_ids = tuple(inc_df["order_id"].tolist())
        if existing_ids:
            placeholders = ",".join(f"'{x}'" for x in existing_ids)
            con.execute(f"DELETE FROM etl_fact_orders WHERE order_id IN ({placeholders})")
        con.execute("INSERT INTO etl_fact_orders SELECT * FROM inc_load")
        total = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.close()
        print(f"[Increment] {len(inc_df)} rows upserted. etl_fact_orders total: {total}")

    # ══════════════════════════════════════════════════════════════════════════
    # AUDIT RUN
    # ══════════════════════════════════════════════════════════════════════════

    @task(trigger_rule="all_done")
    def audit_run(validated_summary: dict, **context):
        run_id     = context["run_id"]
        dag_id     = context["dag"].dag_id
        start_time = context["data_interval_start"].isoformat()
        end_time   = pendulum.now("UTC").isoformat()

        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id           VARCHAR,
                dag_id           VARCHAR,
                start_time       VARCHAR,
                end_time         VARCHAR,
                rows_extracted   INTEGER,
                rows_loaded      INTEGER,
                rows_quarantined INTEGER,
                status           VARCHAR
            )
        """)
        rows_extracted   = (validated_summary.get("orders_passed", 0)
                            + validated_summary.get("orders_quarantined", 0))
        rows_loaded      = validated_summary.get("orders_passed", 0)
        rows_quarantined = validated_summary.get("orders_quarantined", 0)

        try:
            ti_state = context["ti"].state
            status = "SUCCESS" if ti_state == "success" else "PARTIAL"
        except Exception:
            status = "SUCCESS"

        audit_df = pd.DataFrame([{
            "run_id":           run_id,
            "dag_id":           dag_id,
            "start_time":       start_time,
            "end_time":         end_time,
            "rows_extracted":   rows_extracted,
            "rows_loaded":      rows_loaded,
            "rows_quarantined": rows_quarantined,
            "status":           status,
        }])
        con.execute("INSERT INTO pipeline_runs SELECT * FROM audit_df")
        print(f"[Audit] Run logged: {run_id} | status={status} | loaded={rows_loaded} | quarantined={rows_quarantined}")
        con.close()

    # ══════════════════════════════════════════════════════════════════════════
    # DAG WIRING
    # ══════════════════════════════════════════════════════════════════════════

    orders_raw    = extract_orders()
    payments_raw  = extract_payments()
    customers_raw = extract_customers()
    products_raw  = extract_products()
    stores_raw    = extract_stores()

    validated = validate_and_quarantine(
        orders_raw, payments_raw, customers_raw, products_raw, stores_raw
    )

    transformed = transform(validated)
    loaded      = load_star_schema(transformed)
    reconciled  = reconcile_payments(loaded)
    reports     = build_reports()
    increment   = load_increment()

    reconciled >> reports
    loaded     >> increment

    audit_run(validated)


nammamart_etl_chandan()
