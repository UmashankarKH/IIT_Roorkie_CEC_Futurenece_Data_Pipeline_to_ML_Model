"""
include/nammamart_etl.py
------------------------
Core ETL Pipeline logic for NammaMart.
Extract -> Validate -> Transform -> Load (Star Schema) -> Reconcile -> Report.

Target Warehouse: include/warehouse_rajaselvan_thangaraj.duckdb
Quarantine Dir:   include/quarantine/rajaselvan_thangaraj/
"""

import os
import duckdb
import pandas as pd
import numpy as np
import pendulum
from nammamart_quality import (
    validate_orders, validate_customers, validate_products,
    validate_stores, validate_payments
)
from nammamart_masking import mask_email, mask_phone

DEFAULT_WAREHOUSE = "/usr/local/airflow/include/warehouse_rajaselvan_thangaraj.duckdb"
DEFAULT_QUARANTINE = "/usr/local/airflow/include/quarantine/rajaselvan_thangaraj"
DEFAULT_DATA_DIR = "/usr/local/airflow/include/data/nammamart"


def get_paths():
    """Resolves local vs container paths dynamically."""
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, "include")):
        base_dir = os.path.join(cwd, "include")
    else:
        base_dir = "/usr/local/airflow/include"
        
    warehouse_db = os.path.join(base_dir, "warehouse_rajaselvan_thangaraj.duckdb")
    quarantine_dir = os.path.join(base_dir, "quarantine", "rajaselvan_thangaraj")
    data_dir = os.path.join(base_dir, "data", "nammamart")
    return warehouse_db, quarantine_dir, data_dir


def standardize_payment_mode(mode: str) -> str:
    """Standardizes payment mode casing and typos."""
    if pd.isna(mode):
        return "Unknown"
    m = str(mode).strip()
    m_lower = m.lower()
    if m_lower in ('upi', 'upi '):
        return 'UPI'
    elif m_lower in ('card',):
        return 'Card'
    elif m_lower in ('cash on delivery', 'cod', 'cashh'):
        return 'Cash on Delivery'
    elif m_lower in ('wallet',):
        return 'Wallet'
    return m.title()


def standardize_loyalty_tier(tier: str) -> str:
    """Standardizes loyalty tier casing."""
    if pd.isna(tier):
        return "Bronze"
    t = str(tier).strip().lower()
    if t == 'bronze': return 'Bronze'
    if t == 'silver': return 'Silver'
    if t == 'gold': return 'Gold'
    if t == 'platinum': return 'Platinum'
    return t.title()


def run_etl_pipeline(run_id: str = None, dag_id: str = "nammamart_etl_rajaselvan_thangaraj"):
    """
    Executes the entire ETL pipeline from extract to loading, reconciliation, and reporting.
    Returns summary statistics dict.
    """
    if not run_id:
        run_id = f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}"

    warehouse_db, quarantine_dir, data_dir = get_paths()
    os.makedirs(quarantine_dir, exist_ok=True)
    os.makedirs(os.path.dirname(warehouse_db), exist_ok=True)

    print(f"=== [ETL Pipeline Start] run_id={run_id} ===")
    con = duckdb.connect(warehouse_db)

    # ── 1. EXTRACT ────────────────────────────────────────────────
    raw_orders = pd.read_csv(os.path.join(data_dir, "orders.csv"))
    raw_payments = pd.read_csv(os.path.join(data_dir, "payments.csv"))
    raw_customers = pd.read_csv(os.path.join(data_dir, "customers.csv"))
    raw_products = pd.read_csv(os.path.join(data_dir, "products.csv"))
    raw_stores = pd.read_csv(os.path.join(data_dir, "stores.csv"))

    rows_extracted = len(raw_orders) + len(raw_payments) + len(raw_customers) + len(raw_products) + len(raw_stores)
    print(f"[Extract] Loaded {len(raw_orders)} orders, {len(raw_payments)} payments, {len(raw_customers)} customers, {len(raw_products)} products, {len(raw_stores)} stores")

    # ── 2. VALIDATE ───────────────────────────────────────────────
    # Dimension validations (record warnings/checks to dq_results)
    clean_cust = validate_customers(raw_customers, run_id, dag_id, con)
    clean_prod = validate_products(raw_products, run_id, dag_id, con)
    clean_stores = validate_stores(raw_stores, run_id, dag_id, con)
    clean_pay = validate_payments(raw_payments, raw_orders, run_id, dag_id, con)

    # Orders validation (splits into passed and quarantined)
    passed_orders, failed_orders = validate_orders(
        raw_orders, clean_cust, clean_prod, clean_stores, run_id, dag_id, con
    )

    # ── 3. QUARANTINE ─────────────────────────────────────────────
    quarantine_path = os.path.join(quarantine_dir, f"orders_failed_{run_id}.csv")
    if len(failed_orders) > 0:
        failed_orders.to_csv(quarantine_path, index=False)
        print(f"[Quarantine] Wrote {len(failed_orders)} failed rows with failure_reason to {quarantine_path}")
    else:
        print("[Quarantine] 0 failed rows to quarantine.")

    # ── 4. TRANSFORM ──────────────────────────────────────────────
    # A. Standardize & Deduplicate Dimensions
    # Customers: deduplicate on customer_id (keep first/latest), standardize loyalty_tier, mask PII
    dim_cust = clean_cust.drop_duplicates(subset=['customer_id'], keep='first').copy()
    dim_cust['loyalty_tier'] = dim_cust['loyalty_tier'].apply(standardize_loyalty_tier)
    dim_cust['masked_email'] = dim_cust['email'].apply(mask_email)
    dim_cust['masked_phone'] = dim_cust['phone'].apply(mask_phone)
    dim_cust = dim_cust[['customer_id', 'full_name', 'masked_email', 'masked_phone',
                         'city', 'area', 'pincode', 'signup_date', 'loyalty_tier', 'is_active']]

    # Products: deduplicate on product_id (ignore (old) variants), fill missing category if any
    dim_prod = clean_prod.drop_duplicates(subset=['product_id'], keep='first').copy()
    dim_prod['category'] = dim_prod['category'].fillna('Unknown')
    dim_prod = dim_prod[['product_id', 'product_name', 'category', 'sub_category',
                         'brand', 'mrp', 'cost_price', 'unit', 'is_perishable', 'launch_date']]

    # Stores: impute missing zone for S07 (BTM Layout is South zone)
    dim_store = clean_stores.copy()
    dim_store.loc[dim_store['store_id'] == 'S07', 'zone'] = 'South'
    dim_store = dim_store[['store_id', 'store_name', 'zone', 'area', 'pincode',
                           'open_date', 'manager_name', 'capacity_orders_per_day']]

    # B. Transform Orders
    fact_orders = passed_orders.copy()
    fact_orders['order_status'] = fact_orders['order_status'].str.strip().str.upper()
    fact_orders['payment_mode'] = fact_orders['payment_mode'].apply(standardize_payment_mode)
    fact_orders['order_date'] = pd.to_datetime(fact_orders['order_ts']).dt.date.astype(str)

    # Join with product to get cost_price for margin derivation
    prod_cost_map = dim_prod.set_index('product_id')['cost_price'].to_dict()
    fact_orders['cost_price'] = fact_orders['product_id'].map(prod_cost_map).fillna(0.0)

    # Derivations:
    # 1. net_revenue: only DELIVERED orders count as revenue
    fact_orders['net_revenue'] = np.where(
        fact_orders['order_status'] == 'DELIVERED',
        fact_orders['order_amount'],
        0.0
    ).round(2)

    # 2. gross_margin: net_revenue - (quantity * cost_price) for delivered orders
    fact_orders['gross_margin'] = np.where(
        fact_orders['order_status'] == 'DELIVERED',
        (fact_orders['net_revenue'] - (fact_orders['quantity'] * fact_orders['cost_price'])).round(2),
        0.0
    )

    # 3. within_15_min: delivery_minutes <= 15 for delivered orders
    fact_orders['within_15_min'] = np.where(
        fact_orders['order_status'] == 'DELIVERED',
        fact_orders['delivery_minutes'] <= 15.0,
        False
    )

    now_iso = pendulum.now("Asia/Kolkata").to_iso8601_string()
    fact_orders['etl_loaded_at'] = now_iso
    fact_orders['run_id'] = run_id

    final_fact = fact_orders[[
        'order_id', 'order_ts', 'order_date', 'customer_id', 'store_id', 'product_id',
        'quantity', 'unit_price', 'discount_pct', 'order_amount', 'net_revenue', 'gross_margin',
        'payment_mode', 'order_status', 'channel', 'delivery_minutes', 'within_15_min',
        'etl_loaded_at', 'run_id'
    ]]

    # ── 5. LOAD INTO DUCKDB (IDEMPOTENT) ──────────────────────────
    # Idempotence: We replace or upsert without accumulating duplicate order_ids.
    con.execute("CREATE TABLE IF NOT EXISTS etl_dim_customer AS SELECT * FROM dim_cust WHERE 1=0")
    con.execute("DELETE FROM etl_dim_customer")
    con.register("dim_cust_view", dim_cust)
    con.execute("INSERT INTO etl_dim_customer SELECT * FROM dim_cust_view")

    con.execute("CREATE TABLE IF NOT EXISTS etl_dim_product AS SELECT * FROM dim_prod WHERE 1=0")
    con.execute("DELETE FROM etl_dim_product")
    con.register("dim_prod_view", dim_prod)
    con.execute("INSERT INTO etl_dim_product SELECT * FROM dim_prod_view")

    con.execute("CREATE TABLE IF NOT EXISTS etl_dim_store AS SELECT * FROM dim_store WHERE 1=0")
    con.execute("DELETE FROM etl_dim_store")
    con.register("dim_store_view", dim_store)
    con.execute("INSERT INTO etl_dim_store SELECT * FROM dim_store_view")

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

    # Upsert into etl_fact_orders so running twice never duplicates rows (idempotence)
    con.register("final_fact_view", final_fact)
    con.execute("""
        DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM final_fact_view);
        INSERT INTO etl_fact_orders SELECT * FROM final_fact_view;
    """)

    fact_count = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
    print(f"[Load Complete] etl_fact_orders rows in warehouse: {fact_count} (Idempotent)")

    # ── 6. RECONCILE PAYMENTS ─────────────────────────────────────
    # Compare orders with payments.csv
    # Build etl_payment_reconciliation:
    # 1. DELIVERED orders without SUCCESS payment
    # 2. SUCCESS payment amount != order_amount
    # 3. SUCCESS payment on CANCELLED / RETURNED order
    # 4. Payments referencing non-existent order_id
    con.register("raw_payments_view", clean_pay)
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
            FROM raw_payments_view
        ),
        ord_clean AS (
            SELECT
                order_id,
                order_amount,
                net_revenue,
                order_status
            FROM etl_fact_orders
        )
        -- 1. Delivered orders with no SUCCESS payment
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

        -- 2. SUCCESS payment amount mismatch
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

        -- 3. SUCCESS payment for CANCELLED or RETURNED order
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

        -- 4. Payment exists for orphan / rejected order
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
    print(f"[Reconciliation] Recorded {reconcile_count} reconciliation issues in etl_payment_reconciliation")

    # ── 7. REPORT ─────────────────────────────────────────────────
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

    zone_rev_sample = con.execute("SELECT * FROM etl_daily_zone_revenue LIMIT 5").fetchdf()
    cat_margin = con.execute("SELECT * FROM etl_category_margin").fetchdf()

    print("\n[Report: etl_category_margin]")
    print(cat_margin.to_string(index=False))
    print("\n[Report: etl_daily_zone_revenue sample]")
    print(zone_rev_sample.to_string(index=False))

    from nammamart_observability import record_pipeline_run
    start_time = pendulum.now("Asia/Kolkata").to_iso8601_string()
    end_time = pendulum.now("Asia/Kolkata").to_iso8601_string()
    record_pipeline_run(
        con=con,
        run_id=run_id,
        dag_id=dag_id,
        start_time=start_time,
        end_time=end_time,
        rows_extracted=rows_extracted,
        rows_loaded=len(final_fact),
        rows_quarantined=len(failed_orders),
        status="SUCCESS"
    )

    con.close()
    print("=== [ETL Pipeline Completed Successfully] ===")

    return {
        "run_id": run_id,
        "rows_extracted": rows_extracted,
        "rows_loaded": len(final_fact),
        "rows_quarantined": len(failed_orders),
        "fact_count": fact_count,
        "reconcile_count": reconcile_count,
    }
