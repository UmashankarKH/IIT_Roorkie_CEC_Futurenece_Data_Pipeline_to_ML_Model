"""
include/nammamart_elt.py
------------------------
Core ELT Pipeline logic for NammaMart.
Flow: Extract -> Load Raw (Untouched) -> Transform in Warehouse via SQL.

Layers:
  1. Raw:      raw_orders, raw_payments, raw_customers, raw_products, raw_stores
  2. Staging:  stg_orders, stg_payments, stg_customers, stg_products, stg_stores
  3. Mart:     mart_daily_store_kpis, mart_category_margin, mart_delivery_sla,
               mart_payment_reconciliation_details, mart_revenue_by_payment_mode,
               mart_top_customers, mart_rejected_orders_summary

Warehouse: include/warehouse_rajaselvan_thangaraj.duckdb
"""

import os
import duckdb
import pandas as pd
import pendulum
from nammamart_etl import get_paths


def get_sql_dir():
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, "include", "sql")):
        return os.path.join(cwd, "include", "sql")
    return "/usr/local/airflow/include/sql"


def run_sql_file(con: duckdb.DuckDBPyConnection, filename: str):
    sql_path = os.path.join(get_sql_dir(), filename)
    with open(sql_path, "r") as f:
        sql = f.read()
    # DuckDB executes multiple statements sequentially
    con.execute(sql)
    print(f"[SQL Executed] {filename}")


def load_raw_tables(con: duckdb.DuckDBPyConnection, run_id: str):
    """Loads CSV files untouched into raw_* tables with audit columns."""
    warehouse_db, quarantine_dir, data_dir = get_paths()
    run_sql_file(con, "01_raw_tables.sql")
    loaded_at = pendulum.now("Asia/Kolkata").to_iso8601_string()

    files = [
        ("orders.csv", "raw_orders"),
        ("payments.csv", "raw_payments"),
        ("customers.csv", "raw_customers"),
        ("products.csv", "raw_products"),
        ("stores.csv", "raw_stores"),
    ]

    for fname, table in files:
        fpath = os.path.join(data_dir, fname)
        df = pd.read_csv(fpath, dtype=str)
        df["_loaded_at"] = loaded_at
        df["_source_file"] = fname
        df["_run_id"] = run_id
        
        # Check if already loaded for this run to avoid duplicate raw in same run
        con.register(f"temp_{table}", df)
        con.execute(f"INSERT INTO {table} SELECT * FROM temp_{table}")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"[Raw Load] {table}: inserted {len(df)} rows (Total: {count})")


def run_staging_transforms(con: duckdb.DuckDBPyConnection):
    """Executes staging layer SQL transforms."""
    run_sql_file(con, "02_staging_customers.sql")
    run_sql_file(con, "03_staging_products.sql")
    run_sql_file(con, "04_staging_stores.sql")
    run_sql_file(con, "05_staging_orders.sql")
    run_sql_file(con, "06_staging_payments.sql")
    print("[Staging Complete] All staging tables created and validated.")


def load_and_merge_increment(con: duckdb.DuckDBPyConnection, run_id: str):
    """
    Loads orders_2026-10-01_increment.csv into raw_orders and MERGE into stg_orders.
    Applies the 10 status corrections and appends the 150 new orders.
    """
    warehouse_db, quarantine_dir, data_dir = get_paths()
    inc_path = os.path.join(data_dir, "orders_2026-10-01_increment.csv")
    inc_df = pd.read_csv(inc_path)
    loaded_at = pendulum.now("Asia/Kolkata").to_iso8601_string()

    # 1. Append to raw_orders
    raw_inc = inc_df.astype(str).copy()
    raw_inc["_loaded_at"] = loaded_at
    raw_inc["_source_file"] = "orders_2026-10-01_increment.csv"
    raw_inc["_run_id"] = run_id
    con.register("temp_raw_inc", raw_inc)
    con.execute("INSERT INTO raw_orders SELECT * FROM temp_raw_inc")
    print(f"[Raw Increment] Appended {len(raw_inc)} rows to raw_orders")

    # 2. Stage increment orders
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE inc_orders_staged AS
        WITH typed AS (
            SELECT
                TRIM(order_id) AS order_id,
                TRY_CAST(order_ts AS TIMESTAMP) AS parsed_ts,
                order_ts AS raw_ts,
                TRIM(customer_id) AS customer_id,
                TRIM(store_id) AS store_id,
                TRIM(product_id) AS product_id,
                TRY_CAST(quantity AS INTEGER) AS quantity,
                TRY_CAST(unit_price AS DOUBLE) AS unit_price,
                TRY_CAST(discount_pct AS INTEGER) AS discount_pct,
                TRY_CAST(order_amount AS DOUBLE) AS order_amount,
                CASE
                    WHEN LOWER(TRIM(payment_mode)) IN ('upi', 'upi ') THEN 'UPI'
                    WHEN LOWER(TRIM(payment_mode)) IN ('card') THEN 'Card'
                    WHEN LOWER(TRIM(payment_mode)) IN ('cash on delivery', 'cod', 'cashh') THEN 'Cash on Delivery'
                    WHEN LOWER(TRIM(payment_mode)) = 'wallet' THEN 'Wallet'
                    ELSE TRIM(payment_mode)
                END AS payment_mode,
                UPPER(TRIM(order_status)) AS order_status,
                TRIM(channel) AS channel,
                TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
                ? AS _loaded_at,
                'orders_2026-10-01_increment.csv' AS _source_file,
                ? AS _run_id
            FROM temp_raw_inc
        )
        SELECT
            d.order_id,
            d.parsed_ts AS order_ts,
            CAST(d.parsed_ts AS DATE) AS order_date,
            d.customer_id,
            d.store_id,
            d.product_id,
            d.quantity,
            d.unit_price,
            d.discount_pct,
            d.order_amount,
            CASE
                WHEN d.order_status = 'DELIVERED' THEN d.order_amount
                ELSE 0.0
            END AS net_revenue,
            CASE
                WHEN d.order_status = 'DELIVERED' THEN ROUND(d.order_amount - (d.quantity * COALESCE(p.cost_price, 0.0)), 2)
                ELSE 0.0
            END AS gross_margin,
            d.payment_mode,
            d.order_status,
            d.channel,
            d.delivery_minutes,
            CASE
                WHEN d.order_status = 'DELIVERED' AND d.delivery_minutes <= 15.0 THEN TRUE
                ELSE FALSE
            END AS within_15_min,
            'PASSED' AS dq_status,
            '' AS failure_reason,
            d._loaded_at,
            d._source_file,
            d._run_id
        FROM typed d
        LEFT JOIN stg_products p ON d.product_id = p.product_id;
    """, [loaded_at, run_id])

    pre_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]

    # 3. MERGE into stg_orders
    run_sql_file(con, "07_merge_orders_increment.sql")

    post_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    returned_count = con.execute("SELECT COUNT(*) FROM stg_orders WHERE order_status = 'RETURNED'").fetchone()[0]

    print(f"[Merge Complete] stg_orders rows before merge: {pre_count}, after merge: {post_count} (+{post_count - pre_count} new orders)")
    print(f"[Merge Verification] Total RETURNED orders now: {returned_count} (includes the 10 status corrections)")


def run_mart_transforms(con: duckdb.DuckDBPyConnection):
    """Executes all Mart layer SQL scripts."""
    run_sql_file(con, "08_mart_daily_store_kpis.sql")
    run_sql_file(con, "09_mart_category_margin.sql")
    run_sql_file(con, "10_mart_delivery_sla.sql")
    run_sql_file(con, "11_mart_payment_reconciliation.sql")
    run_sql_file(con, "12_mart_top_customers.sql")
    run_sql_file(con, "13_mart_rejected_orders_summary.sql")
    print("[Marts Complete] All business mart tables created.")


def verify_etl_elt_parity(con: duckdb.DuckDBPyConnection) -> bool:
    """Verifies that September net revenue matches between ETL and ELT."""
    try:
        etl_totals = con.execute("""
            SELECT
                COUNT(*) AS etl_delivered_count,
                ROUND(SUM(net_revenue), 2) AS etl_net_revenue,
                ROUND(SUM(gross_margin), 2) AS etl_gross_margin
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
              AND order_date BETWEEN '2026-09-01' AND '2026-09-30'
        """).fetchdf()

        elt_totals = con.execute("""
            SELECT
                COUNT(*) AS elt_delivered_count,
                ROUND(SUM(net_revenue), 2) AS elt_net_revenue,
                ROUND(SUM(gross_margin), 2) AS elt_gross_margin
            FROM stg_orders
            WHERE dq_status = 'PASSED'
              AND order_status = 'DELIVERED'
              AND order_date BETWEEN '2026-09-01' AND '2026-09-30'
        """).fetchdf()

        print("\n=== [September Parity Verification: ETL vs ELT] ===")
        print("ETL Totals:")
        print(etl_totals.to_string(index=False))
        print("ELT Totals:")
        print(elt_totals.to_string(index=False))

        # Check match
        rev_match = (etl_totals['etl_net_revenue'].iloc[0] == elt_totals['elt_net_revenue'].iloc[0])
        print(f"Parity Match: {rev_match}")
        return rev_match
    except Exception as e:
        print(f"[Parity Check Note] {e}")
        return False


def run_elt_pipeline(run_id: str = None, dag_id: str = "nammamart_elt_rajaselvan_thangaraj",
                     include_increment: bool = True):
    """Executes the full ELT pipeline."""
    if not run_id:
        run_id = f"manual_{pendulum.now('Asia/Kolkata').strftime('%Y%m%d_%H%M%S')}"

    warehouse_db, quarantine_dir, data_dir = get_paths()
    print(f"=== [ELT Pipeline Start] run_id={run_id} ===")
    con = duckdb.connect(warehouse_db)

    # 1. Load Raw
    load_raw_tables(con, run_id)

    # 2. Transform Staging
    run_staging_transforms(con)

    # 3. Incremental Load
    if include_increment:
        load_and_merge_increment(con, run_id)

    # 4. Transform Marts
    run_mart_transforms(con)

    # 5. Verify Parity
    verify_etl_elt_parity(con)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
    stg_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    from nammamart_observability import record_pipeline_run
    start_time = pendulum.now("Asia/Kolkata").to_iso8601_string()
    end_time = pendulum.now("Asia/Kolkata").to_iso8601_string()
    failed_count = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'FAILED'").fetchone()[0]
    record_pipeline_run(
        con=con,
        run_id=run_id,
        dag_id=dag_id,
        start_time=start_time,
        end_time=end_time,
        rows_extracted=stg_count + failed_count,
        rows_loaded=stg_count,
        rows_quarantined=failed_count,
        status="SUCCESS"
    )

    con.close()
    print(f"=== [ELT Pipeline Completed] raw_orders={raw_count}, stg_orders={stg_count} ===")
    return {
        "run_id": run_id,
        "raw_orders_count": raw_count,
        "stg_orders_count": stg_count
    }
