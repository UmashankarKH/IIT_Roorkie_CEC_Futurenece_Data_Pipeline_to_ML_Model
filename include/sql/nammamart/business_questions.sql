-- Run these queries after both NammaMart DAGs complete successfully.
-- They return the requested answers; result snapshots should be added to the
-- submission after executing the DAGs in Airflow.

-- ETL 1: Daily net revenue by zone and store (delivered orders only).
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone, store_id;

-- ETL 2: Revenue and gross margin by product category.
SELECT category, net_revenue, gross_margin
FROM etl_category_margin
ORDER BY category;

-- ETL 3: Average delivery time and percent within 15 minutes per store.
SELECT store_id, store_name, zone, delivered_orders,
       avg_delivery_minutes, pct_within_15_min
FROM etl_delivery_sla
ORDER BY store_id;

-- ETL 4: Revenue by payment mode plus the total amount requiring reconciliation.
WITH unreconciled AS (
    SELECT COALESCE(ROUND(SUM(reconciliation_amount), 2), 0) AS amount
    FROM etl_payment_reconciliation
)
SELECT r.payment_mode, r.delivered_orders, r.net_revenue,
       u.amount AS total_unreconciled_amount
FROM etl_payment_mode_revenue r
CROSS JOIN unreconciled u
ORDER BY r.payment_mode;

-- ETL 5: Top customers without exposing contact details.
SELECT customer_id, loyalty_tier, delivered_orders, total_spend
FROM etl_top_customers
ORDER BY total_spend DESC, customer_id;

-- ETL 6: Rejected rows by file and reason for the latest ETL run.
WITH latest_run AS (
    SELECT run_id
    FROM pipeline_runs
    WHERE dag_id = 'nammamart_etl_Rajan_Gupta'
    ORDER BY end_time DESC
    LIMIT 1
)
SELECT q.source_file, q.failure_reason, COUNT(*) AS rejected_rows
FROM etl_quarantine q
JOIN latest_run r USING (run_id)
GROUP BY q.source_file, q.failure_reason
ORDER BY q.source_file, rejected_rows DESC, q.failure_reason;

-- ELT 1: Daily net revenue by zone and store (delivered orders only).
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_id;

-- ELT 2: Revenue and gross margin by product category.
SELECT category, net_revenue, gross_margin
FROM mart_category_margin
ORDER BY category;

-- ELT 3: Average delivery time and percent within 15 minutes per store.
SELECT store_id, store_name, zone, delivered_orders,
       avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY store_id;

-- ELT 4: Revenue by payment mode plus the total amount requiring reconciliation.
WITH unreconciled AS (
    SELECT COALESCE(ROUND(SUM(reconciliation_amount), 2), 0) AS amount
    FROM mart_payment_reconciliation
)
SELECT r.payment_mode, r.delivered_orders, r.net_revenue,
       u.amount AS total_unreconciled_amount
FROM mart_payment_mode_revenue r
CROSS JOIN unreconciled u
ORDER BY r.payment_mode;

-- ELT 5: Top customers without exposing contact details.
SELECT customer_id, loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers
ORDER BY total_spend DESC, customer_id;

-- ELT 6: Rejected rows by file and reason for the latest ELT run.
WITH latest_run AS (
    SELECT run_id
    FROM pipeline_runs
    WHERE dag_id = 'nammamart_elt_Rajan_Gupta'
    ORDER BY end_time DESC
    LIMIT 1
)
SELECT q.source_file, q.failure_reason, COUNT(*) AS rejected_rows
FROM elt_quarantine q
JOIN latest_run r USING (run_id)
GROUP BY q.source_file, q.failure_reason
ORDER BY q.source_file, rejected_rows DESC, q.failure_reason;

