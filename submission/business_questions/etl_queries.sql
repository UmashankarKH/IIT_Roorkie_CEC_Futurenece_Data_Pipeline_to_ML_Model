-- Run with include/warehouse_sol.duckdb as the active DuckDB database.
-- Query results for the latest successful ETL run are saved in results/etl_q*.csv.

-- QUERY Q1: Daily net revenue by zone and store, delivered orders only.
SELECT order_date, zone, store_id, store_name,
       ROUND(SUM(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY order_date, zone, store_id, store_name
ORDER BY order_date, zone, store_id;

-- QUERY Q2: Revenue and gross margin by product category.
SELECT category, ROUND(SUM(revenue), 2) AS revenue,
       ROUND(SUM(gross_margin), 2) AS gross_margin
FROM etl_category_margin
GROUP BY category
ORDER BY category;

-- QUERY Q3: Average delivery minutes and within-15-minute share by store.
SELECT s.store_id, s.store_name, s.zone,
       COUNT(f.order_id) AS delivered_orders,
       ROUND(AVG(f.delivery_minutes), 2) AS average_delivery_minutes,
       ROUND(100.0 * AVG(CASE WHEN f.within_15_min THEN 1.0 ELSE 0.0 END), 2)
           AS within_15_min_pct
FROM etl_dim_store s
LEFT JOIN etl_fact_orders f
  ON f.store_id = s.store_id AND f.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY s.store_id;

-- QUERY Q4: Delivered revenue by payment mode, plus total unreconciled exposure.
WITH by_mode AS (
    SELECT payment_mode, ROUND(SUM(net_revenue), 2) AS delivered_revenue
    FROM etl_fact_orders
    WHERE order_status = 'DELIVERED'
    GROUP BY payment_mode
), mismatch_total AS (
    SELECT ROUND(SUM(
        CASE
            WHEN reason LIKE 'no payment row%' THEN COALESCE(order_amount, 0)
            WHEN reason LIKE '%no validated clean order%' THEN
                 COALESCE(TRY_CAST(amount_paid AS DOUBLE), 0)
            WHEN reason LIKE '%payment status is %' THEN COALESCE(order_amount, 0)
            WHEN order_amount IS NULL THEN COALESCE(TRY_CAST(amount_paid AS DOUBLE), 0)
            ELSE ABS(COALESCE(order_amount, 0) - COALESCE(TRY_CAST(amount_paid AS DOUBLE), 0))
        END
    ), 2) AS unreconciled_amount
    FROM etl_payment_reconciliation
)
SELECT 'BY_PAYMENT_MODE' AS result_type, payment_mode,
       delivered_revenue, CAST(NULL AS DOUBLE) AS unreconciled_amount
FROM by_mode
UNION ALL
SELECT 'TOTAL_UNRECONCILED' AS result_type, 'ALL MODES' AS payment_mode,
       CAST(NULL AS DOUBLE) AS delivered_revenue, unreconciled_amount
FROM mismatch_total
ORDER BY result_type, payment_mode;

-- QUERY Q5: Top 10 customers by delivered spend; identifiers only, no raw PII.
SELECT c.customer_id, c.loyalty_tier,
       ROUND(SUM(f.net_revenue), 2) AS total_spend,
       COUNT(*) AS delivered_orders
FROM etl_fact_orders f
JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.loyalty_tier
ORDER BY total_spend DESC, c.customer_id
LIMIT 10;

-- QUERY Q6: Latest-run quarantined rows by source file and recorded reason.
-- Reads only failure_reason and filename; it does not select quarantined PII fields.
WITH latest AS (
    SELECT regexp_replace(run_id, '[^A-Za-z0-9_.-]+', '_', 'g') AS safe_run_id
    FROM pipeline_runs
    WHERE dag_id = 'nammamart_etl_sol'
    ORDER BY end_time DESC
    LIMIT 1
), quarantine AS (
    SELECT filename,
           regexp_extract(filename, '([^/]+)_failed_', 1) AS file_name,
           replace(regexp_extract(filename, '_failed_(.*)', 1), '.csv', '') AS safe_run_id,
           failure_reason
    FROM read_csv_auto(
        'include/quarantine/sol/*_failed_*.csv',
        filename = true,
        union_by_name = true,
        all_varchar = true
    )
)
SELECT q.file_name, q.failure_reason, COUNT(*) AS rejected_rows
FROM quarantine q
JOIN latest l USING (safe_run_id)
GROUP BY q.file_name, q.failure_reason
ORDER BY q.file_name, q.failure_reason;
