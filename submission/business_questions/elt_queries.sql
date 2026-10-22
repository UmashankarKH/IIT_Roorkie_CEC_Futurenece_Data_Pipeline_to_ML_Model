-- Run with include/warehouse_sol_elt.duckdb as the active DuckDB database.
-- Query results for the latest successful ELT run are saved in results/elt_q*.csv.

-- QUERY Q1: Daily net revenue by zone and store, delivered orders only.
SELECT order_date, zone, store_id, store_name,
       SUM(delivered_orders) AS delivered_orders,
       ROUND(SUM(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY order_date, zone, store_id, store_name
ORDER BY order_date, zone, store_id;

-- QUERY Q2: Revenue and gross margin by product category.
SELECT category, ROUND(SUM(revenue), 2) AS revenue,
       ROUND(SUM(gross_margin), 2) AS gross_margin
FROM mart_category_margin
GROUP BY category
ORDER BY category;

-- QUERY Q3: Average delivery minutes and within-15-minute share by store.
WITH per_store AS (
    SELECT store_id, COUNT(*) AS delivered_orders,
           AVG(delivery_minutes) AS average_delivery_minutes,
           100.0 * AVG(CASE WHEN delivery_minutes <= 15 THEN 1.0 ELSE 0.0 END)
               AS within_15_min_pct
    FROM stg_orders
    WHERE dq_status <> 'FAIL'
      AND order_status = 'DELIVERED'
      AND delivery_minutes IS NOT NULL
    GROUP BY store_id
)
SELECT s.store_id, s.store_name, s.zone,
       COALESCE(p.delivered_orders, 0) AS delivered_orders,
       ROUND(p.average_delivery_minutes, 2) AS average_delivery_minutes,
       ROUND(p.within_15_min_pct, 2) AS within_15_min_pct
FROM stg_stores s
LEFT JOIN per_store p USING (store_id)
WHERE s.dq_status <> 'FAIL'
ORDER BY s.store_id;

-- QUERY Q4: Delivered revenue by payment mode, plus total unreconciled exposure.
SELECT 'BY_PAYMENT_MODE' AS result_type, payment_mode,
       ROUND(delivered_revenue, 2) AS delivered_revenue,
       CAST(NULL AS DOUBLE) AS unreconciled_amount
FROM mart_payment_mode_revenue
UNION ALL
SELECT 'TOTAL_UNRECONCILED' AS result_type, 'ALL MODES' AS payment_mode,
       CAST(NULL AS DOUBLE) AS delivered_revenue,
       ROUND(SUM(unreconciled_amount), 2) AS unreconciled_amount
FROM mart_payment_mode_revenue
ORDER BY result_type, payment_mode;

-- QUERY Q5: Top 10 customers by delivered spend; identifiers only, no raw PII.
SELECT customer_id, loyalty_tier, total_spend, delivered_orders
FROM mart_top_customers
ORDER BY spend_rank
LIMIT 10;

-- QUERY Q6: Latest-run staging rejections by source file and failure reason.
-- Counts are per check/reason; one source row may fail more than one check.
WITH latest AS (
    SELECT regexp_replace(run_id, '[^A-Za-z0-9_.-]+', '_', 'g') AS safe_run_id
    FROM pipeline_runs
    WHERE dag_id = 'nammamart_elt_sol'
    ORDER BY end_time DESC
    LIMIT 1
)
SELECT r._source_file AS file_name, r.failure_reason,
       SUM(r.rejected_rows) AS rejected_rows
FROM mart_dq_rejections r
JOIN latest l ON r._run_id = l.safe_run_id
GROUP BY r._source_file, r.failure_reason
ORDER BY r._source_file, r.failure_reason;
