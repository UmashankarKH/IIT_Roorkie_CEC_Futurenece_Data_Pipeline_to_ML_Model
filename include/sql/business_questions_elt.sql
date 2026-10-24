-- Step 11 business questions for the ELT staging and mart layers.
-- The marts are built from SQL-only transformations over raw/staging tables.

-- QUESTION: q1_daily_revenue_by_zone_store
SELECT order_date, zone, store_id, daily_net_revenue, delivered_orders
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_id;

-- QUESTION: q2_revenue_margin_by_category
SELECT category, revenue, gross_margin
FROM mart_category_margin
ORDER BY revenue DESC;

-- QUESTION: q3_delivery_sla
SELECT store_id, average_delivery_minutes, pct_within_15_min, delivered_orders
FROM mart_delivery_sla
ORDER BY store_id;

-- QUESTION: q4_payment_mode_reconciliation
WITH revenue AS (
    SELECT payment_mode, SUM(net_revenue) AS revenue
    FROM stg_orders
    WHERE dq_status = 'PASS'
      AND order_status = 'DELIVERED'
    GROUP BY payment_mode
), mismatches AS (
    SELECT COALESCE(order_payment_mode, payment_payment_mode) AS payment_mode,
           SUM(COALESCE(amount_paid, 0) - COALESCE(order_amount, 0)) AS unreconciled_amount,
           COUNT(*) AS unreconciled_rows
    FROM mart_payment_reconciliation
    GROUP BY COALESCE(order_payment_mode, payment_payment_mode)
)
SELECT COALESCE(revenue.payment_mode, mismatches.payment_mode) AS payment_mode,
       COALESCE(revenue.revenue, 0) AS revenue,
       COALESCE(mismatches.unreconciled_amount, 0) AS unreconciled_amount,
       COALESCE(mismatches.unreconciled_rows, 0) AS unreconciled_rows
FROM revenue
FULL OUTER JOIN mismatches ON revenue.payment_mode = mismatches.payment_mode
ORDER BY payment_mode;

-- QUESTION: q5_top_customers
SELECT customer_id, full_name, loyalty_tier, total_spend
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10;

-- QUESTION: q6_rejected_rows
-- Rows rejected by today's run: the latest scorecard in dq_results, one row
-- per failed rule (a row that breaks two rules is counted under both).
SELECT split_part(check_name, ':', 1) AS file_name,
       regexp_replace(check_name, '^[^:]+:[^:]+:', '') AS failure_reason,
       severity,
       rows_failed AS rejected_rows
FROM dq_results
WHERE run_id = (SELECT run_id FROM dq_results ORDER BY checked_at DESC LIMIT 1)
  AND rows_failed > 0
ORDER BY file_name, rejected_rows DESC;
