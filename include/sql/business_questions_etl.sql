-- Step 11 business questions for the ETL star schema.
-- The notebook executes each section independently and stores each result.

-- QUESTION: q1_daily_revenue_by_zone_store
SELECT order_date, zone, store_id,
       SUM(net_revenue) AS daily_net_revenue,
       COUNT(*) AS delivered_orders
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY order_date, zone, store_id
ORDER BY order_date, zone, store_id;

-- QUESTION: q2_revenue_margin_by_category
SELECT category,
       SUM(net_revenue) AS revenue,
       SUM(gross_margin) AS gross_margin
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY category
ORDER BY revenue DESC;

-- QUESTION: q3_delivery_sla
SELECT store_id,
       AVG(delivery_minutes) AS average_delivery_minutes,
       100.0 * AVG(CASE WHEN delivery_minutes <= 15 THEN 1 ELSE 0 END) AS pct_within_15_min,
       COUNT(*) AS delivered_orders
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY store_id
ORDER BY store_id;

-- QUESTION: q4_payment_mode_reconciliation
WITH revenue AS (
    SELECT payment_mode, SUM(net_revenue) AS revenue
    FROM etl_fact_orders
    WHERE order_status = 'DELIVERED'
    GROUP BY payment_mode
), mismatches AS (
    SELECT COALESCE(order_payment_mode, payment_payment_mode) AS payment_mode,
           SUM(COALESCE(amount_paid, 0) - COALESCE(order_amount, 0)) AS unreconciled_amount,
           COUNT(*) AS unreconciled_rows
    FROM etl_payment_reconciliation
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
SELECT f.customer_id, c.full_name, c.loyalty_tier,
       SUM(f.net_revenue) AS total_spend
FROM etl_fact_orders f
JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.full_name, c.loyalty_tier
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
