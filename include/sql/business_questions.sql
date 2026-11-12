-- NammaMart business questions: run the ETL and ELT versions separately.
-- The six questions are paired so results can be compared across architectures.

-- 1A. ETL: daily net revenue by zone and store (delivered orders only)
SELECT order_date, zone, store_id, store_name, SUM(net_revenue) AS net_revenue
FROM etl_fact_orders
WHERE upper(order_status) = 'DELIVERED'
GROUP BY ALL ORDER BY 1,2,3;

-- 1B. ELT
SELECT order_date, zone, store_id, store_name, SUM(net_revenue) AS net_revenue
FROM mart_daily_store_kpis GROUP BY ALL ORDER BY 1,2,3;

-- 2A. ETL: revenue and gross margin by product category
SELECT category, SUM(net_revenue) AS revenue, SUM(gross_margin) AS gross_margin
FROM etl_fact_orders GROUP BY category ORDER BY revenue DESC;

-- 2B. ELT
SELECT category, revenue, gross_margin
FROM mart_category_margin ORDER BY revenue DESC;

-- 3A. ETL: average delivery time and share delivered within 15 minutes
SELECT store_id, store_name,
       AVG(try_cast(delivery_minutes AS DOUBLE)) AS avg_delivery_minutes,
       AVG(CASE WHEN within_15_min THEN 1.0 ELSE 0.0 END)*100 AS pct_within_15_min
FROM etl_fact_orders WHERE upper(order_status)='DELIVERED' GROUP BY ALL;

-- 3B. ELT
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla;

-- 4A. ETL: revenue by payment mode (the order's canonical mode)
SELECT payment_mode, SUM(net_revenue) AS revenue
FROM etl_fact_orders WHERE upper(order_status)='DELIVERED'
GROUP BY payment_mode ORDER BY revenue DESC;
SELECT * FROM etl_payment_reconciliation ORDER BY order_id;

-- 4B. ELT
SELECT payment_mode, SUM(CASE WHEN order_status_std='DELIVERED' THEN order_amount_cast ELSE 0 END) AS revenue
FROM stg_orders WHERE dq_status='PASS' GROUP BY payment_mode ORDER BY revenue DESC;
SELECT * FROM mart_payment_reconciliation ORDER BY order_id;

-- 5A. ETL: top 10 customers; do not select raw contact details
SELECT customer_id, loyalty_tier, SUM(net_revenue) AS spend
FROM etl_fact_orders WHERE upper(order_status)='DELIVERED'
GROUP BY customer_id, loyalty_tier ORDER BY spend DESC LIMIT 10;

-- 5B. ELT
SELECT customer_id, loyalty_tier, spend FROM mart_top_customers ORDER BY spend DESC LIMIT 10;

-- 6A. ETL: rejected rows by file and reason, using this run's issues report.
-- Replace the path with the exact report filename generated for the run.
SELECT file, problem AS reason, COUNT(*) AS rejected_or_flagged_rows
FROM read_csv_auto('include/reports/data_issues_<run_id>.csv', union_by_name=true)
GROUP BY file, problem ORDER BY rejected_or_flagged_rows DESC;

-- 6B. ELT: rejected current staging orders by reason
SELECT 'orders.csv' AS file, failure_reason AS reason, COUNT(*) AS rejected_rows
FROM stg_orders WHERE dq_status='FAIL'
GROUP BY failure_reason ORDER BY rejected_rows DESC;

-- Scorecards and audit evidence (run after each DAG)
SELECT * FROM dq_results ORDER BY checked_at DESC, dag_id, dimension, check_name;
SELECT * FROM pipeline_runs ORDER BY end_time DESC;
