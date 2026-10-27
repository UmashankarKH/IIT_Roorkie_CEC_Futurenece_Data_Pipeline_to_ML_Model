-- Q1. Daily net revenue by zone and store (DELIVERED orders only, after discount)
SELECT order_date, zone, store_id, store_name, net_revenue, delivered_orders
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_id
LIMIT 20;

-- Q1b. Same, rolled up to zone per day
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY order_date, zone
ORDER BY order_date, zone
LIMIT 12;

-- Q2. Revenue and gross margin by product category
SELECT category, revenue, gross_margin, margin_pct, delivered_orders, orders_without_cost
FROM mart_category_margin
ORDER BY revenue DESC;

-- Q3. Average delivery time per store and % of delivered orders within 15 minutes
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min, delivered_orders
FROM mart_delivery_sla
ORDER BY store_id;

-- Q4a. Revenue by payment mode
SELECT payment_mode, revenue, delivered_orders
FROM mart_revenue_by_payment_mode
ORDER BY revenue DESC;

-- Q4b. Total amount that does not reconcile with the payment gateway
SELECT count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS total_unreconciled_amount
FROM mart_payment_reconciliation;

-- Q4c. ...broken down by reason
SELECT reason, count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM mart_payment_reconciliation
GROUP BY reason
ORDER BY unreconciled_amount DESC;

-- Q5. Top 10 customers by spend, with loyalty tier (no raw email or phone)
SELECT spend_rank, customer_id, full_name, masked_email, loyalty_tier, total_spend
FROM mart_top_customers
WHERE spend_rank <= 10
ORDER BY spend_rank;

-- Q6. Rows rejected in the latest ELT run, by file and by reason
SELECT source_file, failed_checks AS reason, count(*) AS rows_rejected
FROM mart_rejected_rows
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id LIKE 'nammamart_elt%' ORDER BY start_time DESC LIMIT 1)
GROUP BY source_file, failed_checks
ORDER BY source_file, rows_rejected DESC;
