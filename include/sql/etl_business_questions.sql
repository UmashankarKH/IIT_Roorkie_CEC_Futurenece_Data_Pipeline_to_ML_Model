-- Q1. Daily net revenue by zone and store (DELIVERED orders only, after discount)
SELECT order_date, zone, store_id, store_name, net_revenue, delivered_orders
FROM etl_daily_zone_revenue
ORDER BY order_date, zone, store_id
LIMIT 20;

-- Q1b. Same, rolled up to zone per day
SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY order_date, zone
ORDER BY order_date, zone
LIMIT 12;

-- Q2. Revenue and gross margin by product category
SELECT category, revenue, gross_margin, margin_pct, delivered_orders, orders_without_cost
FROM etl_category_margin
ORDER BY revenue DESC;

-- Q3. Average delivery time per store and % of delivered orders within 15 minutes
SELECT store_id, store_name,
       round(avg(delivery_minutes), 1) AS avg_delivery_minutes,
       round(100.0 * sum(CASE WHEN within_15_min THEN 1 ELSE 0 END) / count(*), 1) AS pct_within_15_min,
       count(*) AS delivered_orders
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY store_id, store_name
ORDER BY store_id;

-- Q4a. Revenue by payment mode
SELECT payment_mode, round(sum(net_revenue), 2) AS revenue, count(*) AS delivered_orders
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY revenue DESC;

-- Q4b. Total amount that does not reconcile with the payment gateway
SELECT count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS total_unreconciled_amount
FROM etl_payment_reconciliation;

-- Q4c. ...broken down by reason
SELECT reason, count(*) AS mismatches, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM etl_payment_reconciliation
GROUP BY reason
ORDER BY unreconciled_amount DESC;

-- Q5. Top 10 customers by spend, with loyalty tier (email and phone are masked in the dimension)
SELECT f.customer_id, c.full_name, c.email AS masked_email, c.loyalty_tier,
       round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f
JOIN etl_dim_customer c ON c.customer_id = f.customer_id
GROUP BY f.customer_id, c.full_name, c.email, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;

-- Q6. Rows rejected in the latest run, by file and by reason
SELECT source_file, failed_checks AS reason, count(*) AS rows_rejected
FROM etl_quarantine_rows
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_chandini' ORDER BY start_time DESC LIMIT 1)
GROUP BY source_file, failed_checks
ORDER BY source_file, rows_rejected DESC;
