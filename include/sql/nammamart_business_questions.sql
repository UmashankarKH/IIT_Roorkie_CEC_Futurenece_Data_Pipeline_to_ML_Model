-- NammaMart business questions for ELT and ETL outputs
-- Use against the DuckDB warehouse created by the Airflow DAGs.

-- 1) Daily net revenue by zone and by store (delivered only, after discount)
SELECT order_date, zone, store_name, SUM(net_revenue) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY order_date, zone, store_name
ORDER BY net_revenue DESC;

-- Equivalent ETL view
SELECT f.order_date,
       s.zone,
       s.store_name,
       SUM(f.calculated_amount) AS net_revenue
FROM etl_fact_orders f
JOIN etl_dim_store s ON s.store_id = f.store_id
WHERE f.order_status = 'DELIVERED'
GROUP BY f.order_date, s.zone, s.store_name
ORDER BY net_revenue DESC;

-- 2) Revenue and gross margin by product category
SELECT category,
       total_revenue AS revenue,
       gross_margin
FROM mart_category_margin
ORDER BY revenue DESC;

-- Equivalent ETL view
SELECT prod.category,
       SUM(f.calculated_amount) AS revenue,
       SUM(f.gross_margin) AS gross_margin
FROM etl_fact_orders f
JOIN etl_dim_product prod ON prod.product_id = f.product_id
WHERE f.order_status = 'DELIVERED'
GROUP BY prod.category
ORDER BY revenue DESC;

-- 3) Average delivery time per store and % of delivered orders within 15 minutes
SELECT store_id,
       store_name,
       avg_delivery_minutes,
       pct_delivered_within_15_min
FROM mart_delivery_sla
ORDER BY avg_delivery_minutes ASC;

-- Equivalent ETL view
SELECT s.store_id, s.store_name,
       AVG(f.delivery_minutes) AS avg_delivery_minutes,
       ROUND(100.0 * SUM(CASE WHEN f.delivery_minutes <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_within_15_min
FROM etl_fact_orders f
JOIN etl_dim_store s ON s.store_id = f.store_id
WHERE f.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name
ORDER BY avg_delivery_minutes ASC;

-- 4) Revenue by payment mode and total amount that does not reconcile with the gateway
SELECT payment_mode,
       SUM(net_revenue) AS revenue_by_mode
FROM stg_orders
WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY revenue_by_mode DESC;

SELECT SUM(unreconciled_amount) AS total_unreconciled_amount
FROM mart_payment_reconciliation
WHERE reconciliation_status IN ('AMOUNT_MISMATCH', 'MISSING_SUCCESSFUL_PAYMENT');

-- Equivalent ETL view
SELECT payment_mode,
       SUM(calculated_amount) AS revenue_by_mode
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY revenue_by_mode DESC;

SELECT SUM(variance) AS total_unreconciled_amount
FROM etl_payment_reconciliation;

-- 5) Top 10 customers by spend, with loyalty tier (masked email/phone not shown)
SELECT customer_id,
       full_name,
       loyalty_tier,
       total_spend
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10;

-- Equivalent ETL view
SELECT c.customer_id,
       c.full_name,
       c.loyalty_tier,
       SUM(f.calculated_amount) AS total_spend
FROM etl_fact_orders f
JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;

-- 6) Number of rows rejected today, by file and reason
SELECT source_file, failure_reason, SUM(rejected_rows) AS rejected_rows
FROM mart_rejected_rows_today
GROUP BY source_file, failure_reason
ORDER BY rejected_rows DESC;

-- Equivalent ETL query over the persisted quarantine table.
SELECT source_file, failure_reason, COUNT(*) AS rejected_rows
FROM etl_quarantine_orders
GROUP BY source_file, failure_reason
ORDER BY rejected_rows DESC;
