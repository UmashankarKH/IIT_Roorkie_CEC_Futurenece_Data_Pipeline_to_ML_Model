-- Run against include/warehouse.duckdb after the corresponding DAG has succeeded.

-- 1. Daily net revenue by zone/store (delivered only, after discount)
SELECT order_date, zone, store_id, store_name, SUM(net_revenue) AS net_revenue
FROM etl_fact_orders WHERE order_status='DELIVERED'
GROUP BY ALL ORDER BY order_date, zone, store_id;

-- 2. Revenue and gross margin by category
SELECT category, SUM(net_revenue) AS net_revenue, SUM(gross_margin) AS gross_margin
FROM etl_category_margin GROUP BY category ORDER BY net_revenue DESC;

-- 3. Average delivery time/store and percent delivered within 15 minutes
SELECT store_id, store_name,
       AVG(delivery_minutes) FILTER (WHERE order_status='DELIVERED') AS avg_delivery_minutes,
       100.0 * COUNT(*) FILTER (WHERE order_status='DELIVERED' AND within_15_min)
       / NULLIF(COUNT(*) FILTER (WHERE order_status='DELIVERED' AND delivery_minutes IS NOT NULL),0) AS within_15_min_pct
FROM etl_fact_orders GROUP BY store_id, store_name ORDER BY store_id;

-- 4. Revenue by payment mode and total unreconciled amount
SELECT payment_mode, SUM(net_revenue) AS net_revenue
FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY payment_mode ORDER BY net_revenue DESC;
SELECT SUM(ABS(unreconciled_amount)) AS total_unreconciled_amount
FROM etl_payment_reconciliation WHERE reconciliation_status='UNRECONCILED';

-- 5. Top 10 customers by spend, with masked/no raw PII
SELECT f.customer_id, c.masked_email, c.masked_phone, c.loyalty_tier, SUM(f.net_revenue) AS spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING(customer_id)
WHERE f.order_status='DELIVERED'
GROUP BY ALL ORDER BY spend DESC LIMIT 10;

-- 6. Rejected rows today by file/reason. ETL quarantine files are CSVs under include/quarantine/indhu.
-- Once Part D creates a rejected-row audit table, query that table for a fully queryable result.
