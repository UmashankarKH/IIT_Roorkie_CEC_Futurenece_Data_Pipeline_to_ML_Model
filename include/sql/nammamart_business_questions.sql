-- Step 9: six business questions for both final warehouse designs.
-- Run the ETL statements against warehouse_rudin.duckdb and the ELT
-- statements against warehouse_rudin_elt.duckdb.

-- 1. Daily net revenue by zone and store.
-- ETL
SELECT order_date, zone, store_id, SUM(net_revenue) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY order_date, zone, store_id
ORDER BY order_date, zone, store_id;
-- ELT
SELECT order_date, zone, store_id, net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_id;

-- 2. Revenue and gross margin by category.
-- ETL
SELECT category, revenue, gross_margin
FROM etl_category_margin
ORDER BY category;
-- ELT
SELECT category, revenue, gross_margin
FROM mart_category_margin
ORDER BY category;

-- 3. Average delivery time and 15-minute SLA percentage by store.
-- ETL
SELECT store_id, AVG(delivery_minutes) AS avg_delivery_minutes,
       100.0 * AVG(CASE WHEN within_15_min THEN 1.0 ELSE 0.0 END) AS within_15_pct
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY store_id
ORDER BY store_id;
-- ELT
SELECT store_id, avg_delivery_minutes, within_15_pct
FROM mart_delivery_sla
ORDER BY store_id;

-- 4. Revenue by payment mode and unreconciled amounts.
-- ETL
SELECT UPPER(payment_mode) AS payment_mode, SUM(net_revenue) AS revenue
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY UPPER(payment_mode)
ORDER BY payment_mode;
SELECT COUNT(*) AS unreconciled_orders,
       SUM(ABS(difference)) AS unreconciled_amount
FROM etl_payment_reconciliation;
-- ELT
SELECT payment_mode, SUM(net_revenue) AS revenue
FROM stg_orders
WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY payment_mode;
SELECT COUNT(*) AS unreconciled_orders,
       SUM(ABS(difference)) AS unreconciled_amount
FROM mart_payment_reconciliation;

-- 5. Top 10 customers by delivered spend and loyalty tier.
-- ETL
SELECT customer_id, loyalty_tier, total_spend
FROM (
    SELECT f.customer_id, c.loyalty_tier, SUM(f.net_revenue) AS total_spend,
           ROW_NUMBER() OVER (ORDER BY SUM(f.net_revenue) DESC) AS spend_rank
    FROM etl_fact_orders f
    JOIN etl_dim_customer c USING (customer_id)
    WHERE f.order_status = 'DELIVERED'
    GROUP BY f.customer_id, c.loyalty_tier
) ranked
WHERE spend_rank <= 10
ORDER BY spend_rank;
-- ELT
SELECT spend_rank, customer_id, loyalty_tier, total_spend
FROM mart_top_customers
ORDER BY spend_rank;

-- 6. Rejected rows by source file and reason.
-- ETL: quarantine CSVs are the rejection store; filename identifies the file.
SELECT filename, failure_reason, COUNT(*) AS rejected_rows
FROM read_csv_auto('include/quarantine/rudin/*_failed_*.csv',
                   union_by_name = true, filename = true)
GROUP BY filename, failure_reason
ORDER BY filename, failure_reason;
-- ELT: staging retains the raw source-file metadata and SQL rejection reason.
SELECT _source_file, failure_reason, COUNT(*) AS rejected_rows
FROM (
    SELECT _source_file, failure_reason FROM stg_orders WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, failure_reason FROM stg_payments WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, failure_reason FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, failure_reason FROM stg_products WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, failure_reason FROM stg_stores WHERE dq_status = 'FAIL'
) rejected
GROUP BY _source_file, failure_reason
ORDER BY _source_file, failure_reason;
