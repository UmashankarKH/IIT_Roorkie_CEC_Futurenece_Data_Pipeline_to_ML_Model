-- SQL marts built from validated staging data and executed by run_elt.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT CAST(o.order_ts_cast AS DATE) AS order_date, s.zone, s.store_id, s.store_name,
       SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END) AS net_revenue,
       COUNT(*) FILTER (WHERE o.order_status_std='DELIVERED') AS delivered_orders
FROM stg_orders o LEFT JOIN stg_stores s USING (store_id)
WHERE o.dq_status='PASS' GROUP BY ALL;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END) AS revenue,
       SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.order_amount_cast ELSE 0 END)
       - SUM(CASE WHEN o.order_status_std='DELIVERED' THEN o.quantity_cast*p.cost_price ELSE 0 END) AS gross_margin
FROM stg_orders o LEFT JOIN stg_products p USING (product_id)
WHERE o.dq_status='PASS' GROUP BY p.category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT s.store_id, s.store_name, AVG(o.delivery_minutes_cast) AS avg_delivery_minutes,
       AVG(CASE WHEN o.delivery_minutes_cast<=15 THEN 1.0 ELSE 0.0 END)*100 AS pct_within_15_min
FROM stg_orders o LEFT JOIN stg_stores s USING (store_id)
WHERE o.dq_status='PASS' AND o.order_status_std='DELIVERED'
GROUP BY ALL;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT o.order_id, o.order_amount_cast AS order_amount, p.amount_paid,
       CASE WHEN p.order_id IS NULL THEN 'No payment found'
            WHEN abs(o.order_amount_cast-p.amount_paid)>0.02 THEN 'Amount mismatch'
            WHEN p.payment_status!='SUCCESS' THEN 'Payment not successful' END AS failure_reason
FROM stg_orders o LEFT JOIN stg_payments p USING (order_id)
WHERE o.dq_status='PASS'
  AND (p.order_id IS NULL OR abs(o.order_amount_cast-p.amount_paid)>0.02 OR p.payment_status!='SUCCESS');

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT o.customer_id, c.loyalty_tier, SUM(o.order_amount_cast) AS spend
FROM stg_orders o LEFT JOIN stg_customers c USING (customer_id)
WHERE o.dq_status='PASS' AND o.order_status_std='DELIVERED'
GROUP BY ALL ORDER BY spend DESC LIMIT 10;
