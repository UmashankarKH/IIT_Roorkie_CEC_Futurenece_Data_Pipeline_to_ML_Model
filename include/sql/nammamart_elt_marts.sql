-- NammaMart business marts. Only PASS rows from staging can reach these tables.

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date, o.store_id, s.store_name, s.zone,
       COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
       SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS net_revenue,
       AVG(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED') AS avg_delivery_minutes,
       100.0 * AVG(CASE WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15 THEN 1.0 ELSE 0.0 END) AS within_15_pct,
       SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - (o.quantity * p.cost_price) ELSE 0 END) AS gross_margin
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS' AND s.dq_status = 'PASS' AND p.dq_status = 'PASS'
GROUP BY o.order_date, o.store_id, s.store_name, s.zone;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS revenue,
       SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - (o.quantity * p.cost_price) ELSE 0 END) AS gross_margin,
       SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.quantity ELSE 0 END) AS quantity_sold
FROM stg_orders o JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS' AND p.dq_status = 'PASS'
GROUP BY p.category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT o.store_id, s.store_name, s.zone,
       AVG(o.delivery_minutes) AS avg_delivery_minutes,
       COUNT(*) AS delivered_orders,
       100.0 * AVG(CASE WHEN o.delivery_minutes <= 15 THEN 1.0 ELSE 0.0 END) AS within_15_pct
FROM stg_orders o JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS' AND s.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY o.store_id, s.store_name, s.zone;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH successful AS (
    SELECT order_id, SUM(amount_paid) AS amount_paid, STRING_AGG(payment_id, ', ') AS payment_ids
    FROM stg_payments WHERE dq_status = 'PASS' AND payment_status = 'SUCCESS'
    GROUP BY order_id
)
SELECT COALESCE(o.order_id, p.order_id) AS order_id,
       o.order_amount, p.amount_paid,
       COALESCE(p.amount_paid, 0) - COALESCE(o.order_amount, 0) AS difference,
       CASE WHEN o.order_id IS NULL THEN 'missing order'
            WHEN p.order_id IS NULL THEN 'missing successful payment'
            WHEN ABS(p.amount_paid - o.order_amount) > 0.01 THEN 'amount mismatch'
            ELSE 'matched' END AS reconciliation_status,
       p.payment_ids
FROM (SELECT order_id, order_amount FROM stg_orders WHERE dq_status = 'PASS') o
FULL OUTER JOIN successful p USING (order_id)
WHERE o.order_id IS NULL OR p.order_id IS NULL OR ABS(p.amount_paid - o.order_amount) > 0.01;

CREATE OR REPLACE TABLE mart_top_customers AS
WITH spend AS (
    SELECT o.customer_id, SUM(o.order_amount) AS total_spend,
           ROW_NUMBER() OVER (ORDER BY SUM(o.order_amount) DESC) AS spend_rank
    FROM stg_orders o
    WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
    GROUP BY o.customer_id
)
SELECT spend.spend_rank, spend.customer_id, c.full_name, c.email, c.phone,
       c.loyalty_tier, spend.total_spend
FROM spend JOIN stg_customers c USING (customer_id)
WHERE c.dq_status = 'PASS' AND spend.spend_rank <= 10
ORDER BY spend.spend_rank;

-- Bonus: SCD Type 2 customer dimension — keeps loyalty-tier history.
CREATE OR REPLACE TABLE etl_dim_customer_scd2 AS
SELECT c.customer_id, c.full_name, c.email, c.phone, c.city, c.area, c.pincode,
       c.signup_date, c.loyalty_tier, c.is_active,
       TRY_CAST(c._loaded_at AS TIMESTAMP) AS valid_from,
       NULL::TIMESTAMP AS valid_to,
       TRUE AS is_current
FROM stg_customers c
WHERE c.dq_status = 'PASS';

-- Bonus: store-capacity check — delivered orders vs capacity_orders_per_day.
CREATE OR REPLACE TABLE mart_store_capacity_check AS
SELECT o.store_id, s.store_name, s.capacity_orders_per_day,
       COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
       COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') > CAST(s.capacity_orders_per_day AS INTEGER) AS over_capacity
FROM stg_orders o JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS' AND s.dq_status = 'PASS'
GROUP BY o.store_id, s.store_name, s.capacity_orders_per_day
ORDER BY o.store_id;
