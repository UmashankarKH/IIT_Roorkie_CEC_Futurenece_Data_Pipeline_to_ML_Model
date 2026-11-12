-- Mart: daily net revenue and order counts by store and zone (business question 1). Delivered orders only, after discount.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date, s.store_id, s.store_name, s.zone,
       COUNT(*) AS total_orders,
       COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
       COUNT(*) FILTER (WHERE o.order_status = 'CANCELLED') AS cancelled_orders,
       COUNT(*) FILTER (WHERE o.order_status = 'RETURNED') AS returned_orders,
       ROUND(COALESCE(SUM(o.order_amount) FILTER (WHERE o.order_status = 'DELIVERED'), 0), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED'
GROUP BY o.order_date, s.store_id, s.store_name, s.zone;

-- Mart: revenue and gross margin by product category (business question 2).
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       ROUND(SUM(o.order_amount), 2) AS revenue,
       ROUND(SUM(ROUND(o.order_amount - o.quantity * p.cost_price, 2)), 2) AS gross_margin,
       ROUND(100 * SUM(ROUND(o.order_amount - o.quantity * p.cost_price, 2)) / NULLIF(SUM(o.order_amount), 0), 2) AS margin_pct
FROM stg_orders o
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED' AND o.order_status = 'DELIVERED'
GROUP BY p.category;

-- Mart: delivery time per store and share of delivered orders within 15 minutes (business question 3).
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT s.store_id, s.store_name, s.zone,
       COUNT(*) AS delivered_orders,
       ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes,
       COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) AS orders_within_15_min,
       ROUND(100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) / COUNT(*), 2) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED' AND o.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name, s.zone;

-- Mart: revenue by payment mode (business question 4, first half).
CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT o.payment_mode, COUNT(*) AS delivered_orders, ROUND(SUM(o.order_amount), 2) AS net_revenue
FROM stg_orders o
WHERE o.dq_status <> 'FAILED' AND o.order_status = 'DELIVERED'
GROUP BY o.payment_mode;

-- Mart: top 10 customers by delivered spend. Only masked contact details are exposed (business question 5).
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT ROW_NUMBER() OVER (ORDER BY SUM(o.order_amount) DESC, c.customer_id) AS spend_rank,
       c.customer_id, c.full_name, c.email_masked, c.phone_masked, c.loyalty_tier,
       COUNT(*) AS delivered_orders, ROUND(SUM(o.order_amount), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED' AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.email_masked, c.phone_masked, c.loyalty_tier
QUALIFY spend_rank <= 10;
