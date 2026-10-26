CREATE TABLE IF NOT EXISTS mart_daily_store_kpis AS
SELECT
    s.zone,
    s.store_id,
    s.store_name,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS delivered_revenue,
    ROUND(AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes END), 2) AS avg_delivery_minutes,
    SUM(CASE WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15 THEN 1 ELSE 0 END) AS within_15_min_orders,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders
FROM stg_orders o
LEFT JOIN stg_stores s ON o.store_id = s.store_id
GROUP BY s.zone, s.store_id, s.store_name;

CREATE TABLE IF NOT EXISTS mart_category_margin AS
SELECT
    p.category,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.unit_price * o.quantity * (1 - o.discount_pct/100) ELSE 0 END), 2) AS revenue,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN (o.unit_price * o.quantity * (1 - o.discount_pct/100)) - (o.quantity * p.cost_price) ELSE 0 END), 2) AS gross_margin
FROM stg_orders o
LEFT JOIN stg_products p ON o.product_id = p.product_id
GROUP BY p.category;

CREATE TABLE IF NOT EXISTS mart_delivery_sla AS
SELECT
    store_id,
    ROUND(AVG(delivery_minutes), 2) AS avg_delivery_minutes,
    ROUND(100.0 * SUM(CASE WHEN delivery_minutes <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_within_15_min
FROM stg_orders
WHERE order_status = 'DELIVERED'
GROUP BY store_id;

CREATE TABLE IF NOT EXISTS mart_payment_reconciliation AS
SELECT
    o.order_id,
    o.order_amount,
    p.amount_paid,
    CASE WHEN o.order_amount <> p.amount_paid THEN 'amount mismatch' ELSE 'ok' END AS reason
FROM stg_orders o
LEFT JOIN stg_payments p ON o.order_id = p.order_id AND p.payment_status = 'SUCCESS';

CREATE TABLE IF NOT EXISTS mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.loyalty_tier,
    ROUND(SUM(o.order_amount), 2) AS total_spend
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
