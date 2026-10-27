-- MART: revenue by payment mode (DELIVERED orders only).
CREATE OR REPLACE TABLE mart_revenue_by_payment_mode AS
SELECT payment_mode, round(sum(order_amount), 2) AS revenue, count(*) AS delivered_orders
FROM stg_orders
WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
GROUP BY payment_mode;
