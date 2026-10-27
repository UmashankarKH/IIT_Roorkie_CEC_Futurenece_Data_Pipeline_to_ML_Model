-- include/sql/mart_top_customers.sql
-- Answers Business Question 5: Top 10 customers by spend, with loyalty tier (no raw email or phone shown)
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.masked_email,
    c.masked_phone,
    c.city,
    c.loyalty_tier,
    COUNT(o.order_id) AS total_orders,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0.0 END), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASSED'
GROUP BY c.customer_id, c.full_name, c.masked_email, c.masked_phone, c.city, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
