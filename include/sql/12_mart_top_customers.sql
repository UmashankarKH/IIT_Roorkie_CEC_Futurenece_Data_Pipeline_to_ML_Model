-- include/sql/12_mart_top_customers.sql
-- Mart: Top 10 customers by spend with loyalty tier (PII masked, no raw email or phone).
-- Answers Business Question 5.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.masked_email,
    c.masked_phone,
    c.city,
    c.loyalty_tier,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(o.net_revenue), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.masked_email, c.masked_phone, c.city, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
