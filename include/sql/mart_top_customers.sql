-- include/sql/mart_top_customers.sql
-- Mart Layer: Top 10 customers by spend with loyalty tier (masked PII only)
-- Answers Business Question 5: Top 10 customers by spend, with loyalty tier

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS total_delivered_orders,
    ROUND(SUM(o.net_revenue), 2) AS total_spend,
    ROUND(AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount END), 2) AS avg_order_value
FROM stg_orders o
INNER JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'VALID' AND c.dq_status != 'QUARANTINE'
GROUP BY c.customer_id, c.full_name, c.email_masked, c.phone_masked, c.loyalty_tier
HAVING total_spend > 0
ORDER BY total_spend DESC
LIMIT 10;
