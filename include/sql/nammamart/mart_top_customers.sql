-- include/sql/nammamart/mart_top_customers.sql
-- BQ5: Top 10 customers by spend, with loyalty tier (no raw PII)

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    o.customer_id,
    c.email,          -- already masked in stg_customers
    c.phone,          -- already masked in stg_customers
    c.loyalty_tier,
    c.city,
    COUNT(*)                           AS order_count,
    ROUND(SUM(o.net_revenue), 2)       AS total_spend
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status    = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.email, c.phone, c.loyalty_tier, c.city
ORDER BY total_spend DESC
LIMIT 10;
