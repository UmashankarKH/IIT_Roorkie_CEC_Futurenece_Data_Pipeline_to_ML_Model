-- Top 10 customers by delivered spend.
-- Do not include email or phone in this mart.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.loyalty_tier,
    COUNT(*) AS delivered_orders,
    ROUND(SUM(o.order_amount), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c USING (customer_id)
WHERE o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.loyalty_tier
ORDER BY total_spend DESC, c.customer_id
LIMIT 10;
