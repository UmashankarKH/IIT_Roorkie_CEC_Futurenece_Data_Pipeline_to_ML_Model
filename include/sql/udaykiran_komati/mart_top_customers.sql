CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    round(sum(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASS' AND c.dq_status = 'PASS'
GROUP BY 1, 2, 3, 4, 5
ORDER BY total_spend DESC
LIMIT 10;
