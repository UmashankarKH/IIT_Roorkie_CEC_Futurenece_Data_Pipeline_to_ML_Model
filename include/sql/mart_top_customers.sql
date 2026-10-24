-- Top customers by delivered spend with masked contact data only.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    o.customer_id,
    c.full_name,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    sum(o.net_revenue) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id AND c.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2, 3, 4, 5
ORDER BY total_spend DESC
LIMIT 10;
