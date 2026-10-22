-- Business question 5: top customers by spend, with loyalty tier.
-- Only masked name / email / phone ever appear here.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    dense_rank() OVER (ORDER BY sum(o.net_revenue) DESC) AS spend_rank,
    c.customer_id,
    c.customer_name,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
    round(sum(o.net_revenue), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED'
GROUP BY ALL
HAVING sum(o.net_revenue) > 0
ORDER BY spend_rank, c.customer_id;
