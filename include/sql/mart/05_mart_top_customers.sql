-- Business question 5: top 10 customers by spend, with loyalty tier. Only masked contact details.
CREATE OR REPLACE TABLE mart_top_customers AS
WITH spend AS (
    SELECT customer_id,
           count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
           round(sum(net_revenue), 2)                         AS total_spend
    FROM int_orders_enriched
    GROUP BY customer_id
)
SELECT
    row_number() OVER (ORDER BY s.total_spend DESC, s.customer_id) AS spend_rank,
    s.customer_id,
    c.customer_name_masked, c.email_masked, c.phone_masked,
    c.loyalty_tier,
    s.delivered_orders,
    s.total_spend
FROM spend s
JOIN stg_customers c ON c.customer_id = s.customer_id AND c.dq_status <> 'FAIL'
ORDER BY spend_rank
LIMIT 10;
