-- Q5: customers ranked by spend (delivered net revenue). Only masked PII is exposed.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    rank() OVER (ORDER BY sum(o.net_revenue) DESC)  AS spend_rank,
    c.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
    round(sum(o.net_revenue), 2)                    AS total_spend
FROM v_clean_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status = 'PASSED'
GROUP BY ALL
ORDER BY spend_rank, c.customer_id;
