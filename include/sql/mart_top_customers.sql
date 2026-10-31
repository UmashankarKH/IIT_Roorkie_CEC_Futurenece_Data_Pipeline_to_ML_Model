-- Mart: top 10 customers by spend (delivered orders). Only MASKED contact data is shown.
CREATE OR REPLACE TABLE mart_top_customers AS
WITH spend AS (
    SELECT customer_id,
           count(*)                       AS delivered_orders,
           round(sum(order_amount), 2)    AS total_spend
    FROM stg_orders
    WHERE dq_status <> 'REJECTED' AND order_status = 'DELIVERED'
    GROUP BY customer_id
)
SELECT
    row_number() OVER (ORDER BY sp.total_spend DESC, sp.customer_id) AS spend_rank,
    sp.customer_id,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    sp.delivered_orders,
    sp.total_spend
FROM spend sp
JOIN (SELECT * FROM stg_customers WHERE dq_status <> 'REJECTED') c ON c.customer_id = sp.customer_id
ORDER BY sp.total_spend DESC, sp.customer_id
LIMIT 10;