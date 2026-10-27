-- MART: customers ranked by spend. Email and phone are already masked in stg_customers.
CREATE OR REPLACE TABLE mart_top_customers AS
WITH spend AS (
  SELECT customer_id, round(sum(order_amount), 2) AS total_spend, count(*) AS delivered_orders
  FROM stg_orders
  WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
  GROUP BY customer_id
)
SELECT row_number() OVER (ORDER BY sp.total_spend DESC, sp.customer_id) AS spend_rank,
       sp.customer_id, c.full_name, c.email AS masked_email, c.loyalty_tier, sp.total_spend, sp.delivered_orders
FROM spend sp
JOIN stg_customers c ON c.customer_id = sp.customer_id AND c.dim_usable;
