CREATE OR REPLACE TABLE mart_top_customers AS
WITH customer_spend AS (
    SELECT o.customer_id,
           c.loyalty_tier,
           ROUND(SUM(o.order_amount), 2) AS total_spend,
           COUNT(*) AS delivered_orders
    FROM stg_orders o
    JOIN stg_customers c USING (customer_id)
    WHERE o.dq_status <> 'FAIL'
      AND c.dq_status <> 'FAIL'
      AND o.order_status = 'DELIVERED'
    GROUP BY o.customer_id, c.loyalty_tier
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY total_spend DESC, customer_id) AS spend_rank
    FROM customer_spend
)
SELECT spend_rank, customer_id, loyalty_tier, total_spend, delivered_orders
FROM ranked
WHERE spend_rank <= 10
ORDER BY spend_rank;
