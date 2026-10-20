-- include/sql/mart/mart_top_customers.sql
-- ELT layer 3: MART - answers business question 5
--
-- "Top 10 customers by spend, with loyalty tier (no raw email or phone shown)."
--
-- Reads  : stg_orders (PASS only) joined to stg_customers (PASS only)
-- Privacy: the staging layer already replaced name, email and phone with masked
--          values, so this table cannot expose personal data even by accident.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    o.customer_id,
    coalesce(c.customer_name_masked, 'UNKNOWN')                        AS customer_name_masked,
    c.email_masked,
    c.phone_masked,
    coalesce(c.loyalty_tier, 'BRONZE')                                 AS loyalty_tier,
    coalesce(c.city, 'Unknown')                                        AS city,
    count(*)                                                           AS delivered_orders,
    round(sum(o.net_revenue), 2)                                       AS total_spend,
    round(avg(o.net_revenue), 2)                                       AS avg_order_value,
    max(o.order_date)                                                  AS last_order_date,
    rank() OVER (ORDER BY sum(o.net_revenue) DESC)                     AS spend_rank
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_customers WHERE dq_status = 'PASS') c ON c.customer_id = o.customer_id
WHERE o.dq_status = 'PASS'
  AND o.is_revenue
GROUP BY o.customer_id, coalesce(c.customer_name_masked, 'UNKNOWN'), c.email_masked,
         c.phone_masked, coalesce(c.loyalty_tier, 'BRONZE'), coalesce(c.city, 'Unknown')
ORDER BY total_spend DESC;
