-- Business question 5 (ETL): top 10 customers by spend, with loyalty tier.
-- Personal data is already masked in etl_dim_customer, so this query cannot
-- expose a real email address or phone number.
SELECT f.customer_id,
       c.customer_name_masked,
       c.email_masked,
       c.phone_masked,
       f.loyalty_tier,
       count(*)                   AS delivered_orders,
       round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f
LEFT JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.is_revenue
GROUP BY f.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, f.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
