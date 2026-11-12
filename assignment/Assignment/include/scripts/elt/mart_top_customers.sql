CREATE OR REPLACE TABLE mart_top_customers AS
SELECT o.customer_id,c.full_name,c.loyalty_tier_clean loyalty_tier,
 SUM(o.order_amount_clean) total_spend,COUNT(*) delivered_orders
FROM stg_orders o JOIN stg_customers c ON o.customer_id=c.customer_id
WHERE o.is_valid AND o.order_status_clean='DELIVERED'
GROUP BY o.customer_id,c.full_name,c.loyalty_tier_clean
ORDER BY total_spend DESC LIMIT 10;
