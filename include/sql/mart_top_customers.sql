CREATE OR REPLACE TABLE mart_top_customers AS
SELECT c.customer_id, c.masked_email, c.masked_phone, c.loyalty_tier,
       round(sum(o.quantity*o.unit_price*(1-o.discount_pct/100.0)),2) total_spend
FROM stg_orders o JOIN stg_customers c USING(customer_id)
WHERE o.dq_status='PASS' AND o.order_status='DELIVERED'
GROUP BY c.customer_id,c.masked_email,c.masked_phone,c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
