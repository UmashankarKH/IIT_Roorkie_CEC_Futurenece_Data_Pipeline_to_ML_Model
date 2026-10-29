CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       round(sum(CASE WHEN o.order_status='DELIVERED' THEN o.quantity*o.unit_price*(1-o.discount_pct/100.0) ELSE 0 END),2) AS net_revenue,
       round(sum(CASE WHEN o.order_status='DELIVERED' THEN o.quantity*o.unit_price*(1-o.discount_pct/100.0)-o.quantity*p.cost_price ELSE 0 END),2) AS gross_margin
FROM stg_orders o JOIN stg_products p USING(product_id)
WHERE o.dq_status='PASS'
GROUP BY p.category;
