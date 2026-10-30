CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    ROUND(SUM(o.order_amount), 2) AS revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2) AS cost_of_goods,
    ROUND(SUM(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin
FROM stg_orders o
JOIN stg_products p USING (product_id)
WHERE o.dq_status <> 'FAIL'
  AND p.dq_status <> 'FAIL'
  AND o.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY p.category;
