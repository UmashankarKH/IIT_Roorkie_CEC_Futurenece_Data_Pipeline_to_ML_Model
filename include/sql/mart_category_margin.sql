-- Revenue and gross margin by product category.
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    sum(o.net_revenue) AS revenue,
    sum(o.net_revenue - (o.quantity * p.cost_price)) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id AND p.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY p.category;
