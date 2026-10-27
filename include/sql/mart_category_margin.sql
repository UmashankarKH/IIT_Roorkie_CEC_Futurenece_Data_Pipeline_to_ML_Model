-- include/sql/mart_category_margin.sql
-- Answers Business Question 2: Revenue and gross margin by product category
-- margin = revenue - quantity * cost_price (delivered orders only)
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    COALESCE(p.category, 'Uncategorized') AS category,
    COUNT(o.order_id) AS delivered_orders,
    ROUND(SUM(o.order_amount), 2) AS net_revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2) AS total_cogs,
    ROUND(SUM(o.order_amount - (o.quantity * p.cost_price)), 2) AS gross_margin,
    ROUND(SUM(o.order_amount - (o.quantity * p.cost_price)) / NULLIF(SUM(o.order_amount), 0) * 100.0, 2) AS gross_margin_pct
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY COALESCE(p.category, 'Uncategorized')
ORDER BY net_revenue DESC;
