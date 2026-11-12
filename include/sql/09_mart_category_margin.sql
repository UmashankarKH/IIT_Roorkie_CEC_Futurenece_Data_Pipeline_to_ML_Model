-- include/sql/09_mart_category_margin.sql
-- Mart: Net revenue and gross margin by product category.
-- Answers Business Question 2.

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    COALESCE(p.category, 'Unknown') AS category,
    COUNT(o.order_id) AS delivered_orders,
    ROUND(SUM(o.net_revenue), 2) AS total_revenue,
    ROUND(SUM(o.gross_margin), 2) AS gross_margin,
    ROUND(SUM(o.gross_margin) * 100.0 / NULLIF(SUM(o.net_revenue), 0), 2) AS gross_margin_pct
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY COALESCE(p.category, 'Unknown')
ORDER BY total_revenue DESC;
