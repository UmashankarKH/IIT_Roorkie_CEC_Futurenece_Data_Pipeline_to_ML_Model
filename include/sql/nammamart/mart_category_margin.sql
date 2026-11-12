-- include/sql/nammamart/mart_category_margin.sql
-- BQ2: Revenue and gross margin by product category

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(*)                                                           AS order_count,
    ROUND(SUM(o.quantity), 0)                                          AS total_units_sold,
    ROUND(SUM(o.net_revenue), 2)                                       AS net_revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2)                           AS total_cost,
    ROUND(SUM(o.net_revenue - o.quantity * p.cost_price), 2)          AS gross_margin,
    ROUND(100.0 * SUM(o.net_revenue - o.quantity * p.cost_price)
          / NULLIF(SUM(o.net_revenue), 0), 2)                          AS margin_pct
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status    = 'PASS'
  AND o.order_status = 'DELIVERED'
  AND p.dq_status    = 'PASS'
GROUP BY p.category
ORDER BY net_revenue DESC;
