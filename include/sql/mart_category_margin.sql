-- include/sql/mart_category_margin.sql
-- Mart Layer: Revenue and gross margin by product category
-- Answers Business Question 2: Revenue and gross margin by product category

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    COALESCE(p.category, 'Uncategorized') AS category,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.quantity ELSE 0 END) AS units_sold,
    ROUND(SUM(o.net_revenue), 2) AS total_revenue,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.quantity * p.cost_price ELSE 0.0 END), 2) AS total_cost,
    ROUND(SUM(o.gross_margin), 2) AS total_gross_margin,
    ROUND(CASE 
        WHEN SUM(o.net_revenue) > 0 
        THEN (SUM(o.gross_margin) / SUM(o.net_revenue)) * 100.0 
        ELSE 0.0 
    END, 2) AS margin_pct
FROM stg_orders o
INNER JOIN (SELECT DISTINCT product_id, category, cost_price FROM stg_products WHERE dq_status != 'QUARANTINE') p
    ON o.product_id = p.product_id
WHERE o.dq_status = 'VALID'
GROUP BY p.category
ORDER BY total_revenue DESC;
