-- Category revenue and gross margin.
-- Gross margin = delivered revenue - delivered quantity * cost price.

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS delivered_orders,
    SUM(o.quantity) AS units_sold,
    ROUND(SUM(o.order_amount), 2) AS revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2) AS cost_of_goods_sold,
    ROUND(
        SUM(o.order_amount) - SUM(o.quantity * p.cost_price), 2
    ) AS gross_margin,
    ROUND(
        100.0 * (
            SUM(o.order_amount) - SUM(o.quantity * p.cost_price)
        ) / NULLIF(SUM(o.order_amount), 0), 2
    ) AS gross_margin_pct
FROM stg_orders o
JOIN stg_products p USING (product_id)
WHERE o.order_status = 'DELIVERED'
GROUP BY p.category;
