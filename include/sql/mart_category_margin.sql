-- Business question 2: revenue and gross margin by product category.
-- gross margin = revenue - quantity x cost_price (delivered orders only).

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
    sum(o.quantity) FILTER (WHERE o.order_status = 'DELIVERED') AS units_sold,
    round(sum(o.net_revenue), 2) AS net_revenue,
    round(sum(o.cost_amount), 2) AS cost_amount,
    round(sum(o.gross_margin), 2) AS gross_margin,
    round(100 * sum(o.gross_margin) / nullif(sum(o.net_revenue), 0), 2) AS margin_pct
FROM stg_orders o
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED'
GROUP BY ALL
ORDER BY net_revenue DESC;
