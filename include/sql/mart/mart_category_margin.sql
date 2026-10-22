-- Q2: revenue and gross margin by product category (delivered orders only).
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    category,
    count(*)                                      AS delivered_orders,
    sum(quantity)                                 AS quantity,
    round(sum(net_revenue), 2)                    AS revenue,
    round(sum(quantity * cost_price), 2)          AS cost,
    round(sum(gross_margin), 2)                   AS gross_margin,
    round(100 * sum(gross_margin) / sum(net_revenue), 2) AS margin_pct
FROM v_clean_orders
WHERE order_status = 'DELIVERED'
GROUP BY category
ORDER BY revenue DESC;
