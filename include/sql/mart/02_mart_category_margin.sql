-- Business question 2: revenue and gross margin by product category
-- Margin % only uses orders whose cost is known; orders_without_cost shows how many were left out.
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    category,
    count(*)                                   AS delivered_orders,
    round(sum(net_revenue), 2)                 AS revenue,
    round(sum(cost_amount), 2)                 AS cost,
    round(sum(gross_margin), 2)                AS gross_margin,
    round(100 * sum(gross_margin)
          / nullif(sum(net_revenue) FILTER (WHERE gross_margin IS NOT NULL), 0), 1) AS margin_pct,
    count(*) FILTER (WHERE cost_amount IS NULL) AS orders_without_cost
FROM int_orders_enriched
WHERE order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY revenue DESC;
