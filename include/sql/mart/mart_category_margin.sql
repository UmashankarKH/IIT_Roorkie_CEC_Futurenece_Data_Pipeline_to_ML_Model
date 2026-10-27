-- Margin by category. Only delivered orders count as revenue.
-- Two different things end up with no category: an order whose product row was
-- rejected in staging (no cost either, so no margin) and an order for a product that
-- exists but has a blank category (cost is fine). orders_missing_cost separates them,
-- otherwise the margin percentage of that bucket looks broken for no visible reason.

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    coalesce(category, '(no category)')                        AS category,
    count(*)                                                   AS delivered_orders,
    count_if(cost_price IS NULL)                               AS orders_missing_cost,
    round(sum(revenue_amount), 2)                              AS net_revenue,
    round(sum(margin_amount), 2)                               AS gross_margin,
    round(100.0 * sum(margin_amount) / nullif(sum(revenue_amount), 0), 1) AS margin_pct
FROM mart_fact_orders
WHERE is_revenue
GROUP BY ALL
ORDER BY net_revenue DESC;
