-- Business question 2 (ETL): revenue and gross margin by product category.
-- margin = revenue - quantity x cost_price, computed row by row during the transform.
SELECT category,
       delivered_orders,
       net_revenue,
       cost_of_goods,
       gross_margin,
       margin_pct
FROM etl_category_margin
ORDER BY net_revenue DESC;
