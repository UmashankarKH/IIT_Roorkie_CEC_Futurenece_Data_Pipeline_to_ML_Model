-- Business question 2 (ELT): revenue and gross margin by product category.
SELECT category,
       delivered_orders,
       net_revenue,
       cost_of_goods,
       gross_margin,
       margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC;
