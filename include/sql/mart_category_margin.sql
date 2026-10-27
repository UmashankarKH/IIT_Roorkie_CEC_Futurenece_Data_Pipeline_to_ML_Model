-- MART: revenue and gross margin by product category.
CREATE OR REPLACE TABLE mart_category_margin AS
WITH f AS (
  SELECT coalesce(p.category, 'Unknown') AS category, o.order_status,
         CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END AS net_revenue,
         CASE WHEN o.order_status = 'DELIVERED' THEN round(o.order_amount - o.quantity * p.cost_price, 2) END AS gross_margin
  FROM stg_orders o
  LEFT JOIN stg_products p ON p.product_id = o.product_id AND p.dim_usable
  WHERE o.dq_status = 'PASS'
)
SELECT category,
       round(sum(net_revenue), 2) AS revenue,
       round(sum(gross_margin), 2) AS gross_margin,
       round(100.0 * sum(gross_margin) / nullif(sum(net_revenue) FILTER (WHERE gross_margin IS NOT NULL), 0), 1) AS margin_pct,
       count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
       count(*) FILTER (WHERE order_status = 'DELIVERED' AND gross_margin IS NULL) AS orders_without_cost
FROM f
GROUP BY category;
