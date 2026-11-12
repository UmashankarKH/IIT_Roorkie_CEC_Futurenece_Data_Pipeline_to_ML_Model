-- include/sql/mart/mart_category_margin.sql
-- ELT layer 3: MART - answers business question 2
--
-- "Revenue and gross margin by product category
--  (margin = revenue - quantity x cost_price)."
--
-- Reads  : stg_orders (PASS only) joined to stg_products (PASS only)
-- Returns: one row per category with revenue, cost of goods, margin and margin %
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    coalesce(p.category, 'Unknown')                                    AS category,
    count(*)                                                           AS delivered_orders,
    round(sum(o.net_revenue), 2)                                       AS net_revenue,
    round(sum(coalesce(o.quantity, 0) * coalesce(p.cost_price, 0)), 2) AS cost_of_goods,
    round(sum(o.net_revenue)
          - sum(coalesce(o.quantity, 0) * coalesce(p.cost_price, 0)), 2) AS gross_margin,
    round(100.0 * (sum(o.net_revenue)
          - sum(coalesce(o.quantity, 0) * coalesce(p.cost_price, 0)))
          / nullif(sum(o.net_revenue), 0), 2)                          AS margin_pct
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_products WHERE dq_status = 'PASS') p ON p.product_id = o.product_id
WHERE o.dq_status = 'PASS'
  AND o.is_revenue
GROUP BY coalesce(p.category, 'Unknown')
ORDER BY net_revenue DESC;
