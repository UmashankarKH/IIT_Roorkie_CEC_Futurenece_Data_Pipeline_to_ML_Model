-- Mart: revenue and gross margin by category (margin = revenue - quantity * cost_price).
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    coalesce(p.category, 'UNKNOWN')                        AS category,
    count(*)                                               AS delivered_orders,
    round(sum(o.order_amount), 2)                          AS net_revenue,
    round(coalesce(sum(o.order_amount - o.quantity * p.cost_price), 0), 2) AS gross_margin,
    CASE WHEN p.category IS NULL THEN NULL
         ELSE round(100.0 * sum(o.order_amount - o.quantity * p.cost_price) / sum(o.order_amount), 1)
    END                                                    AS margin_pct
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_products WHERE dq_status <> 'REJECTED') p ON p.product_id = o.product_id
WHERE o.dq_status <> 'REJECTED'
  AND o.order_status = 'DELIVERED'
GROUP BY p.category;