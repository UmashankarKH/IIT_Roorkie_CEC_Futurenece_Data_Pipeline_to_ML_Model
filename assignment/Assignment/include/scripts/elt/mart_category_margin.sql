
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    SUM(
        o.quantity_clean * o.unit_price_clean
        * (1 - o.discount_pct_clean / 100.0)
    ) AS revenue,
    SUM(
        o.quantity_clean * p.cost_price_clean
    ) AS total_cost,
    SUM(
        o.quantity_clean * o.unit_price_clean
        * (1 - o.discount_pct_clean / 100.0)
    ) - SUM(
        o.quantity_clean * p.cost_price_clean
    ) AS gross_margin
FROM stg_orders o
JOIN stg_products p
    ON o.product_id = p.product_id
WHERE o.is_valid
  AND o.order_status_clean = 'DELIVERED'
  AND p.is_valid
GROUP BY p.category;
