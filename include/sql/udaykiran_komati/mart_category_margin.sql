CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    round(sum(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS revenue,
    round(sum(
        CASE WHEN o.order_status = 'DELIVERED'
            THEN o.order_amount - (o.quantity * p.cost_price)
            ELSE 0
        END
    ), 2) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS' AND p.dq_status = 'PASS'
GROUP BY 1
ORDER BY revenue DESC;
