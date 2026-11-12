-- Intermediate view: every order that passed the critical checks, joined to its store, product
-- and customer, with the revenue logic defined ONCE. Every mart table reads from here.
-- Same definitions as build_fact() in the ETL:
--   net_revenue  = order_amount for DELIVERED orders (already after discount), else 0
--   gross_margin = net_revenue - quantity x cost_price (NULL when cost_price is unknown)
CREATE OR REPLACE VIEW int_orders_enriched AS
SELECT
    o.order_id, o.order_date, o.order_ts,
    o.customer_id, o.product_id, o.store_id,
    o.quantity, o.unit_price, coalesce(o.discount_pct, 0) AS discount_pct, o.order_amount,
    o.payment_mode, o.order_status, o.channel, o.delivery_minutes,
    s.zone, s.store_name,
    p.category, p.cost_price,
    c.customer_id IS NOT NULL AS customer_matched,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END                AS net_revenue,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.quantity * p.cost_price END            AS cost_amount,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - o.quantity * p.cost_price END AS gross_margin,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes <= 15 END             AS within_15_min,
    o._source_file, o._source_row, o._run_id
FROM stg_orders o
LEFT JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAIL'
LEFT JOIN stg_products  p ON p.product_id  = o.product_id  AND p.dq_status <> 'FAIL'
LEFT JOIN stg_stores    s ON s.store_id    = o.store_id    AND s.dq_status <> 'FAIL'
WHERE o.dq_status <> 'FAIL';

-- Guard rails: fail loudly instead of silently dropping or double-counting revenue
SELECT CASE WHEN (SELECT count(*) FROM int_orders_enriched)
              <> (SELECT count(*) FROM stg_orders WHERE dq_status <> 'FAIL')
            THEN error('int_orders_enriched: the join changed the row count (duplicate dimension keys?)') END AS check_row_count;
SELECT CASE WHEN count(*) > 0
            THEN error('int_orders_enriched: ' || count(*) || ' clean orders have no matching store, product or customer') END
       AS check_dimensions
FROM int_orders_enriched WHERE zone IS NULL OR category IS NULL OR NOT customer_matched;
