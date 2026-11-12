-- Clean, enriched order view shared by every mart: PASSED orders only, joined to
-- the clean dimensions, with the derived finance measures.
CREATE OR REPLACE VIEW v_clean_orders AS
SELECT
    o.order_id, o.order_ts, o.order_date, o.customer_id, o.store_id, o.product_id,
    o.quantity, o.unit_price, o.discount_pct, o.order_amount, o.payment_mode,
    o.order_status, o.channel, o.delivery_minutes, o._source_file,
    s.store_name, s.zone,
    p.category, p.cost_price,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END                         AS net_revenue,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - o.quantity * p.cost_price ELSE 0 END AS gross_margin,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes <= 15 END                       AS within_15_min
FROM stg_orders o
JOIN stg_stores s   ON s.store_id = o.store_id AND s.dq_status = 'PASSED'
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED';
