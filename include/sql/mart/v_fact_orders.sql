-- The fact projection lives in a view so the full load and the incremental MERGE
-- cannot drift apart. mart_fact_orders.sql materialises it; merge_orders_increment.sql
-- merges the rows belonging to one run out of it.
--
-- Only orders with dq_status = 'PASSED' get in. The LEFT JOIN on the dimensions is
-- deliberate: when a product row was rejected the order still counts as revenue, its
-- margin is simply unknown and the row says so in dq_warnings.

CREATE OR REPLACE VIEW v_fact_orders AS
SELECT
    o.order_id,
    o.order_ts,
    o.order_date,
    o.customer_id,
    o.store_id,
    s.store_name,
    s.zone,
    o.product_id,
    p.category,
    p.sub_category,
    c.loyalty_tier,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.net_revenue,
    p.cost_price,
    round(o.net_revenue - o.quantity * p.cost_price, 2)                    AS gross_margin,
    o.order_status = 'DELIVERED'                                           AS is_revenue,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue ELSE 0.0 END AS revenue_amount,
    CASE WHEN o.order_status = 'DELIVERED'
         THEN round(o.net_revenue - o.quantity * p.cost_price, 2)
         ELSE 0.0 END                                                      AS margin_amount,
    o.payment_mode,
    o.order_status,
    o.channel,
    o.delivery_minutes,
    o.within_15_min,
    CASE WHEN p.product_id IS NULL
         THEN trim(BOTH ' |' FROM concat_ws(' | ', nullif(o.dq_warnings, ''),
                   'Warning: product master row quarantined, margin unknown'))
         ELSE o.dq_warnings END                                            AS dq_warnings,
    o._run_id,
    o._source_file,
    o._loaded_at
FROM stg_orders o
LEFT JOIN mart_dim_product  p ON p.product_id  = o.product_id
LEFT JOIN mart_dim_store    s ON s.store_id    = o.store_id
LEFT JOIN mart_dim_customer c ON c.customer_id = o.customer_id
WHERE o.dq_status = 'PASSED';
