-- include/sql/07_merge_orders_increment.sql
-- Incremental Merge on stg_orders using increment data
-- Updates 10 status changes (DELIVERED -> RETURNED) and inserts 150 new orders.

MERGE INTO stg_orders t
USING inc_orders_staged s
ON t.order_id = s.order_id
WHEN MATCHED THEN
    UPDATE SET
        order_status = s.order_status,
        net_revenue = s.net_revenue,
        gross_margin = s.gross_margin,
        delivery_minutes = s.delivery_minutes,
        within_15_min = s.within_15_min,
        dq_status = s.dq_status,
        failure_reason = s.failure_reason,
        _loaded_at = s._loaded_at,
        _run_id = s._run_id
WHEN NOT MATCHED THEN
    INSERT (
        order_id, order_ts, order_date, customer_id, store_id, product_id,
        quantity, unit_price, discount_pct, order_amount, net_revenue, gross_margin,
        payment_mode, order_status, channel, delivery_minutes, within_15_min,
        dq_status, failure_reason, _loaded_at, _source_file, _run_id
    )
    VALUES (
        s.order_id, s.order_ts, s.order_date, s.customer_id, s.store_id, s.product_id,
        s.quantity, s.unit_price, s.discount_pct, s.order_amount, s.net_revenue, s.gross_margin,
        s.payment_mode, s.order_status, s.channel, s.delivery_minutes, s.within_15_min,
        s.dq_status, s.failure_reason, s._loaded_at, s._source_file, s._run_id
    );
