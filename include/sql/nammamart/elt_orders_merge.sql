-- Upsert by order_id: increment rows replace existing snapshots without doubling facts.
CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT * FROM _orders_source WHERE FALSE;

MERGE INTO stg_orders AS target
USING _orders_source AS source
ON COALESCE(target.order_id, '') = COALESCE(source.order_id, '')
WHEN MATCHED THEN UPDATE SET
    order_ts = source.order_ts,
    order_date = source.order_date,
    customer_id = source.customer_id,
    store_id = source.store_id,
    product_id = source.product_id,
    quantity = source.quantity,
    unit_price = source.unit_price,
    discount_pct = source.discount_pct,
    order_amount = source.order_amount,
    payment_mode = source.payment_mode,
    order_status = source.order_status,
    channel = source.channel,
    delivery_minutes = source.delivery_minutes,
    category = source.category,
    cost_price = source.cost_price,
    store_name = source.store_name,
    zone = source.zone,
    net_revenue = source.net_revenue,
    gross_margin = source.gross_margin,
    within_15_min = source.within_15_min,
    _source_file = source._source_file,
    _source_row_number = source._source_row_number,
    failure_reason = source.failure_reason,
    dq_status = source.dq_status
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount, payment_mode, order_status,
    channel, delivery_minutes, category, cost_price, store_name, zone,
    net_revenue, gross_margin, within_15_min, _source_file, _source_row_number,
    failure_reason, dq_status
) VALUES (
    source.order_id, source.order_ts, source.order_date, source.customer_id,
    source.store_id, source.product_id, source.quantity, source.unit_price,
    source.discount_pct, source.order_amount, source.payment_mode, source.order_status,
    source.channel, source.delivery_minutes, source.category, source.cost_price,
    source.store_name, source.zone, source.net_revenue, source.gross_margin,
    source.within_15_min, source._source_file, source._source_row_number,
    source.failure_reason, source.dq_status
);
