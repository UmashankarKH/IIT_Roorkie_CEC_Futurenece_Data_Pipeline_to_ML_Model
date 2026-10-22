MERGE INTO stg_orders AS target
USING stg_orders_source AS source
ON target.order_id IS NOT DISTINCT FROM source.order_id
WHEN MATCHED AND (
    target.order_ts IS DISTINCT FROM source.order_ts
    OR target.customer_id IS DISTINCT FROM source.customer_id
    OR target.store_id IS DISTINCT FROM source.store_id
    OR target.product_id IS DISTINCT FROM source.product_id
    OR target.quantity IS DISTINCT FROM source.quantity
    OR target.unit_price IS DISTINCT FROM source.unit_price
    OR target.discount_pct IS DISTINCT FROM source.discount_pct
    OR target.order_amount IS DISTINCT FROM source.order_amount
    OR target.calculated_amount IS DISTINCT FROM source.calculated_amount
    OR target.payment_mode IS DISTINCT FROM source.payment_mode
    OR target.order_status IS DISTINCT FROM source.order_status
    OR target.channel IS DISTINCT FROM source.channel
    OR target.delivery_minutes IS DISTINCT FROM source.delivery_minutes
    OR target.net_revenue IS DISTINCT FROM source.net_revenue
    OR target.gross_margin IS DISTINCT FROM source.gross_margin
    OR target.dq_status IS DISTINCT FROM source.dq_status
    OR target.failure_reason IS DISTINCT FROM source.failure_reason
    OR target._source_file IS DISTINCT FROM source._source_file
)
THEN UPDATE SET
    order_ts = source.order_ts,
    customer_id = source.customer_id,
    store_id = source.store_id,
    product_id = source.product_id,
    quantity = source.quantity,
    unit_price = source.unit_price,
    discount_pct = source.discount_pct,
    order_amount = source.order_amount,
    calculated_amount = source.calculated_amount,
    payment_mode = source.payment_mode,
    order_status = source.order_status,
    channel = source.channel,
    delivery_minutes = source.delivery_minutes,
    net_revenue = source.net_revenue,
    gross_margin = source.gross_margin,
    dq_status = source.dq_status,
    failure_reason = source.failure_reason,
    _source_file = source._source_file
WHEN NOT MATCHED THEN INSERT (
    order_id,
    order_ts,
    customer_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    order_amount,
    calculated_amount,
    payment_mode,
    order_status,
    channel,
    delivery_minutes,
    net_revenue,
    gross_margin,
    dq_status,
    failure_reason,
    _source_file,
    _run_id
)
VALUES (
    source.order_id,
    source.order_ts,
    source.customer_id,
    source.store_id,
    source.product_id,
    source.quantity,
    source.unit_price,
    source.discount_pct,
    source.order_amount,
    source.calculated_amount,
    source.payment_mode,
    source.order_status,
    source.channel,
    source.delivery_minutes,
    source.net_revenue,
    source.gross_margin,
    source.dq_status,
    source.failure_reason,
    source._source_file,
    source._run_id
)
RETURNING merge_action, order_id;
