-- Merge the latest deduplicated order snapshot into persistent staging.
-- Raw is append-only; stg_orders is the current order state used by marts.
--
-- stg_orders is kept between runs, so the MERGE below is a real upsert:
-- an order_id that already exists is updated in place and a new order_id is
-- inserted. Keys that are no longer present in the current source snapshot
-- (a withdrawn or corrected source row) are removed first so that a row can
-- never linger in the reporting tables.

CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT * FROM stg_orders_snapshot WHERE 1 = 0;

DELETE FROM stg_orders
WHERE NOT EXISTS (
    SELECT 1 FROM stg_orders_snapshot s WHERE s.order_id = stg_orders.order_id
);

MERGE INTO stg_orders AS target
USING stg_orders_snapshot AS source
ON target.order_id = source.order_id
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
    net_revenue = source.net_revenue,
    within_15_min = source.within_15_min,
    dq_status = source.dq_status,
    failure_reason = source.failure_reason,
    _loaded_at = source._loaded_at,
    _source_file = source._source_file,
    _run_id = source._run_id
WHEN NOT MATCHED THEN INSERT BY NAME;
