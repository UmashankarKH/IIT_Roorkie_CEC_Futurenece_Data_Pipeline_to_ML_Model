CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR, order_ts TIMESTAMP, order_date DATE,
    customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR,
    delivery_minutes DOUBLE, dq_status VARCHAR, failure_reason VARCHAR,
    _source_file VARCHAR, _run_id VARCHAR, source_row_count BIGINT
);

MERGE INTO stg_orders AS target
USING (
WITH batch_ranked AS (
    SELECT _source_file, _run_id, MAX(_loaded_at) AS batch_at,
           ROW_NUMBER() OVER (PARTITION BY _source_file ORDER BY MAX(_loaded_at) DESC) AS batch_rank
    FROM raw_orders
    GROUP BY _source_file, _run_id
), latest_batches AS (
    SELECT _source_file, _run_id FROM batch_ranked WHERE batch_rank = 1
), source_rows AS (
    SELECT r.*,
           COUNT(*) OVER (PARTITION BY r.order_id, r._source_file) AS source_key_count,
           ROW_NUMBER() OVER (
               PARTITION BY r.order_id
               ORDER BY CASE WHEN r._source_file = 'orders_2026-10-01_increment.csv' THEN 1 ELSE 0 END DESC,
                        r._loaded_at DESC, r._run_id DESC
           ) AS key_rank
    FROM raw_orders r
    JOIN latest_batches b USING (_source_file, _run_id)
), chosen AS (
    SELECT * FROM source_rows WHERE key_rank = 1
), typed AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS order_id,
        TRY_CAST(NULLIF(TRIM(order_ts), '') AS TIMESTAMP) AS order_ts,
        CAST(TRY_CAST(NULLIF(TRIM(order_ts), '') AS TIMESTAMP) AS DATE) AS order_date,
        NULLIF(TRIM(customer_id), '') AS customer_id,
        NULLIF(TRIM(store_id), '') AS store_id,
        NULLIF(TRIM(product_id), '') AS product_id,
        TRY_CAST(NULLIF(TRIM(quantity), '') AS INTEGER) AS quantity,
        TRY_CAST(NULLIF(TRIM(unit_price), '') AS DOUBLE) AS unit_price,
        TRY_CAST(NULLIF(TRIM(discount_pct), '') AS DOUBLE) AS discount_pct,
        TRY_CAST(NULLIF(TRIM(order_amount), '') AS DOUBLE) AS order_amount,
        CASE LOWER(TRIM(payment_mode))
            WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet' ELSE TRIM(payment_mode)
        END AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status,
        UPPER(TRIM(channel)) AS channel,
        TRY_CAST(NULLIF(TRIM(delivery_minutes), '') AS DOUBLE) AS delivery_minutes,
        source_key_count, _source_file, _run_id
    FROM chosen
), checked AS (
    SELECT t.*,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN order_id IS NULL THEN 'order_id is missing' END,
            CASE WHEN order_ts IS NULL THEN 'order_ts is missing or invalid' END,
            CASE WHEN order_ts IS NOT NULL AND CAST(order_ts AS DATE) > CAST(TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP) AS DATE) THEN 'order_ts is in the future' END,
            CASE WHEN customer_id IS NULL THEN 'customer_id is missing' END,
            CASE WHEN store_id IS NULL THEN 'store_id is missing' END,
            CASE WHEN product_id IS NULL THEN 'product_id is missing' END,
            CASE WHEN quantity IS NULL OR quantity <= 0 THEN 'quantity must be positive' END,
            CASE WHEN unit_price IS NULL OR unit_price < 0 THEN 'unit_price is invalid' END,
            CASE WHEN discount_pct IS NULL OR discount_pct < 0 OR discount_pct > 100 THEN 'discount_pct must be from 0 to 100' END,
            CASE WHEN order_amount IS NULL THEN 'order_amount is missing or invalid' END,
            CASE WHEN order_amount IS NOT NULL AND quantity IS NOT NULL AND unit_price IS NOT NULL AND discount_pct IS NOT NULL
                      AND ROUND(order_amount, 2) <> ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)
                 THEN 'order_amount does not match the discounted quantity x unit_price formula' END,
            CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'invalid payment_mode' END,
            CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'invalid order_status' END,
            CASE WHEN LOWER(COALESCE(order_id, '')) LIKE '%test%' OR LOWER(COALESCE(channel, '')) LIKE '%test%' THEN 'test order/channel' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'DELIVERED order requires delivery_minutes' END,
            CASE WHEN delivery_minutes < 0 OR delivery_minutes > 120 THEN 'delivery_minutes is outside plausible 0..120 range' END,
            CASE WHEN source_key_count > 1 THEN 'duplicate order_id in selected source file' END,
            CASE WHEN customer_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id = t.customer_id AND c.dq_status <> 'FAIL') THEN 'customer_id is not present in a usable customer dimension' END,
            CASE WHEN product_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id = t.product_id AND p.dq_status <> 'FAIL') THEN 'product_id is not present in a usable product dimension' END,
            CASE WHEN store_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id = t.store_id AND s.dq_status <> 'FAIL') THEN 'store_id is not present in a usable store dimension' END
        ), '') AS critical_reason,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN delivery_minutes IS NOT NULL AND delivery_minutes >= 0 AND delivery_minutes <= 120 AND (delivery_minutes < 8 OR delivery_minutes > 35) THEN 'delivery_minutes is outside the normal 8..35 minute range' END
        ), '') AS warning_reason
    FROM typed t
)
SELECT order_id, order_ts, order_date, customer_id, store_id, product_id,
       quantity, unit_price, discount_pct, order_amount, payment_mode,
       order_status, channel, delivery_minutes,
       CASE WHEN critical_reason IS NOT NULL THEN 'FAIL'
            WHEN warning_reason IS NOT NULL THEN 'WARN' ELSE 'PASS' END AS dq_status,
       NULLIF(CONCAT_WS('; ', critical_reason, warning_reason), '') AS failure_reason,
       _source_file, _run_id, source_key_count AS source_row_count
FROM checked
) AS source
ON target.order_id IS NOT DISTINCT FROM source.order_id
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
    dq_status = source.dq_status,
    failure_reason = source.failure_reason,
    _source_file = source._source_file,
    _run_id = source._run_id,
    source_row_count = source.source_row_count
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount, payment_mode,
    order_status, channel, delivery_minutes, dq_status, failure_reason,
    _source_file, _run_id, source_row_count
) VALUES (
    source.order_id, source.order_ts, source.order_date, source.customer_id,
    source.store_id, source.product_id, source.quantity, source.unit_price,
    source.discount_pct, source.order_amount, source.payment_mode,
    source.order_status, source.channel, source.delivery_minutes,
    source.dq_status, source.failure_reason, source._source_file,
    source._run_id, source.source_row_count
);
