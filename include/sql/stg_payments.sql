CREATE OR REPLACE TABLE stg_payments AS
WITH batch_ranked AS (
    SELECT _source_file, _run_id, MAX(_loaded_at) AS batch_at,
           ROW_NUMBER() OVER (PARTITION BY _source_file ORDER BY MAX(_loaded_at) DESC) AS batch_rank
    FROM raw_payments
    GROUP BY _source_file, _run_id
), latest_batches AS (
    SELECT _source_file, _run_id FROM batch_ranked WHERE batch_rank = 1
), ranked AS (
    SELECT r.*,
           COUNT(*) OVER (PARTITION BY r.payment_id, r._source_file) AS source_key_count,
           ROW_NUMBER() OVER (PARTITION BY r.payment_id ORDER BY r._loaded_at ASC) AS key_rank
    FROM raw_payments r
    JOIN latest_batches b USING (_source_file, _run_id)
), typed AS (
    SELECT
        NULLIF(TRIM(payment_id), '') AS payment_id,
        NULLIF(TRIM(order_id), '') AS order_id,
        TRY_CAST(NULLIF(TRIM(payment_ts), '') AS TIMESTAMP) AS payment_ts,
        CASE LOWER(TRIM(payment_mode))
            WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet' ELSE TRIM(payment_mode)
        END AS payment_mode,
        TRY_CAST(NULLIF(TRIM(amount_paid), '') AS DOUBLE) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        NULLIF(TRIM(gateway_ref), '') AS gateway_ref,
        source_key_count, _source_file, _run_id
    FROM ranked
    WHERE key_rank = 1
), checked AS (
    SELECT p.*,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN payment_id IS NULL THEN 'payment_id is missing' END,
            CASE WHEN order_id IS NULL THEN 'order_id is missing' END,
            CASE WHEN payment_ts IS NULL THEN 'payment_ts is missing or invalid' END,
            CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'invalid payment_mode' END,
            CASE WHEN payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'invalid payment_status' END,
            CASE WHEN amount_paid IS NULL OR amount_paid < 0 THEN 'amount_paid must be non-negative' END,
            CASE WHEN source_key_count > 1 THEN 'duplicate payment_id in source file' END,
            CASE WHEN order_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM stg_orders o WHERE o.order_id = p.order_id AND o.dq_status <> 'FAIL') THEN 'order_id is not present in a usable order record' END,
            CASE WHEN payment_status = 'SUCCESS' AND EXISTS (SELECT 1 FROM stg_orders o WHERE o.order_id = p.order_id AND o.dq_status <> 'FAIL' AND ABS(o.order_amount - p.amount_paid) > 0.005) THEN 'SUCCESS payment amount does not equal order_amount' END,
            CASE WHEN EXISTS (SELECT 1 FROM stg_orders o WHERE o.order_id = p.order_id AND o.dq_status <> 'FAIL' AND o.payment_mode <> p.payment_mode) THEN 'payment_mode does not match order' END
        ), '') AS critical_reason
    FROM typed p
)
SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid,
       payment_status, gateway_ref,
       CASE WHEN critical_reason IS NOT NULL THEN 'FAIL' ELSE 'PASS' END AS dq_status,
       critical_reason AS failure_reason,
       _source_file, _run_id, source_key_count AS source_row_count
FROM checked;
