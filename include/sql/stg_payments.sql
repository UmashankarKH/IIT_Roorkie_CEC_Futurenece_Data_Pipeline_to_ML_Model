-- include/sql/stg_payments.sql
CREATE OR REPLACE TABLE stg_payments AS
WITH latest_raw AS (
    SELECT *
    FROM raw_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _source_file, _row_id ORDER BY _loaded_at DESC) = 1
),
ranked AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
        CASE
            WHEN LOWER(TRIM(payment_mode)) IN ('upi') THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) IN ('card') THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) IN ('cash on delivery', 'cod') THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) IN ('wallet') THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        TRIM(COALESCE(gateway_ref, '')) AS gateway_ref,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(payment_id) ORDER BY _row_id) AS row_num
    FROM latest_raw
),
valid_orders AS (
    SELECT DISTINCT order_id FROM stg_orders
),
validated AS (
    SELECT
        r.*,
        (o.order_id IS NOT NULL) AS order_exists
    FROM ranked r
    LEFT JOIN valid_orders o ON r.order_id = o.order_id
)
SELECT
    payment_id,
    order_id,
    payment_ts,
    payment_mode,
    amount_paid,
    payment_status,
    gateway_ref,
    CASE
        WHEN row_num > 1
          OR amount_paid <= 0
          OR payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
          OR NOT order_exists
          OR payment_ts > TIMESTAMP '2026-10-01 23:59:59'
        THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CASE
        WHEN row_num > 1 THEN 'uniqueness:duplicate_payment_id'
        WHEN amount_paid <= 0 THEN 'accuracy:amount_paid_le_zero'
        WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'validity:invalid_payment_mode'
        WHEN NOT order_exists THEN 'consistency:orphan_order_id'
        WHEN payment_ts > TIMESTAMP '2026-10-01 23:59:59' THEN 'timeliness:future_payment_ts'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _run_id
FROM validated;
