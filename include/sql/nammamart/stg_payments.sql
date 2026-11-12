-- include/sql/nammamart/stg_payments.sql

CREATE OR REPLACE TABLE stg_payments AS
WITH base AS (
    SELECT
        payment_id,
        order_id,
        CAST(payment_ts AS TIMESTAMP)   AS payment_ts,
        CASE
            WHEN LOWER(TRIM(payment_mode)) = 'upi'              THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) = 'card'             THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) = 'cash on delivery' THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) = 'wallet'           THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END                             AS payment_mode,
        CAST(amount_paid AS DOUBLE)     AS amount_paid,
        UPPER(TRIM(payment_status))     AS payment_status,
        gateway_ref,
        _loaded_at,
        _source_file,
        _run_id
    FROM raw_payments
),
checked AS (
    SELECT *,
        CASE
            WHEN payment_id IS NULL   THEN 'payment_id is null'
            WHEN order_id IS NULL     THEN 'order_id is null'
            WHEN amount_paid IS NULL  THEN 'amount_paid is null'
            WHEN payment_status IS NULL THEN 'payment_status is null'
            ELSE NULL
        END AS failure_reason
    FROM base
)
SELECT *,
    CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status
FROM checked;
