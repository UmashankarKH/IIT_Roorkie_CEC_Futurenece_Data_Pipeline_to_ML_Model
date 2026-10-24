-- include/sql/stg_payments.sql
-- Staging layer for payments: deduplication, standardisation, DQ validation

CREATE OR REPLACE TABLE stg_payments AS
WITH raw_typed AS (
    SELECT
        payment_id,
        order_id,
        payment_ts AS payment_ts_raw,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts_parsed,
        CASE 
            WHEN UPPER(TRIM(payment_mode)) IN ('UPI', 'UPI ') THEN 'UPI'
            WHEN UPPER(TRIM(payment_mode)) IN ('CARD', 'CREDIT CARD', 'DEBIT CARD') THEN 'Card'
            WHEN UPPER(TRIM(payment_mode)) IN ('CASH ON DELIVERY', 'COD') THEN 'Cash on Delivery'
            WHEN UPPER(TRIM(payment_mode)) = 'WALLET' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS clean_payment_mode,
        TRY_CAST(amount_paid AS DECIMAL(10, 2)) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        gateway_ref,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_payments
),
evaluated AS (
    SELECT
        payment_id,
        order_id,
        payment_ts_parsed AS payment_ts,
        clean_payment_mode AS payment_mode,
        amount_paid,
        payment_status,
        gateway_ref,
        _loaded_at,
        _source_file,
        _run_id,
        rn,
        CASE
            WHEN rn > 1 THEN 'DUPLICATE_PAYMENT_ID'
            WHEN order_id NOT IN (SELECT order_id FROM raw_orders) THEN 'UNKNOWN_ORDER_ID'
            WHEN clean_payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'INVALID_PAYMENT_MODE'
            ELSE NULL
        END AS critical_failure,
        CASE
            WHEN gateway_ref IS NULL AND payment_status = 'SUCCESS' THEN 'MISSING_GATEWAY_REF'
            ELSE NULL
        END AS warning_reason
    FROM raw_typed
)
SELECT
    payment_id,
    order_id,
    payment_ts,
    payment_mode,
    amount_paid,
    payment_status,
    gateway_ref,
    _loaded_at,
    _source_file,
    _run_id,
    CASE 
        WHEN critical_failure IS NOT NULL THEN 'QUARANTINE'
        WHEN warning_reason IS NOT NULL THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    COALESCE(critical_failure, warning_reason) AS failure_reason
FROM evaluated;
