-- include/sql/06_staging_payments.sql
-- Staging Payments: Cast types, deduplicate, standardize, flag DQ.

CREATE OR REPLACE TABLE stg_payments AS
WITH typed AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
        CASE
            WHEN LOWER(TRIM(payment_mode)) IN ('upi', 'upi ') THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) IN ('card') THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) IN ('cash on delivery', 'cod', 'cashh') THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        TRIM(gateway_ref) AS gateway_ref,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(payment_id) ORDER BY _loaded_at DESC) AS rnk
    FROM raw_payments
)
SELECT
    p.payment_id,
    p.order_id,
    p.payment_ts,
    p.payment_mode,
    p.amount_paid,
    p.payment_status,
    p.gateway_ref,
    CASE
        WHEN p.amount_paid <= 0 THEN 'FAILED'
        WHEN p.payment_ts > TIMESTAMP '2026-10-02 23:59:59' THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CONCAT_WS('; ',
        CASE WHEN p.amount_paid <= 0 THEN 'Accuracy: non-positive amount_paid' END,
        CASE WHEN p.payment_ts > TIMESTAMP '2026-10-02 23:59:59' THEN 'Timeliness: future payment_ts' END
    ) AS failure_reason,
    p._loaded_at,
    p._source_file,
    p._run_id
FROM typed p
WHERE p.rnk = 1;
