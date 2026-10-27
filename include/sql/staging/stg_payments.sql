-- Staging payments. Same rules as payment_checks() in include/nammamart/quality.py.
-- A payment whose order_id points nowhere is NOT rejected here: finance still has
-- to look at it, so it is kept and surfaced by mart_payment_reconciliation.

CREATE OR REPLACE TABLE stg_payments AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(payment_id)
                              ORDER BY _loaded_at DESC, rowid DESC) AS dup_rank
    FROM raw_payments
),
typed AS (
    SELECT
        trim(payment_id)                       AS payment_id,
        nullif(trim(order_id), '')             AS order_id,
        try_cast(payment_ts AS TIMESTAMP)      AS payment_ts,
        upper(trim(payment_status))            AS payment_status,
        try_cast(amount_paid AS DOUBLE)        AS amount_paid,
        nullif(trim(gateway_ref), '')          AS gateway_ref,
        trim(payment_mode)                     AS payment_mode_source,
        standard_payment_mode(payment_mode)    AS payment_mode,
        dup_rank, _run_id, _source_file, _loaded_at
    FROM ranked
),
flagged AS (
    SELECT *,
        list_filter([
            CASE WHEN payment_id IS NULL OR payment_id = ''
                 THEN 'Critical: payment_id is missing' END,
            CASE WHEN dup_rank > 1
                 THEN 'Critical: duplicate payment_id, later row kept' END,
            CASE WHEN order_id IS NULL
                 THEN 'Critical: payment has no order_id' END,
            CASE WHEN payment_ts IS NULL
                 THEN 'Critical: payment_ts does not parse' END,
            CASE WHEN amount_paid IS NULL OR amount_paid < 0
                 THEN 'Critical: amount_paid is negative or unreadable' END
        ], lambda x: x IS NOT NULL) AS critical_reasons,
        list_filter([
            CASE WHEN gateway_ref IS NULL
                 THEN 'Warning: no gateway reference recorded' END,
            CASE WHEN payment_mode IS NOT NULL AND payment_mode <> payment_mode_source
                 THEN 'Warning: payment_mode needed standardising' END
        ], lambda x: x IS NOT NULL) AS warning_reasons
    FROM typed
)
SELECT
    payment_id, order_id, payment_ts, payment_mode, payment_mode_source,
    payment_status, amount_paid, gateway_ref,
    CASE WHEN len(critical_reasons) > 0 THEN 'REJECTED' ELSE 'PASSED' END AS dq_status,
    array_to_string(list_concat(critical_reasons, warning_reasons), ' | ') AS failure_reason,
    array_to_string(warning_reasons, ' | ')                               AS dq_warnings,
    _run_id, _source_file, _loaded_at
FROM flagged;
