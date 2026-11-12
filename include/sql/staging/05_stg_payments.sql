CREATE OR REPLACE TABLE stg_payments AS
SELECT
    payment_id, order_id, payment_ts,
    coalesce(payment_mode, 'UNKNOWN') AS payment_mode,                            -- DI-24
    amount_paid, payment_status, gateway_ref,
    (payment_id IS NOT NULL AND NOT "f_payment_id_unique") AS is_first_copy,      -- DI-19 duplicates -> FALSE
    dq_status, failure_reason, warning_reason,
    _row AS _source_row, _source_file, _run_id, _loaded_at
FROM stg_payments_flagged;
