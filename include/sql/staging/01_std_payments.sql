CREATE OR REPLACE TABLE stg_payments_std AS
SELECT
    _row,
    nm_text(payment_id)              AS payment_id,
    nm_text(order_id)                AS order_id,
    nm_ts(payment_ts)                AS payment_ts,
    nm_raw(payment_ts)               AS payment_ts_raw,
    nm_payment_mode(payment_mode)    AS payment_mode,
    nm_raw(payment_mode)             AS payment_mode_raw,
    nm_num(amount_paid)              AS amount_paid,
    nm_payment_status(payment_status) AS payment_status,
    nm_raw(payment_status)           AS payment_status_raw,
    nm_text(gateway_ref)             AS gateway_ref,
    _source_file, _run_id, _loaded_at
FROM raw_payments
WHERE _run_id = '{{ run_id }}';
