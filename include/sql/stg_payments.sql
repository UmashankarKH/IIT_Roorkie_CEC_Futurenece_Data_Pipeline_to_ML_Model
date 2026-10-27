-- Staging: payments received in THIS run.
-- A payment is an orphan only if the order system never sent that order_id at all
-- (in this run's orders.csv or any increment file).

CREATE OR REPLACE TABLE stg_payments AS
WITH src AS (
    SELECT
        *,
        try_strptime(trim(payment_ts), '%Y-%m-%d %H:%M:%S') AS payment_ts_parsed,
        try_cast(trim(amount_paid) AS DOUBLE) AS amount,
        upper(trim(payment_status)) AS status_std,
        CASE lower(trim(payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'wallet' THEN 'Wallet'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'cashh' THEN 'Cash on Delivery'
            ELSE trim(payment_mode)
        END AS mode_std,
        CAST(getvariable('base_business_date') AS DATE) + INTERVAL 1 DAY + INTERVAL 9 HOUR AS as_of,
        row_number() OVER (PARTITION BY payment_id ORDER BY _row_num) AS dup_rank
    FROM raw_payments
    WHERE _run_id = getvariable('run_id')
),

checked AS (
    SELECT
        *,
        list_filter([
            CASE WHEN coalesce(trim(order_id), '') = '' THEN 'order_id_not_null' END,
            CASE WHEN dup_rank > 1 THEN 'payment_id_unique' END,
            CASE WHEN payment_ts_parsed IS NULL THEN 'payment_ts_valid' END,
            CASE WHEN status_std NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'payment_status_valid' END,
            CASE WHEN mode_std NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode_valid' END,
            CASE WHEN amount IS NULL OR amount < 0 OR (status_std = 'SUCCESS' AND amount = 0) THEN 'amount_paid_valid' END,
            CASE WHEN order_id NOT IN (SELECT order_id FROM stg_orders_validated) THEN 'order_id_in_orders' END,
            CASE WHEN payment_ts_parsed > as_of THEN 'payment_ts_not_future' END
        ], x -> x IS NOT NULL) AS dq_failed,
        list_filter([
            CASE WHEN mode_std <> 'Cash on Delivery' AND coalesce(trim(gateway_ref), '') = '' THEN 'gateway_ref_present' END,
            CASE WHEN payment_mode <> mode_std THEN 'values_need_standardising' END
        ], x -> x IS NOT NULL) AS dq_warnings
    FROM src
)

SELECT
    payment_id,
    order_id,
    payment_ts_parsed AS payment_ts,
    mode_std AS payment_mode,
    amount AS amount_paid,
    status_std AS payment_status,
    nullif(trim(gateway_ref), '') AS gateway_ref,
    CASE WHEN len(dq_failed) = 0 THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    array_to_string(dq_failed, '; ') AS failure_reason,
    array_to_string(dq_warnings, '; ') AS warnings,
    dq_failed,
    dq_warnings,
    dup_rank,
    _row_num,
    _source_file,
    _run_id
FROM checked;
