-- stg_payments: typed, standardised payment-gateway export with DQ flags.
CREATE OR REPLACE TABLE stg_payments AS
WITH src AS (
    SELECT * FROM raw_payments WHERE _run_id = {{run_id}}
),
known_orders AS (
    SELECT DISTINCT trim(order_id) AS order_id FROM raw_orders WHERE _run_id = {{run_id}}
),
typed AS (
    SELECT
        trim(payment_id)                                     AS payment_id,
        trim(order_id)                                       AS order_id,
        try_strptime(trim(payment_ts), '%Y-%m-%d %H:%M:%S')  AS payment_ts,
        std_payment_mode(payment_mode)                       AS payment_mode,
        try_cast(trim(amount_paid) AS DOUBLE)                AS amount_paid,
        upper(trim(payment_status))                          AS payment_status,
        nullif(trim(gateway_ref), '')                        AS gateway_ref,
        trim(payment_mode)                                   AS raw_payment_mode,
        _row_num, _source_file, _run_id
    FROM src
),
flags AS (
    SELECT *,
        row_number() OVER (PARTITION BY payment_id ORDER BY _row_num) > 1                AS f_pay_payment_id_unique,
        order_id IS NULL OR order_id NOT IN (SELECT order_id FROM known_orders)           AS f_pay_order_exists,
        NOT coalesce(payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), false) AS f_pay_mode_valid,
        NOT coalesce(payment_status IN ('SUCCESS', 'FAILED', 'REFUNDED'), false)         AS f_pay_status_valid,
        payment_ts IS NULL                                                                AS f_pay_ts_parseable,
        NOT coalesce(amount_paid > 0, false)                                              AS f_pay_amount_positive,
        coalesce(payment_ts >= CAST({{cutoff}} AS TIMESTAMP), false)                      AS f_pay_ts_not_future,
        gateway_ref IS NULL AND coalesce(payment_mode <> 'Cash on Delivery', true)        AS f_pay_gateway_ref_present,
        coalesce(payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
                 AND raw_payment_mode <> payment_mode, false)                            AS f_pay_value_casing
    FROM typed
)
SELECT
    payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref,
    f_pay_payment_id_unique, f_pay_order_exists, f_pay_mode_valid, f_pay_status_valid,
    f_pay_ts_parseable, f_pay_amount_positive, f_pay_ts_not_future, f_pay_gateway_ref_present,
    f_pay_value_casing,
    nullif(concat_ws('; ',
        CASE WHEN f_pay_payment_id_unique THEN 'duplicate payment_id' END,
        CASE WHEN f_pay_order_exists THEN 'order_id not in orders file' END,
        CASE WHEN f_pay_mode_valid THEN 'payment_mode not in allowed list' END,
        CASE WHEN f_pay_status_valid THEN 'payment_status not in allowed list' END,
        CASE WHEN f_pay_ts_parseable THEN 'payment_ts is not a valid timestamp' END,
        CASE WHEN f_pay_amount_positive THEN 'amount_paid <= 0' END,
        CASE WHEN f_pay_ts_not_future THEN 'payment_ts is in the future' END), '') AS failure_reason,
    nullif(concat_ws('; ',
        CASE WHEN f_pay_gateway_ref_present THEN 'non-COD payment has no gateway_ref' END,
        CASE WHEN f_pay_value_casing THEN 'payment_mode needed standardising' END), '') AS dq_warnings,
    CASE WHEN f_pay_payment_id_unique OR f_pay_order_exists OR f_pay_mode_valid OR f_pay_status_valid
           OR f_pay_ts_parseable OR f_pay_amount_positive OR f_pay_ts_not_future
         THEN 'FAILED' ELSE 'PASSED' END AS dq_status,
    _row_num, _source_file, _run_id
FROM flags;
