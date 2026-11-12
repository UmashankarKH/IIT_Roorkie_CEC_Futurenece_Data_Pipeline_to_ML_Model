-- Staging: payments. A payment must point at an order that exists in orders.csv (or the increment file).
CREATE OR REPLACE TABLE stg_payments AS
WITH known_orders AS (
    SELECT DISTINCT trim(order_id) AS order_id FROM v_latest_raw_orders
), std AS (
    SELECT _row_num, _source_file, _run_id,
           trim(payment_id) AS payment_id, trim(order_id) AS order_id,
           try_strptime(trim(payment_ts), '%Y-%m-%d %H:%M:%S') AS payment_ts,
           CASE lower(trim(payment_mode))
                WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
                WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
                ELSE trim(payment_mode) END AS payment_mode,
           TRY_CAST(trim(amount_paid) AS DOUBLE) AS amount_paid, trim(payment_status) AS payment_status,
           trim(gateway_ref) AS gateway_ref
    FROM v_latest_raw_payments
), flags AS (
    SELECT *,
        coalesce(payment_id, '') = ''                                        AS chk_payments_payment_id_not_null,
        coalesce(order_id, '') = ''                                          AS chk_payments_order_id_not_null,
        coalesce(gateway_ref, '') = '' AND payment_mode <> 'Cash on Delivery' AS chk_payments_gateway_ref_present,
        row_number() OVER (PARTITION BY payment_id ORDER BY _row_num) > 1    AS chk_payments_payment_id_unique,
        payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')    AS chk_payments_payment_mode_valid,
        coalesce(payment_status, '') NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') AS chk_payments_status_valid,
        payment_ts IS NULL                                                   AS chk_payments_payment_ts_parses,
        NOT coalesce(amount_paid > 0, false)                                 AS chk_payments_amount_positive,
        order_id NOT IN (SELECT order_id FROM known_orders)                  AS chk_payments_order_exists,
        coalesce(payment_ts > TIMESTAMP '{{as_of}}', false)                  AS chk_payments_payment_ts_not_future
    FROM std
)
SELECT *,
    concat_ws('; ',
        CASE WHEN chk_payments_payment_id_not_null THEN 'payments_payment_id_not_null' END,
        CASE WHEN chk_payments_order_id_not_null THEN 'payments_order_id_not_null' END,
        CASE WHEN chk_payments_payment_id_unique THEN 'payments_payment_id_unique' END,
        CASE WHEN chk_payments_payment_mode_valid THEN 'payments_payment_mode_valid' END,
        CASE WHEN chk_payments_status_valid THEN 'payments_status_valid' END,
        CASE WHEN chk_payments_payment_ts_parses THEN 'payments_payment_ts_parses' END,
        CASE WHEN chk_payments_amount_positive THEN 'payments_amount_positive' END,
        CASE WHEN chk_payments_order_exists THEN 'payments_order_exists' END,
        CASE WHEN chk_payments_payment_ts_not_future THEN 'payments_payment_ts_not_future' END) AS failure_reason,
    CASE WHEN chk_payments_gateway_ref_present THEN 'payments_gateway_ref_present' ELSE '' END AS warning_reason,
    CASE WHEN chk_payments_payment_id_not_null OR chk_payments_order_id_not_null OR chk_payments_payment_id_unique
              OR chk_payments_payment_mode_valid OR chk_payments_status_valid OR chk_payments_payment_ts_parses
              OR chk_payments_amount_positive OR chk_payments_order_exists OR chk_payments_payment_ts_not_future
         THEN 'FAIL'
         WHEN chk_payments_gateway_ref_present THEN 'WARN' ELSE 'PASS' END AS dq_status
FROM flags;
