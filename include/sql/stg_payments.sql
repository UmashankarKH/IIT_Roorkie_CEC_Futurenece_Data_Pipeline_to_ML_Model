-- stg_payments: cleaned, typed and quality-checked copy of raw_payments for ONE run.
-- Rows that fail a Critical check stay here with dq_status = 'FAIL' (marts ignore them).
CREATE OR REPLACE TABLE stg_payments AS
WITH src AS (
  SELECT trim(payment_id) AS payment_id, trim(order_id) AS order_id, trim(payment_ts) AS payment_ts_raw, trim(payment_mode) AS pm_raw,
    try_cast(trim(amount_paid) AS DOUBLE) AS amount_num, trim(payment_status) AS payment_status, trim(gateway_ref) AS gateway_ref,
    _source_row, _source_file, _run_id
  FROM raw_payments
  WHERE _run_id = '{{run_id}}'
),
exp AS (
  SELECT trim(order_id) AS order_id, arg_min(try_cast(trim(order_amount) AS DOUBLE), _source_row) AS expected
  FROM raw_orders WHERE _run_id = '{{run_id}}' AND _source_file = 'orders.csv'
  GROUP BY trim(order_id)
),
typed AS (
  SELECT src.*,
    try_strptime(payment_ts_raw, '%Y-%m-%d %H:%M:%S') AS payment_ts,
    CASE lower(pm_raw) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cash on delivery' THEN 'Cash on Delivery'
         WHEN 'cod' THEN 'Cash on Delivery' WHEN 'cashh' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' END AS payment_mode,
    row_number() OVER (PARTITION BY src.payment_id ORDER BY src._source_row) AS _rn,
    exp.expected AS expected_amount
  FROM src LEFT JOIN exp ON exp.order_id = src.order_id
),
flagged AS (
  SELECT typed.*,
    coalesce((payment_id = ''), false) AS chk_payment_id_not_null,
    coalesce((order_id = ''), false) AS chk_order_id_not_null,
    coalesce((coalesce(payment_mode, '') <> 'Cash on Delivery' AND gateway_ref = ''), false) AS chk_gateway_ref_present_for_non_cod,
    coalesce((payment_id <> '' AND _rn > 1), false) AS chk_payment_id_unique,
    coalesce((payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED')), false) AS chk_payment_status_valid,
    coalesce((payment_mode IS NULL), false) AS chk_payment_mode_valid,
    coalesce((payment_mode IS NOT NULL AND pm_raw <> payment_mode), false) AS chk_payment_mode_needs_standardising,
    coalesce((payment_ts IS NULL), false) AS chk_payment_ts_valid,
    coalesce((payment_ts > CAST('{{as_of}}' AS TIMESTAMP)), false) AS chk_payment_ts_not_future,
    coalesce((NOT coalesce(amount_num > 0, false)), false) AS chk_amount_paid_positive,
    coalesce((order_id <> '' AND order_id NOT IN (SELECT trim(order_id) FROM raw_orders WHERE _run_id = '{{run_id}}' AND _source_file = 'orders.csv')), false) AS chk_order_id_in_orders,
    coalesce((payment_status = 'SUCCESS' AND expected_amount IS NOT NULL AND NOT coalesce(abs(amount_num - expected_amount) <= 0.01, false)), false) AS chk_success_amount_matches_order
  FROM typed
),
scored AS (
  SELECT flagged.*,
    concat_ws(', ', CASE WHEN chk_payment_id_not_null THEN 'payment_id_not_null' END, CASE WHEN chk_order_id_not_null THEN 'order_id_not_null' END, CASE WHEN chk_payment_id_unique THEN 'payment_id_unique' END, CASE WHEN chk_payment_status_valid THEN 'payment_status_valid' END, CASE WHEN chk_payment_mode_valid THEN 'payment_mode_valid' END, CASE WHEN chk_payment_ts_valid THEN 'payment_ts_valid' END, CASE WHEN chk_payment_ts_not_future THEN 'payment_ts_not_future' END, CASE WHEN chk_amount_paid_positive THEN 'amount_paid_positive' END, CASE WHEN chk_order_id_in_orders THEN 'order_id_in_orders' END) AS failed_checks,
    concat_ws('; ', CASE WHEN chk_payment_id_not_null THEN 'payment_id is missing' END, CASE WHEN chk_order_id_not_null THEN 'order_id is missing' END, CASE WHEN chk_payment_id_unique THEN 'duplicate payment_id (repeat of an earlier row)' END, CASE WHEN chk_payment_status_valid THEN 'payment_status is not SUCCESS/FAILED/REFUNDED' END, CASE WHEN chk_payment_mode_valid THEN 'payment_mode is not UPI/Card/Cash on Delivery/Wallet' END, CASE WHEN chk_payment_ts_valid THEN 'payment_ts is missing or not a valid timestamp' END, CASE WHEN chk_payment_ts_not_future THEN 'payment_ts is in the future' END, CASE WHEN chk_amount_paid_positive THEN 'amount_paid is zero, negative or not a number' END, CASE WHEN chk_order_id_in_orders THEN 'order_id not found in orders (orphan payment)' END) AS failure_reason,
    concat_ws('; ', CASE WHEN chk_gateway_ref_present_for_non_cod THEN 'non-COD payment has no gateway_ref' END, CASE WHEN chk_payment_mode_needs_standardising THEN 'payment_mode spelling/casing will be standardised' END, CASE WHEN chk_success_amount_matches_order THEN 'SUCCESS amount_paid differs from the order''s order_amount (see reconciliation)' END) AS dq_warnings
  FROM flagged
)
SELECT
  payment_id, order_id, payment_ts, payment_mode, amount_num AS amount_paid, payment_status, gateway_ref,
  chk_payment_id_not_null, chk_order_id_not_null, chk_gateway_ref_present_for_non_cod, chk_payment_id_unique, chk_payment_status_valid, chk_payment_mode_valid, chk_payment_mode_needs_standardising, chk_payment_ts_valid, chk_payment_ts_not_future, chk_amount_paid_positive, chk_order_id_in_orders, chk_success_amount_matches_order,
  failed_checks, failure_reason, dq_warnings,
  CASE WHEN failed_checks = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
  _source_row, _source_file, _run_id
FROM scored;
