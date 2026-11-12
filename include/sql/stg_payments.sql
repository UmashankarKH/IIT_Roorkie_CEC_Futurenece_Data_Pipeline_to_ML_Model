-- Staging for payments: trim, standardise, dedupe, type, apply quality rules. Placeholders: {run_id} {dag_id} {as_of}
DELETE FROM dq_results WHERE run_id = '{run_id}' AND dag_id = '{dag_id}' AND check_name LIKE 'payments.%';

CREATE OR REPLACE TEMP TABLE flags AS
WITH src AS (
    SELECT _row_id, _source_file,
           NULLIF(TRIM(payment_id), '') AS payment_id, NULLIF(TRIM(order_id), '') AS order_id,
           NULLIF(TRIM(payment_ts), '') AS payment_ts_raw,
           CAST(TRY_STRPTIME(NULLIF(TRIM(payment_ts), ''), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP) AS payment_ts,
           CASE LOWER(TRIM(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
                WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
                WHEN 'wallet' THEN 'Wallet' ELSE NULLIF(TRIM(payment_mode), '') END AS payment_mode,
           TRY_CAST(NULLIF(TRIM(amount_paid), '') AS DOUBLE) AS amount_paid,
           UPPER(NULLIF(TRIM(payment_status), '')) AS payment_status,
           NULLIF(TRIM(gateway_ref), '') AS gateway_ref
    FROM raw_payments WHERE _run_id = '{run_id}'
), order_ids AS (
    SELECT DISTINCT TRIM(order_id) AS order_id FROM raw_orders WHERE _run_id = '{run_id}'
)
SELECT s.*,
    EXISTS (SELECT 1 FROM order_ids o WHERE o.order_id = s.order_id) AS order_in_source,
    payment_id IS NULL AS f_payment_id_present,
    order_id IS NULL AS f_order_id_present,
    NOT COALESCE(payment_mode = 'Cash on Delivery', FALSE) AND gateway_ref IS NULL AS f_gateway_ref_for_non_cod,
    payment_id IS NOT NULL AND ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _row_id) > 1 AS f_payment_id_unique,
    NOT COALESCE(payment_status IN ('SUCCESS', 'REFUNDED', 'FAILED'), FALSE) AS f_payment_status_allowed,
    NOT COALESCE(payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), FALSE) AS f_payment_mode_allowed,
    payment_ts IS NULL AS f_payment_ts_parses,
    NOT COALESCE(amount_paid > 0, FALSE) AS f_amount_paid_positive,
    NOT EXISTS (SELECT 1 FROM order_ids o WHERE o.order_id = s.order_id) AS f_order_exists,
    COALESCE(CAST(payment_ts AS DATE) > DATE '{as_of}', FALSE) AS f_payment_ts_not_future
FROM src s;

CREATE OR REPLACE TABLE stg_payments AS
WITH s AS (
    SELECT *, (f_payment_id_present OR f_order_id_present OR f_payment_id_unique OR f_payment_status_allowed
               OR f_payment_mode_allowed OR f_payment_ts_parses OR f_amount_paid_positive
               OR f_payment_ts_not_future) AS critical,
              (f_gateway_ref_for_non_cod OR f_order_exists) AS warn
    FROM flags
)
SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref, order_in_source,
       CASE WHEN critical THEN 'FAILED' WHEN warn THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_payment_id_present THEN 'payment_id missing' END,
           CASE WHEN f_order_id_present THEN 'order_id missing' END,
           CASE WHEN f_payment_id_unique THEN 'duplicate payment_id' END,
           CASE WHEN f_payment_status_allowed THEN 'payment_status not in allowed list' END,
           CASE WHEN f_payment_mode_allowed THEN 'payment_mode not in allowed list' END,
           CASE WHEN f_payment_ts_parses THEN 'payment_ts not a valid timestamp' END,
           CASE WHEN f_amount_paid_positive THEN 'amount_paid not positive' END,
           CASE WHEN f_payment_ts_not_future THEN 'payment_ts in the future' END), '') AS failure_reason,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_gateway_ref_for_non_cod THEN 'gateway_ref missing' END,
           CASE WHEN f_order_exists THEN 'order_id not found in orders' END), '') AS warning_reason,
       _row_id, _source_file
FROM s;

INSERT INTO dq_results
WITH agg AS (
    SELECT 'payments' AS dataset, COUNT(*) AS n,
        COUNT(*) FILTER (WHERE f_payment_id_present) AS payment_id_present,
        COUNT(*) FILTER (WHERE f_order_id_present) AS order_id_present,
        COUNT(*) FILTER (WHERE f_gateway_ref_for_non_cod) AS gateway_ref_for_non_cod,
        COUNT(*) FILTER (WHERE f_payment_id_unique) AS payment_id_unique,
        COUNT(*) FILTER (WHERE f_payment_status_allowed) AS payment_status_allowed,
        COUNT(*) FILTER (WHERE f_payment_mode_allowed) AS payment_mode_allowed,
        COUNT(*) FILTER (WHERE f_payment_ts_parses) AS payment_ts_parses,
        COUNT(*) FILTER (WHERE f_amount_paid_positive) AS amount_paid_positive,
        COUNT(*) FILTER (WHERE f_order_exists) AS order_exists,
        COUNT(*) FILTER (WHERE f_payment_ts_not_future) AS payment_ts_not_future
    FROM flags
), u AS (UNPIVOT agg ON COLUMNS(* EXCLUDE (dataset, n)) INTO NAME check_name VALUE failed),
meta(check_name, dimension, severity) AS (VALUES
    ('payment_id_present', 'Completeness', 'Critical'), ('order_id_present', 'Completeness', 'Critical'),
    ('gateway_ref_for_non_cod', 'Completeness', 'Warning'), ('payment_id_unique', 'Uniqueness', 'Critical'),
    ('payment_status_allowed', 'Validity', 'Critical'), ('payment_mode_allowed', 'Validity', 'Critical'),
    ('payment_ts_parses', 'Validity', 'Critical'), ('amount_paid_positive', 'Accuracy', 'Critical'),
    ('order_exists', 'Consistency', 'Warning'), ('payment_ts_not_future', 'Timeliness', 'Critical'))
SELECT '{run_id}', '{dag_id}', u.dataset || '.' || u.check_name, m.dimension, m.severity, u.n, u.failed,
       CASE WHEN u.failed = 0 THEN 'PASS' WHEN m.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       TIMEZONE('Asia/Kolkata', NOW())
FROM u JOIN meta m USING (check_name);
