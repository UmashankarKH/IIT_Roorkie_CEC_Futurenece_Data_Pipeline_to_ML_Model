-- Staging for orders, step 1: standardise, type and quality-check this run's raw rows (base file and increment).
-- Placeholders: {run_id} {dag_id} {as_of}
DELETE FROM dq_results WHERE run_id = '{run_id}' AND dag_id = '{dag_id}'
    AND (check_name LIKE 'orders.%' OR check_name LIKE 'orders_increment.%');

CREATE OR REPLACE TEMP TABLE flags AS
WITH src AS (
    SELECT _row_id, _source_file, _run_id,
           CASE WHEN _source_file LIKE '%increment%' THEN 'orders_increment' ELSE 'orders' END AS dataset,
           COALESCE(NULLIF(REGEXP_EXTRACT(_source_file, '(\d{4}-\d{2}-\d{2})', 1), ''), '2026-09-30') AS source_batch,
           NULLIF(TRIM(order_id), '') AS order_id, NULLIF(TRIM(order_ts), '') AS order_ts_raw,
           CAST(TRY_STRPTIME(NULLIF(TRIM(order_ts), ''), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP) AS order_ts,
           NULLIF(TRIM(customer_id), '') AS customer_id, NULLIF(TRIM(store_id), '') AS store_id,
           NULLIF(TRIM(product_id), '') AS product_id,
           TRY_CAST(NULLIF(TRIM(quantity), '') AS DOUBLE) AS quantity,
           TRY_CAST(NULLIF(TRIM(unit_price), '') AS DOUBLE) AS unit_price,
           TRY_CAST(NULLIF(TRIM(discount_pct), '') AS DOUBLE) AS discount_pct,
           TRY_CAST(NULLIF(TRIM(order_amount), '') AS DOUBLE) AS order_amount,
           CASE LOWER(TRIM(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
                WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
                WHEN 'wallet' THEN 'Wallet' ELSE NULLIF(TRIM(payment_mode), '') END AS payment_mode,
           UPPER(NULLIF(TRIM(order_status), '')) AS order_status, NULLIF(TRIM(channel), '') AS channel,
           TRY_CAST(NULLIF(TRIM(delivery_minutes), '') AS DOUBLE) AS delivery_minutes
    FROM raw_orders WHERE _run_id = '{run_id}'
)
SELECT s.*,
    order_id IS NULL AS f_order_id_present,
    customer_id IS NULL AS f_customer_id_present,
    order_ts_raw IS NULL AS f_order_ts_present,
    COALESCE(order_status = 'DELIVERED', FALSE) AND delivery_minutes IS NULL AS f_delivered_has_delivery_minutes,
    order_id IS NOT NULL AND ROW_NUMBER() OVER (PARTITION BY _source_file, order_id ORDER BY _row_id) > 1 AS f_order_id_unique,
    NOT COALESCE(order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED'), FALSE) AS f_order_status_allowed,
    NOT COALESCE(payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), FALSE) AS f_payment_mode_allowed,
    NOT COALESCE(discount_pct BETWEEN 0 AND 100, FALSE) AS f_discount_pct_0_100,
    order_ts_raw IS NOT NULL AND order_ts IS NULL AS f_order_ts_parses,
    NOT COALESCE(channel IN ('App', 'Web'), FALSE) AS f_channel_allowed,
    NOT COALESCE(quantity > 0, FALSE) AS f_quantity_positive,
    NOT COALESCE(unit_price > 0, FALSE) AS f_unit_price_positive,
    NOT COALESCE(ABS(ROUND(quantity * unit_price * (1 - discount_pct / 100.0), 2) - order_amount) <= 0.01, FALSE)
        AS f_order_amount_formula,
    COALESCE(delivery_minutes >= 120, FALSE) AS f_delivery_minutes_below_120,
    COALESCE(delivery_minutes < 120 AND NOT (delivery_minutes BETWEEN 8 AND 35), FALSE) AS f_delivery_minutes_8_to_35,
    customer_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM stg_customers c WHERE c.customer_id = s.customer_id AND c.dq_status <> 'FAILED') AS f_customer_exists,
    NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id = s.product_id AND p.dq_status <> 'FAILED') AS f_product_exists,
    NOT EXISTS (SELECT 1 FROM stg_stores t WHERE t.store_id = s.store_id AND t.dq_status <> 'FAILED') AS f_store_exists,
    COALESCE(CAST(order_ts AS DATE) > LEAST(DATE '{as_of}', CAST(source_batch AS DATE)), FALSE) AS f_order_ts_not_future
FROM src s;

CREATE OR REPLACE TABLE stg_orders_incoming AS
WITH s AS (
    SELECT *, (f_order_id_present OR f_customer_id_present OR f_order_ts_present OR f_delivered_has_delivery_minutes
               OR f_order_id_unique OR f_order_status_allowed OR f_payment_mode_allowed OR f_discount_pct_0_100
               OR f_order_ts_parses OR f_quantity_positive OR f_unit_price_positive OR f_order_amount_formula
               OR f_delivery_minutes_below_120 OR f_customer_exists OR f_product_exists OR f_store_exists
               OR f_order_ts_not_future) AS critical,
              (f_channel_allowed OR f_delivery_minutes_8_to_35) AS warn
    FROM flags
)
SELECT order_id, order_ts, CAST(order_ts AS DATE) AS order_date, customer_id, store_id, product_id,
       CAST(quantity AS BIGINT) AS quantity, unit_price, discount_pct, order_amount, payment_mode, order_status,
       channel, delivery_minutes,
       CASE WHEN critical THEN 'FAILED' WHEN warn THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_order_id_present THEN 'order_id missing' END,
           CASE WHEN f_customer_id_present THEN 'customer_id missing' END,
           CASE WHEN f_order_ts_present THEN 'order_ts missing' END,
           CASE WHEN f_delivered_has_delivery_minutes THEN 'DELIVERED order without delivery_minutes' END,
           CASE WHEN f_order_id_unique THEN 'duplicate order_id' END,
           CASE WHEN f_order_status_allowed THEN 'order_status not in allowed list' END,
           CASE WHEN f_payment_mode_allowed THEN 'payment_mode not in allowed list' END,
           CASE WHEN f_discount_pct_0_100 THEN 'discount_pct outside 0-100' END,
           CASE WHEN f_order_ts_parses THEN 'order_ts not a valid timestamp' END,
           CASE WHEN f_quantity_positive THEN 'quantity not positive' END,
           CASE WHEN f_unit_price_positive THEN 'unit_price not positive' END,
           CASE WHEN f_order_amount_formula THEN 'order_amount does not match quantity x unit_price x (1 - discount)' END,
           CASE WHEN f_delivery_minutes_below_120 THEN 'delivery_minutes 120 or more' END,
           CASE WHEN f_customer_exists THEN 'customer_id not in validated customers' END,
           CASE WHEN f_product_exists THEN 'product_id not in validated products' END,
           CASE WHEN f_store_exists THEN 'store_id not in validated stores' END,
           CASE WHEN f_order_ts_not_future THEN 'order_ts in the future' END), '') AS failure_reason,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_channel_allowed THEN 'channel not App/Web' END,
           CASE WHEN f_delivery_minutes_8_to_35 THEN 'delivery_minutes outside 8-35' END), '') AS warning_reason,
       source_batch, _source_file, _row_id, _run_id
FROM s;

INSERT INTO dq_results
WITH agg AS (
    SELECT dataset, COUNT(*) AS n,
        COUNT(*) FILTER (WHERE f_order_id_present) AS order_id_present,
        COUNT(*) FILTER (WHERE f_customer_id_present) AS customer_id_present,
        COUNT(*) FILTER (WHERE f_order_ts_present) AS order_ts_present,
        COUNT(*) FILTER (WHERE f_delivered_has_delivery_minutes) AS delivered_has_delivery_minutes,
        COUNT(*) FILTER (WHERE f_order_id_unique) AS order_id_unique,
        COUNT(*) FILTER (WHERE f_order_status_allowed) AS order_status_allowed,
        COUNT(*) FILTER (WHERE f_payment_mode_allowed) AS payment_mode_allowed,
        COUNT(*) FILTER (WHERE f_discount_pct_0_100) AS discount_pct_0_100,
        COUNT(*) FILTER (WHERE f_order_ts_parses) AS order_ts_parses,
        COUNT(*) FILTER (WHERE f_channel_allowed) AS channel_allowed,
        COUNT(*) FILTER (WHERE f_quantity_positive) AS quantity_positive,
        COUNT(*) FILTER (WHERE f_unit_price_positive) AS unit_price_positive,
        COUNT(*) FILTER (WHERE f_order_amount_formula) AS order_amount_formula,
        COUNT(*) FILTER (WHERE f_delivery_minutes_below_120) AS delivery_minutes_below_120,
        COUNT(*) FILTER (WHERE f_delivery_minutes_8_to_35) AS delivery_minutes_8_to_35,
        COUNT(*) FILTER (WHERE f_customer_exists) AS customer_exists,
        COUNT(*) FILTER (WHERE f_product_exists) AS product_exists,
        COUNT(*) FILTER (WHERE f_store_exists) AS store_exists,
        COUNT(*) FILTER (WHERE f_order_ts_not_future) AS order_ts_not_future
    FROM flags GROUP BY dataset
), u AS (UNPIVOT agg ON COLUMNS(* EXCLUDE (dataset, n)) INTO NAME check_name VALUE failed),
meta(check_name, dimension, severity) AS (VALUES
    ('order_id_present', 'Completeness', 'Critical'), ('customer_id_present', 'Completeness', 'Critical'),
    ('order_ts_present', 'Completeness', 'Critical'), ('delivered_has_delivery_minutes', 'Completeness', 'Critical'),
    ('order_id_unique', 'Uniqueness', 'Critical'), ('order_status_allowed', 'Validity', 'Critical'),
    ('payment_mode_allowed', 'Validity', 'Critical'), ('discount_pct_0_100', 'Validity', 'Critical'),
    ('order_ts_parses', 'Validity', 'Critical'), ('channel_allowed', 'Validity', 'Warning'),
    ('quantity_positive', 'Accuracy', 'Critical'), ('unit_price_positive', 'Accuracy', 'Critical'),
    ('order_amount_formula', 'Accuracy', 'Critical'), ('delivery_minutes_below_120', 'Accuracy', 'Critical'),
    ('delivery_minutes_8_to_35', 'Accuracy', 'Warning'), ('customer_exists', 'Consistency', 'Critical'),
    ('product_exists', 'Consistency', 'Critical'), ('store_exists', 'Consistency', 'Critical'),
    ('order_ts_not_future', 'Timeliness', 'Critical'))
SELECT '{run_id}', '{dag_id}', u.dataset || '.' || u.check_name, m.dimension, m.severity, u.n, u.failed,
       CASE WHEN u.failed = 0 THEN 'PASS' WHEN m.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       TIMEZONE('Asia/Kolkata', NOW())
FROM u JOIN meta m USING (check_name);

-- Dataset-level timeliness: the newest non-future order must be at most 1 day old at run time.
INSERT INTO dq_results
SELECT '{run_id}', '{dag_id}', dataset || '.latest_order_within_1_day', 'Timeliness', 'Warning', 1,
       CAST(COALESCE(CAST(MAX(order_ts) FILTER (WHERE CAST(order_ts AS DATE) <= LEAST(DATE '{as_of}', CAST(source_batch AS DATE))) AS DATE)
                     < DATE '{as_of}' - 1, TRUE) AS INTEGER),
       CASE WHEN COALESCE(CAST(MAX(order_ts) FILTER (WHERE CAST(order_ts AS DATE) <= LEAST(DATE '{as_of}', CAST(source_batch AS DATE))) AS DATE)
                          < DATE '{as_of}' - 1, TRUE) THEN 'WARN' ELSE 'PASS' END,
       TIMEZONE('Asia/Kolkata', NOW())
FROM flags GROUP BY dataset;

-- Volume: Warning when the base orders file shrank by more than 90% versus the previous run.
INSERT INTO dq_results
WITH now_rows AS (SELECT COUNT(*) AS n FROM flags WHERE dataset = 'orders'),
prev AS (
    SELECT rows_checked FROM dq_results
    WHERE dag_id = '{dag_id}' AND check_name = 'orders.volume_vs_previous_run' AND run_id <> '{run_id}'
    ORDER BY checked_at DESC LIMIT 1
)
SELECT '{run_id}', '{dag_id}', 'orders.volume_vs_previous_run', 'Completeness', 'Warning', n,
       CASE WHEN n < 0.1 * (SELECT rows_checked FROM prev) THEN 1 ELSE 0 END,
       CASE WHEN n < 0.1 * (SELECT rows_checked FROM prev) THEN 'WARN' ELSE 'PASS' END,
       TIMEZONE('Asia/Kolkata', NOW())
FROM now_rows;
