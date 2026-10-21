-- Stage all payment rows as SQL, retaining one canonical row per payment_id.
CREATE OR REPLACE TEMP TABLE _payments_ranked AS
WITH latest_copy AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY _source_file, _source_row_number
        ORDER BY _loaded_at DESC, _run_id DESC
    ) AS _copy_rank
    FROM raw_payments
), stable AS (
    SELECT * EXCLUDE (_copy_rank) FROM latest_copy WHERE _copy_rank = 1
)
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY NULLIF(TRIM(payment_id), '') ORDER BY _source_row_number) AS _key_rank,
       COUNT(*) OVER (PARTITION BY NULLIF(TRIM(payment_id), '')) AS _duplicate_count
FROM stable;

CREATE OR REPLACE TABLE stg_payments AS
WITH canonical AS (
    SELECT * FROM _payments_ranked WHERE _key_rank = 1
), latest_orders AS (
    SELECT * EXCLUDE (_order_rank)
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(order_id), '')
            ORDER BY CASE WHEN _source_file LIKE '%increment%' THEN 1 ELSE 0 END DESC,
                     _loaded_at DESC, _source_row_number DESC
        ) AS _order_rank
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY _source_file, _source_row_number
                ORDER BY _loaded_at DESC, _run_id DESC
            ) AS _copy_rank
            FROM raw_orders
        ) copies
        WHERE _copy_rank = 1
    ) ranked
    WHERE _order_rank = 1
), typed AS (
    SELECT
        NULLIF(TRIM(p.payment_id), '') AS payment_id,
        NULLIF(TRIM(p.order_id), '') AS order_id,
        TRY_CAST(NULLIF(TRIM(p.payment_ts), '') AS TIMESTAMP) AS payment_ts,
        CASE LOWER(TRIM(p.payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet'
            ELSE TRIM(p.payment_mode)
        END AS payment_mode,
        TRY_CAST(NULLIF(TRIM(p.amount_paid), '') AS DOUBLE) AS amount_paid,
        UPPER(TRIM(p.payment_status)) AS payment_status,
        NULLIF(TRIM(p.gateway_ref), '') AS gateway_ref,
        p._source_file, p._source_row_number, p._duplicate_count,
        o.order_id AS matched_order_id,
        UPPER(TRIM(o.order_status)) AS matched_order_status,
        TRY_CAST(NULLIF(TRIM(o.order_amount), '') AS DOUBLE) AS expected_amount
    FROM canonical p
    LEFT JOIN latest_orders o ON NULLIF(TRIM(o.order_id), '') = NULLIF(TRIM(p.order_id), '')
), evaluated AS (
    SELECT *,
        CONCAT_WS('; ',
            CASE WHEN payment_id IS NULL THEN 'payment_id is required' END,
            CASE WHEN order_id IS NULL THEN 'order_id is required' END,
            CASE WHEN payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'payment_status is invalid' END,
            CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode is invalid' END,
            CASE WHEN payment_ts IS NULL THEN 'payment_ts is missing or invalid' END,
            CASE WHEN payment_ts > CURRENT_TIMESTAMP THEN 'payment_ts is in the future' END,
            CASE WHEN amount_paid IS NULL OR amount_paid < 0 THEN 'amount_paid must be nonnegative' END,
            CASE WHEN matched_order_id IS NULL THEN 'payment references an unknown order_id' END,
            CASE WHEN matched_order_status = 'CANCELLED' THEN 'payment exists for a cancelled order' END,
            CASE WHEN payment_status = 'SUCCESS' AND matched_order_id IS NOT NULL
                      AND (expected_amount IS NULL OR ABS(amount_paid - expected_amount) > 0.01)
                 THEN 'successful payment amount does not match order_amount' END
        ) AS critical_reason,
        CASE WHEN gateway_ref IS NULL THEN 'gateway_ref is missing' END AS warning_reason
    FROM typed
)
SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status,
       gateway_ref, _source_file, _source_row_number, _duplicate_count,
       CONCAT_WS('; ', NULLIF(critical_reason, ''), NULLIF(warning_reason, '')) AS failure_reason,
       CASE WHEN critical_reason <> '' THEN 'FAIL'
            WHEN warning_reason IS NOT NULL THEN 'WARNING'
            ELSE 'PASS' END AS dq_status
FROM evaluated;
