-- Staging: payments. One row per payment_id. Amount mismatches are warnings (handled in reconciliation).
CREATE OR REPLACE TABLE stg_payments AS
WITH base AS (
    SELECT
        trim(payment_id)                        AS payment_id,
        trim(order_id)                          AS order_id,
        TRY_CAST(trim(payment_ts) AS TIMESTAMP) AS payment_ts,
        trim(payment_mode)                      AS payment_mode_raw,
        CASE lower(trim(payment_mode))
            WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet'
        END                                     AS payment_mode,
        TRY_CAST(trim(amount_paid) AS DOUBLE)   AS amount_paid,
        upper(trim(payment_status))             AS payment_status,
        trim(gateway_ref)                       AS gateway_ref,
        _source_file, _run_id, _loaded_at,
        row_number() OVER (PARTITION BY trim(payment_id) ORDER BY rowid) AS rn
    FROM raw_payments
    WHERE _run_id = '{{run_id}}'
),
deduped AS (
    SELECT * FROM base WHERE rn = 1 OR payment_id = ''   -- later copies stay in raw only
),
with_order AS (
    SELECT d.*, o.order_amount AS order_amount
    FROM deduped d
    LEFT JOIN (SELECT order_id, order_amount FROM stg_orders WHERE dq_status <> 'REJECTED') o
           ON o.order_id = d.order_id
),
checked AS (
    SELECT *,
        concat_ws(';',
            CASE WHEN payment_id = '' THEN 'payment_id_not_null' END,
            CASE WHEN payment_mode IS NULL THEN 'payment_mode_allowed' END,
            CASE WHEN payment_status NOT IN ('SUCCESS', 'FAILED', 'PENDING', 'REFUNDED') THEN 'payment_status_allowed' END,
            CASE WHEN NOT coalesce(amount_paid > 0, false) THEN 'amount_paid_positive' END,
            CASE WHEN order_id NOT IN (SELECT order_id FROM stg_orders) THEN 'payment_has_order' END,
            CASE WHEN payment_ts > CAST(current_timestamp AS TIMESTAMP) THEN 'payment_ts_not_future' END
        ) AS failure_reason,
        concat_ws(';',
            CASE WHEN payment_mode IS NOT NULL
                  AND payment_mode_raw NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
                 THEN 'payment_mode_needs_standardising' END,
            CASE WHEN payment_status = 'SUCCESS' AND order_amount IS NOT NULL
                  AND abs(order_amount - amount_paid) > 0.01
                 THEN 'success_amount_equals_order_amount' END
        ) AS warning_reason
    FROM with_order
)
SELECT
    payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref,
    CASE WHEN failure_reason <> '' THEN 'REJECTED'
         WHEN warning_reason <> '' THEN 'WARNING'
         ELSE 'PASSED' END AS dq_status,
    failure_reason, warning_reason,
    _source_file, _run_id, _loaded_at
FROM checked;