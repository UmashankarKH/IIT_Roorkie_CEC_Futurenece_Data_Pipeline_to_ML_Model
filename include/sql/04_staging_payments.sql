-- NammaMart ELT: payment staging and reconciliation inputs.

-- Deduplicate exact payment rows.
CREATE OR REPLACE TABLE payments_distinct AS
SELECT DISTINCT * FROM raw_payments_current;

-- Identify payment IDs that map to conflicting records.
CREATE OR REPLACE TABLE payments_conflict_keys AS
SELECT payment_id
FROM payments_distinct
WHERE payment_id IS NOT NULL AND TRIM(payment_id) <> ''
GROUP BY payment_id
HAVING COUNT(*) > 1;

-- Keep missing IDs and conflicting IDs in quarantine.
CREATE OR REPLACE TABLE payments_key_quarantine AS
SELECT p.*, 'conflicting_payment_id' AS quarantine_reason
FROM payments_distinct p
JOIN payments_conflict_keys k USING (payment_id)

UNION ALL

SELECT p.*, 'missing_payment_id' AS quarantine_reason
FROM payments_distinct p
WHERE payment_id IS NULL OR TRIM(payment_id) = '';

-- Cast fields and evaluate payment-level quality checks.
CREATE OR REPLACE TABLE payments_validated AS
WITH typed AS (
    SELECT
        p.*,
        TRY_CAST(payment_ts AS TIMESTAMP) AS parsed_payment_ts,
        TRY_CAST(amount_paid AS DECIMAL(12,2)) AS parsed_amount_paid,
        UPPER(TRIM(payment_status)) AS normalized_payment_status,
        CASE
            WHEN UPPER(TRIM(payment_mode)) = 'COD'
                THEN 'CASH ON DELIVERY'
            ELSE UPPER(TRIM(payment_mode))
        END AS normalized_payment_mode
    FROM payments_distinct p
),
checked AS (
    SELECT
        t.*,
        o.order_amount AS matched_order_amount,
        CONCAT_WS(',',
            CASE
                WHEN t.payment_id IS NULL
                  OR TRIM(t.payment_id) = ''
                THEN 'missing_payment_id'
                WHEN k.payment_id IS NOT NULL
                THEN 'conflicting_payment_id'
            END,
            CASE
                WHEN t.order_id IS NULL OR TRIM(t.order_id) = ''
                THEN 'missing_order_id'
                WHEN o.order_id IS NULL
                THEN 'orphan_order_id'
            END,
            CASE
                WHEN t.payment_ts IS NULL OR TRIM(t.payment_ts) = ''
                THEN 'missing_payment_ts'
                WHEN t.parsed_payment_ts IS NULL
                THEN 'invalid_payment_ts'
                WHEN t.parsed_payment_ts > CURRENT_TIMESTAMP
                THEN 'future_payment_ts'
            END,
            CASE
                WHEN t.parsed_amount_paid IS NULL
                  OR t.parsed_amount_paid < 0
                THEN 'invalid_amount_paid'
            END,
            CASE
                WHEN t.normalized_payment_status NOT IN
                     ('SUCCESS', 'FAILED', 'REFUNDED')
                  OR t.normalized_payment_status IS NULL
                THEN 'invalid_payment_status'
            END,
            CASE
                WHEN t.normalized_payment_mode NOT IN
                     ('UPI', 'CARD', 'CASH ON DELIVERY', 'WALLET')
                  OR t.normalized_payment_mode IS NULL
                THEN 'invalid_payment_mode'
            END,
            CASE
                WHEN t.normalized_payment_status = 'SUCCESS'
                  AND o.order_id IS NOT NULL
                  AND (
                      t.parsed_amount_paid IS NULL
                      OR ROUND(t.parsed_amount_paid, 2)
                         <> ROUND(o.order_amount, 2)
                  )
                THEN 'payment_amount_mismatch'
            END
        ) AS failure_reason
    FROM typed t
    LEFT JOIN orders_curated o
        ON o.order_id = TRIM(t.order_id)
    LEFT JOIN payments_conflict_keys k
        ON k.payment_id = t.payment_id
)
SELECT * FROM checked;

-- Quarantine every failed payment, preserving its reasons.
CREATE OR REPLACE TABLE payments_quarantine AS
SELECT
    payment_id, order_id, payment_ts, payment_mode,
    amount_paid, payment_status, gateway_ref,
    failure_reason AS quarantine_reason
FROM payments_validated
WHERE COALESCE(failure_reason, '') <> '';

-- Publish only valid payments; retain status for reconciliation.
CREATE OR REPLACE TABLE stg_payments AS
SELECT
    TRIM(payment_id) AS payment_id,
    TRIM(order_id) AS order_id,
    parsed_payment_ts AS payment_ts,
    CASE normalized_payment_mode
        WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
        WHEN 'UPI' THEN 'UPI'
        WHEN 'CARD' THEN 'Card'
        WHEN 'WALLET' THEN 'Wallet'
    END AS payment_mode,
    parsed_amount_paid AS amount_paid,
    normalized_payment_status AS payment_status,
    NULLIF(TRIM(gateway_ref), '') AS gateway_ref,
    'PASS' AS dq_status,
    CAST(NULL AS VARCHAR) AS failure_reason
FROM payments_validated
WHERE COALESCE(failure_reason, '') = '';


-- Reconciliation uses only curated, valid successful payments.
-- Amount-mismatch payment records remain available in quarantine.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH valid_successful AS (
    SELECT
        TRIM(order_id) AS order_id,
        SUM(amount_paid) AS successful_amount_paid,
        COUNT(*) AS successful_payment_count
    FROM stg_payments
    WHERE payment_status = 'SUCCESS'
    GROUP BY TRIM(order_id)
),
successful AS (
    SELECT
        order_id,
        successful_amount_paid,
        successful_payment_count
    FROM valid_successful
),
quarantined_orders AS (
    SELECT
        TRIM(order_id) AS order_id,
        STRING_AGG(DISTINCT quarantine_reason, ',') AS quarantine_reason
    FROM orders_quarantine_final
    WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
    GROUP BY TRIM(order_id)
),
order_universe AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRY_CAST(order_amount AS DECIMAL(18,2)) AS order_amount,
        'CURATED' AS order_source
    FROM orders_curated
    WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''

    UNION ALL

    SELECT
        q.order_id,
        TRY_CAST(q.order_amount AS DECIMAL(18,2)) AS order_amount,
        'QUARANTINED' AS order_source
    FROM (
        SELECT DISTINCT order_id, order_amount
        FROM orders_quarantine_final
        WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
    ) q
    WHERE NOT EXISTS (
        SELECT 1 FROM orders_curated c WHERE c.order_id = q.order_id
    )

    UNION ALL

    SELECT
        s.order_id,
        CAST(NULL AS DECIMAL(18,2)) AS order_amount,
        'ORDER_MISSING' AS order_source
    FROM successful s
    WHERE NOT EXISTS (
        SELECT 1 FROM orders_curated c WHERE c.order_id = s.order_id
    )
      AND NOT EXISTS (
        SELECT 1 FROM orders_quarantine_final q
        WHERE q.order_id = s.order_id
    )
)
SELECT
    o.order_id,
    o.order_amount,
    s.successful_amount_paid,
    COALESCE(s.successful_payment_count, 0) AS successful_payment_count,
    CASE
        WHEN o.order_amount IS NULL THEN NULL
        ELSE ROUND(
            o.order_amount - COALESCE(s.successful_amount_paid, 0), 2
        )
    END AS amount_difference,
    CASE
        WHEN o.order_source = 'ORDER_MISSING' THEN 'ORDER_MISSING'
        WHEN o.order_source = 'QUARANTINED' THEN 'ORDER_QUARANTINED'
        WHEN s.order_id IS NULL THEN 'NO_SUCCESSFUL_PAYMENT'
        WHEN ABS(ROUND(
            o.order_amount - s.successful_amount_paid, 2
        )) > 0.01 THEN 'AMOUNT_MISMATCH'
        ELSE 'RECONCILED'
    END AS reconciliation_status
FROM order_universe o
LEFT JOIN successful s USING (order_id);

