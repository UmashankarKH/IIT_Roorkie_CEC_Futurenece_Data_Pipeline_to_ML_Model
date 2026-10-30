CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH clean_orders AS (
    SELECT * FROM stg_orders WHERE dq_status <> 'FAIL'
), payment_rows AS (
    SELECT p.*, o.order_amount, o.payment_mode AS order_payment_mode,
           o.order_status, o.dq_status AS order_dq_status,
           COUNT(*) OVER (PARTITION BY p.order_id) AS order_payment_count
    FROM stg_payments p
    LEFT JOIN clean_orders o USING (order_id)
), payment_mismatches AS (
    SELECT
        order_id,
        payment_id,
        COALESCE(order_payment_mode, payment_mode, 'UNKNOWN') AS payment_mode,
        order_amount,
        amount_paid,
        ROUND(CASE WHEN order_amount IS NULL THEN COALESCE(amount_paid, 0)
                   WHEN payment_status <> 'SUCCESS' AND order_status <> 'CANCELLED' THEN order_amount
                   ELSE ABS(order_amount - COALESCE(amount_paid, 0)) END, 2) AS mismatch_amount,
        COALESCE(NULLIF(failure_reason, ''),
            CASE
                WHEN order_id IS NULL THEN 'payment order_id is missing'
                WHEN order_dq_status IS NULL THEN 'payment has no clean matching order'
                WHEN order_payment_count > 1 THEN 'multiple payment rows for one order'
                WHEN order_status = 'CANCELLED' THEN 'cancelled order has a payment row'
                WHEN payment_status <> 'SUCCESS' THEN 'payment status is not SUCCESS'
                WHEN ABS(order_amount - amount_paid) > 0.005 THEN 'payment amount differs from order_amount'
                WHEN payment_mode <> order_payment_mode THEN 'payment mode differs from order'
            END
        ) AS reason
    FROM payment_rows
    WHERE dq_status = 'FAIL'
       OR order_dq_status IS NULL
       OR order_payment_count > 1
       OR order_status = 'CANCELLED'
       OR payment_status <> 'SUCCESS'
       OR ABS(order_amount - amount_paid) > 0.005
       OR payment_mode <> order_payment_mode
), missing_payments AS (
    SELECT
        o.order_id,
        CAST(NULL AS VARCHAR) AS payment_id,
        o.payment_mode,
        o.order_amount,
        CAST(NULL AS DOUBLE) AS amount_paid,
        ROUND(o.order_amount, 2) AS mismatch_amount,
        'no payment row for non-cancelled order' AS reason
    FROM clean_orders o
    WHERE o.order_status <> 'CANCELLED'
      AND NOT EXISTS (SELECT 1 FROM stg_payments p WHERE p.order_id = o.order_id)
)
SELECT * FROM payment_mismatches
UNION ALL
SELECT * FROM missing_payments
ORDER BY order_id, payment_id;
