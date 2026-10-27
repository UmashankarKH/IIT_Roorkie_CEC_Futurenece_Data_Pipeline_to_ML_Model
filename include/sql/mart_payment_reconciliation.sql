-- include/sql/mart_payment_reconciliation.sql
-- Answers Business Question 4: Payment reconciliation between clean orders and gateway payments
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH matched AS (
    SELECT
        o.order_id,
        p.payment_id,
        o.order_amount,
        p.amount_paid,
        o.order_status,
        p.payment_status,
        o.payment_mode AS order_payment_mode,
        p.payment_mode AS gateway_payment_mode,
        p.gateway_ref,
        CASE
            WHEN p.payment_id IS NULL AND o.order_status != 'CANCELLED' THEN 'MISSING_PAYMENT_FOR_ACTIVE_ORDER'
            WHEN p.payment_status = 'REFUNDED' THEN 'PAYMENT_STATUS_REFUNDED'
            WHEN p.payment_status = 'FAILED' THEN 'PAYMENT_STATUS_FAILED'
            WHEN p.payment_status = 'SUCCESS' AND ABS(o.order_amount - p.amount_paid) > 0.02 THEN 'AMOUNT_MISMATCH'
            WHEN p.payment_status = 'SUCCESS' AND o.payment_mode != p.payment_mode THEN 'PAYMENT_MODE_MISMATCH'
            ELSE 'RECONCILED'
        END AS reconciliation_status
    FROM stg_orders o
    LEFT JOIN stg_payments p ON o.order_id = p.order_id
    WHERE o.dq_status = 'PASSED'
)
SELECT
    order_id,
    payment_id,
    order_amount,
    amount_paid,
    order_status,
    payment_status,
    order_payment_mode,
    gateway_payment_mode,
    reconciliation_status,
    CASE
        WHEN reconciliation_status = 'RECONCILED' THEN 0.0
        WHEN amount_paid IS NOT NULL AND reconciliation_status = 'AMOUNT_MISMATCH' THEN ABS(order_amount - amount_paid)
        ELSE order_amount
    END AS unreconciled_amount
FROM matched
WHERE reconciliation_status != 'RECONCILED'
ORDER BY unreconciled_amount DESC;
