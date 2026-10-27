-- Business question 4 (part 2): every order / payment mismatch, with one reason per row.
-- Orders side   = clean orders (what finance reports).
-- Payments side = everything the gateway sent, one copy per payment_id.
-- First matching rule wins. unreconciled_amount is the money that doesn't agree.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH orders AS (
    SELECT order_id, order_status, order_amount, payment_mode
    FROM stg_orders
    WHERE dq_status = 'PASSED'
),

payments AS (
    SELECT payment_id, order_id, payment_status, payment_mode AS gateway_payment_mode, amount_paid
    FROM stg_payments
    WHERE dup_rank = 1
),

joined AS (
    SELECT
        coalesce(o.order_id, p.order_id) AS order_id,
        p.payment_id,
        o.order_status,
        p.payment_status,
        o.payment_mode,
        p.gateway_payment_mode,
        o.order_amount,
        p.amount_paid,
        CASE
            WHEN o.order_id IS NULL THEN 'payment_without_finance_order'
            WHEN o.order_status = 'DELIVERED' AND p.payment_id IS NULL THEN 'delivered_order_missing_payment'
            WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'FAILED' THEN 'payment_failed_for_delivered_order'
            WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'REFUNDED' THEN 'delivered_order_refunded'
            WHEN o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS' THEN 'cancelled_order_has_payment'
            WHEN o.order_status = 'RETURNED' AND p.payment_status = 'SUCCESS' THEN 'returned_order_not_refunded'
            WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'SUCCESS'
                 AND abs(o.order_amount - p.amount_paid) > 0.01 THEN 'amount_mismatch'
            WHEN p.payment_id IS NOT NULL AND o.payment_mode <> p.gateway_payment_mode THEN 'payment_mode_mismatch'
        END AS mismatch_reason
    FROM orders o
    FULL OUTER JOIN payments p ON p.order_id = o.order_id
)

SELECT
    *,
    round(abs(CASE mismatch_reason
        WHEN 'payment_without_finance_order' THEN amount_paid
        WHEN 'delivered_order_missing_payment' THEN order_amount
        WHEN 'payment_failed_for_delivered_order' THEN order_amount
        WHEN 'delivered_order_refunded' THEN order_amount
        WHEN 'cancelled_order_has_payment' THEN amount_paid
        WHEN 'returned_order_not_refunded' THEN amount_paid
        WHEN 'amount_mismatch' THEN order_amount - amount_paid
        ELSE 0
    END), 2) AS unreconciled_amount
FROM joined
WHERE mismatch_reason IS NOT NULL
ORDER BY unreconciled_amount DESC;
