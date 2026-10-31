-- Mart: one row per order/payment mismatch, with a reason.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH fact AS (
    SELECT order_id, order_amount, order_status FROM stg_orders WHERE dq_status <> 'REJECTED'
),
pay_by_order AS (
    SELECT order_id,
           count(*) FILTER (WHERE payment_status = 'SUCCESS')        AS n_success,
           sum(amount_paid) FILTER (WHERE payment_status = 'SUCCESS') AS success_amount,
           array_to_string(list_sort(list_distinct(list(payment_status))), '_') AS statuses
    FROM stg_payments
    GROUP BY order_id
),
order_side AS (
    SELECT f.order_id,
           NULL::VARCHAR AS payment_id,
           f.order_amount,
           CASE WHEN p.order_id IS NULL THEN 'MISSING_PAYMENT'
                WHEN p.n_success = 0 THEN
                     CASE WHEN p.statuses LIKE '%REFUNDED%' AND f.order_status = 'RETURNED' THEN NULL
                          ELSE 'PAYMENT_' || p.statuses END
                WHEN abs(p.success_amount - f.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
           END AS reason,
           p.success_amount
    FROM fact f
    LEFT JOIN pay_by_order p ON p.order_id = f.order_id
    WHERE f.order_status <> 'CANCELLED'
),
payment_side AS (
    SELECT p.order_id, p.payment_id,
           NULL::DOUBLE AS order_amount,
           p.amount_paid,
           CASE WHEN p.order_id NOT IN (SELECT order_id FROM stg_orders) THEN 'ORPHAN_PAYMENT'
                WHEN p.order_id NOT IN (SELECT order_id FROM fact) THEN 'ORDER_QUARANTINED'
                WHEN p.order_id IN (SELECT order_id FROM fact WHERE order_status = 'CANCELLED')
                     THEN 'PAYMENT_FOR_CANCELLED_ORDER'
           END AS reason
    FROM stg_payments p
),
combined AS (
    SELECT order_id, payment_id, order_amount,
           CASE WHEN reason = 'AMOUNT_MISMATCH' THEN success_amount END AS amount_paid,
           reason
    FROM order_side WHERE reason IS NOT NULL
    UNION ALL
    SELECT order_id, payment_id, order_amount, amount_paid, reason
    FROM payment_side WHERE reason IS NOT NULL
)
SELECT order_id, payment_id, order_amount, amount_paid, reason,
       coalesce(order_amount, 0) - coalesce(amount_paid, 0) AS difference
FROM combined;