-- include/sql/nammamart/mart_payment_reconciliation.sql
-- BQ4: Revenue by payment mode, and total that doesn't reconcile

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
-- Revenue summary by payment mode
WITH order_summary AS (
    SELECT
        o.order_id,
        o.payment_mode                          AS order_payment_mode,
        o.order_status,
        o.net_revenue                           AS order_amount,
        p.payment_id,
        p.amount_paid,
        p.payment_status,
        p.payment_mode                          AS payment_payment_mode,
        CASE
            WHEN o.order_status = 'DELIVERED' AND p.payment_id IS NULL
                THEN 'DELIVERED order has no SUCCESS payment'
            WHEN o.order_status = 'DELIVERED'
                 AND ABS(o.net_revenue - p.amount_paid) > 0.05
                THEN 'Amount mismatch'
            WHEN o.order_status = 'DELIVERED'
                 AND o.payment_mode <> p.payment_mode
                THEN 'Payment mode mismatch'
            ELSE NULL
        END AS mismatch_reason
    FROM stg_orders o
    LEFT JOIN stg_payments p
           ON o.order_id = p.order_id
          AND p.payment_status = 'SUCCESS'
          AND p.dq_status = 'PASS'
    WHERE o.dq_status = 'PASS'
)
SELECT
    order_id,
    order_payment_mode,
    order_status,
    order_amount,
    payment_id,
    amount_paid,
    payment_status,
    payment_payment_mode,
    ROUND(order_amount - COALESCE(amount_paid, 0), 2) AS difference,
    mismatch_reason
FROM order_summary
WHERE mismatch_reason IS NOT NULL
ORDER BY mismatch_reason, order_id;
