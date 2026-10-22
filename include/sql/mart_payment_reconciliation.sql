-- Mart: one row per order/payment that does not reconcile (business question 4, second half). Placeholders: {run_id}
-- Expected payment per order status: DELIVERED -> SUCCESS, RETURNED -> REFUNDED, CANCELLED -> no payment.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH good_orders AS (SELECT * FROM stg_orders WHERE dq_status <> 'FAILED'),
good_payments AS (SELECT * FROM stg_payments WHERE dq_status <> 'FAILED'),
per_order AS (
    SELECT o.order_id, o.order_status, o.order_amount,
           COUNT(p.payment_id) AS n_pay,
           COUNT(*) FILTER (WHERE p.payment_status = CASE o.order_status WHEN 'DELIVERED' THEN 'SUCCESS'
                                                       WHEN 'RETURNED' THEN 'REFUNDED' END) AS n_expected,
           SUM(p.amount_paid) FILTER (WHERE p.payment_status = CASE o.order_status WHEN 'DELIVERED' THEN 'SUCCESS'
                                                       WHEN 'RETURNED' THEN 'REFUNDED' END) AS expected_paid,
           SUM(p.amount_paid) AS any_paid,
           STRING_AGG(p.payment_id, ',') AS payment_ids,
           STRING_AGG(DISTINCT p.payment_status, ',') AS payment_statuses
    FROM good_orders o LEFT JOIN good_payments p USING (order_id)
    GROUP BY ALL
)
SELECT order_id, payment_ids, order_status, order_amount, payment_statuses,
       CASE WHEN order_status = 'CANCELLED' THEN any_paid ELSE expected_paid END AS payment_amount,
       ROUND(CASE WHEN order_status = 'CANCELLED' THEN -any_paid
                  ELSE order_amount - COALESCE(expected_paid, 0) END, 2) AS difference,
       CASE WHEN order_status = 'CANCELLED' THEN 'CANCELLED_ORDER_HAS_PAYMENT'
            WHEN n_pay = 0 THEN 'NO_PAYMENT_FOUND'
            WHEN n_expected = 0 THEN 'PAYMENT_STATUS_MISMATCH'
            ELSE 'AMOUNT_MISMATCH' END AS mismatch_reason,
       '{run_id}' AS run_id
FROM per_order
WHERE (order_status = 'CANCELLED' AND n_pay > 0)
   OR (order_status <> 'CANCELLED' AND (n_pay = 0 OR n_expected = 0 OR ABS(order_amount - expected_paid) > 0.01))
UNION ALL
SELECT p.order_id, p.payment_id, NULL, NULL, p.payment_status, p.amount_paid, ROUND(-p.amount_paid, 2),
       CASE WHEN p.order_in_source THEN 'PAYMENT_FOR_REJECTED_ORDER' ELSE 'PAYMENT_FOR_UNKNOWN_ORDER' END, '{run_id}'
FROM good_payments p
WHERE NOT EXISTS (SELECT 1 FROM good_orders o WHERE o.order_id = p.order_id);
