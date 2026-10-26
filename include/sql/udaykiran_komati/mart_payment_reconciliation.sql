CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT
    o.order_id,
    o.order_amount,
    p.amount_paid,
    p.payment_status,
    p.payment_mode,
    CASE
        WHEN p.payment_id IS NULL THEN 'missing payment row'
        WHEN p.payment_status = 'SUCCESS'
             AND abs(p.amount_paid - o.order_amount) > 0.02 THEN 'amount mismatch'
        ELSE 'other mismatch'
    END AS mismatch_reason
FROM stg_orders o
LEFT JOIN stg_payments p ON o.order_id = p.order_id
WHERE o.dq_status = 'PASS'
  AND (
        p.payment_id IS NULL
        OR (p.payment_status = 'SUCCESS' AND abs(p.amount_paid - o.order_amount) > 0.02)
      );
