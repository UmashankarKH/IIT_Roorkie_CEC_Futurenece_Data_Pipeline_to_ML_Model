-- Business question 4 (part 2): one row per order/payment mismatch, with a reason.
-- Same logic as build_reconciliation() in the ETL. Payments are read from staging INCLUDING
-- rejected ones (only duplicate copies are skipped), because a bad payment is itself a mismatch.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH pay AS (
    SELECT payment_id, order_id, payment_mode, amount_paid, payment_status
    FROM stg_payments WHERE is_first_copy
),
succ AS (
    SELECT order_id, sum(amount_paid) AS amount_paid, count(*) AS n_success
    FROM pay WHERE payment_status = 'SUCCESS' GROUP BY order_id
),
allp AS (
    SELECT order_id,
           string_agg(payment_id, ', ' ORDER BY payment_id)                  AS payment_ids,
           string_agg(DISTINCT payment_status, ', ' ORDER BY payment_status) AS statuses,
           bool_or(payment_status = 'REFUNDED')                              AS has_refund
    FROM pay GROUP BY order_id
),
f AS (
    SELECT o.order_id, o.order_status, o.order_amount, o.payment_mode,
           coalesce(s.amount_paid, 0) AS amount_paid, coalesce(s.n_success, 0) AS n_success,
           a.payment_ids, a.statuses, coalesce(a.has_refund, FALSE) AS has_refund,
           a.order_id IS NOT NULL AS has_any_payment
    FROM int_orders_enriched o
    LEFT JOIN succ s USING (order_id)
    LEFT JOIN allp a USING (order_id)
),
order_issues AS (
    SELECT *, 'NO_PAYMENT_RECORD' AS reason, -order_amount AS difference FROM f
        WHERE order_status = 'DELIVERED' AND NOT has_any_payment
    UNION ALL
    SELECT *, 'PAYMENT_FAILED', -order_amount FROM f
        WHERE order_status = 'DELIVERED' AND has_any_payment AND n_success = 0
    UNION ALL
    SELECT *, 'MULTIPLE_SUCCESS', amount_paid - order_amount FROM f
        WHERE order_status = 'DELIVERED' AND n_success > 1
    UNION ALL
    SELECT *, 'AMOUNT_MISMATCH', amount_paid - order_amount FROM f
        WHERE order_status = 'DELIVERED' AND n_success = 1 AND abs(amount_paid - order_amount) > 0.01
    UNION ALL
    SELECT *, 'PAYMENT_ON_CANCELLED', amount_paid FROM f
        WHERE order_status = 'CANCELLED' AND n_success > 0
    UNION ALL
    SELECT *, 'REFUND_MISSING', amount_paid FROM f
        WHERE order_status = 'RETURNED' AND n_success > 0 AND NOT has_refund
),
loose_payments AS (   -- payments that point at no reportable order
    SELECT p.order_id,
           CASE WHEN EXISTS (SELECT 1 FROM stg_orders so WHERE so.order_id = p.order_id)
                THEN 'PAYMENT_FOR_REJECTED_ORDER' ELSE 'ORPHAN_PAYMENT' END AS reason,
           CAST(NULL AS VARCHAR) AS order_status, CAST(NULL AS DOUBLE) AS order_amount,
           p.amount_paid, p.amount_paid AS difference, p.payment_mode,
           p.payment_id AS payment_ids, p.payment_status AS statuses
    FROM pay p
    WHERE NOT EXISTS (SELECT 1 FROM int_orders_enriched o WHERE o.order_id = p.order_id)
)
SELECT order_id, reason, order_status, order_amount, amount_paid, round(difference, 2) AS difference,
       payment_mode, payment_ids, statuses, '{{ run_id }}' AS _run_id
FROM (
    SELECT order_id, reason, order_status, order_amount, amount_paid, difference,
           payment_mode, payment_ids, statuses FROM order_issues
    UNION ALL
    SELECT order_id, reason, order_status, order_amount, amount_paid, difference,
           payment_mode, payment_ids, statuses FROM loose_payments
)
ORDER BY reason, order_id;
