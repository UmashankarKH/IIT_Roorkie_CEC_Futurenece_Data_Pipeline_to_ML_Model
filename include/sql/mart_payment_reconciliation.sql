-- MART: every order/payment mismatch, one row per problem, with the reason.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH pay AS (SELECT * FROM stg_payments WHERE NOT chk_payment_id_unique),
o AS (SELECT order_id, order_status, order_amount FROM stg_orders WHERE dq_status = 'PASS'),
succ AS (SELECT * FROM pay WHERE payment_status = 'SUCCESS'),
raw_ids AS (SELECT trim(order_id) AS order_id FROM raw_orders WHERE _run_id = '{{run_id}}')
SELECT order_id, CAST(NULL AS VARCHAR) AS payment_id, order_status, order_amount, CAST(NULL AS VARCHAR) AS payment_status,
       CAST(NULL AS DOUBLE) AS amount_paid, round(abs(order_amount), 2) AS unreconciled_amount,
       'DELIVERED_ORDER_HAS_NO_PAYMENT' AS reason
FROM o WHERE order_status = 'DELIVERED' AND order_id NOT IN (SELECT order_id FROM pay)
UNION ALL
SELECT o.order_id, p.payment_id, o.order_status, o.order_amount, p.payment_status, p.amount_paid,
       round(abs(o.order_amount), 2), 'DELIVERED_ORDER_ONLY_FAILED_PAYMENT'
FROM o JOIN pay p ON p.order_id = o.order_id
WHERE o.order_status = 'DELIVERED' AND o.order_id NOT IN (SELECT order_id FROM succ)
UNION ALL
SELECT o.order_id, p.payment_id, o.order_status, o.order_amount, p.payment_status, p.amount_paid,
       round(abs(round(p.amount_paid - o.order_amount, 2)), 2), 'AMOUNT_MISMATCH'
FROM succ p JOIN o ON o.order_id = p.order_id
WHERE abs(round(p.amount_paid - o.order_amount, 2)) > 0.01
UNION ALL
SELECT order_id, CAST(NULL AS VARCHAR), order_status, order_amount, CAST(NULL AS VARCHAR), CAST(NULL AS DOUBLE),
       round(abs(order_amount), 2), 'RETURNED_ORDER_HAS_NO_PAYMENT'
FROM o WHERE order_status = 'RETURNED' AND order_id NOT IN (SELECT order_id FROM pay)
UNION ALL
SELECT o.order_id, p.payment_id, o.order_status, o.order_amount, p.payment_status, p.amount_paid,
       round(abs(p.amount_paid), 2), 'RETURNED_ORDER_NOT_REFUNDED'
FROM succ p JOIN o ON o.order_id = p.order_id
WHERE o.order_status = 'RETURNED'
  AND o.order_id NOT IN (SELECT order_id FROM pay WHERE payment_status = 'REFUNDED')
UNION ALL
SELECT p.order_id, p.payment_id, CAST(NULL AS VARCHAR), CAST(NULL AS DOUBLE), p.payment_status, p.amount_paid,
       round(abs(p.amount_paid), 2), 'PAYMENT_FOR_REJECTED_ORDER'
FROM pay p
WHERE p.order_id NOT IN (SELECT order_id FROM o) AND p.order_id IN (SELECT order_id FROM raw_ids)
UNION ALL
SELECT p.order_id, p.payment_id, CAST(NULL AS VARCHAR), CAST(NULL AS DOUBLE), p.payment_status, p.amount_paid,
       round(abs(p.amount_paid), 2), 'ORPHAN_PAYMENT_NO_SUCH_ORDER'
FROM pay p
WHERE p.order_id NOT IN (SELECT order_id FROM o) AND p.order_id NOT IN (SELECT order_id FROM raw_ids);
