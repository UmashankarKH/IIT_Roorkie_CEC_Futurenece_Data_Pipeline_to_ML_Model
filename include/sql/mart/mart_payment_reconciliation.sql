-- include/sql/mart/mart_payment_reconciliation.sql
-- ELT layer 3: MART - answers business question 4 and client acceptance criterion 7
--
-- "Revenue by payment mode, and the total amount that doesn't reconcile with
--  the payment gateway."
--
-- One row per mismatch between an order and the payment gateway export, each
-- with the reason finance needs:
--
--   MISSING_PAYMENT         delivered order with no successful payment
--   ORPHAN_PAYMENT          successful payment whose order is not in the mart
--   AMOUNT_MISMATCH         amount paid differs from the order amount
--   PAYMENT_FOR_NON_REVENUE successful payment against a cancelled/returned order
--   DUPLICATE_PAYMENT       more than one successful payment for the same order
--   PAYMENT_MODE_MISMATCH   gateway mode differs from the order mode
--
-- Reads  : stg_orders (PASS only) and all non-superseded rows of stg_payments.
--          Quarantined payments are deliberately included, because a payment
--          that failed a quality rule is exactly what finance must chase.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH clean_orders AS (
    SELECT order_id, order_amount, order_status, payment_mode, net_revenue
    FROM stg_orders
    WHERE dq_status = 'PASS'
),
payments AS (
    SELECT payment_id, order_id, payment_status, payment_mode, amount_paid, dq_status
    FROM stg_payments
    WHERE dq_status <> 'SUPERSEDED'
),
successful AS (
    SELECT * FROM payments WHERE payment_status = 'SUCCESS'
),
matched AS (
    SELECT p.payment_id, p.order_id, p.amount_paid, p.payment_mode AS payment_mode_pay,
           p.payment_status, o.order_amount, o.order_status, o.payment_mode AS payment_mode_ord,
           round(p.amount_paid - o.order_amount, 2) AS difference
    FROM successful p
    JOIN clean_orders o ON o.order_id = p.order_id
)
SELECT * FROM (
    -- 1. delivered orders that were never paid
    SELECT o.order_id, NULL AS payment_id, o.order_amount, 0.0 AS amount_paid,
           round(o.order_amount, 2) AS difference, o.order_status, NULL AS payment_status,
           'MISSING_PAYMENT' AS mismatch_reason
    FROM clean_orders o
    WHERE o.order_status = 'DELIVERED'
      AND NOT EXISTS (SELECT 1 FROM successful p WHERE p.order_id = o.order_id)

    UNION ALL
    -- 2. successful payments with no matching clean order
    SELECT p.order_id, p.payment_id, NULL AS order_amount, p.amount_paid,
           round(p.amount_paid, 2) AS difference, NULL AS order_status, p.payment_status,
           'ORPHAN_PAYMENT' AS mismatch_reason
    FROM successful p
    WHERE NOT EXISTS (SELECT 1 FROM clean_orders o WHERE o.order_id = p.order_id)

    UNION ALL
    -- 3. amount does not match the order
    SELECT order_id, payment_id, order_amount, amount_paid, difference, order_status,
           payment_status, 'AMOUNT_MISMATCH' AS mismatch_reason
    FROM matched
    WHERE abs(difference) > 0.01

    UNION ALL
    -- 4. money collected for an order that is not revenue
    SELECT order_id, payment_id, order_amount, amount_paid, difference, order_status,
           payment_status, 'PAYMENT_FOR_NON_REVENUE' AS mismatch_reason
    FROM matched
    WHERE order_status <> 'DELIVERED'

    UNION ALL
    -- 5. the same payment sent twice by the gateway, or one order paid twice.
    --    _raw_copies records how many identical copies of the row arrived, so a
    --    repeated gateway export is still visible after staging collapsed it.
    SELECT m.order_id, m.payment_id, m.order_amount, m.amount_paid, m.difference,
           m.order_status, m.payment_status, 'DUPLICATE_PAYMENT' AS mismatch_reason
    FROM matched m
    JOIN stg_payments sp ON sp.payment_id = m.payment_id
    WHERE sp._raw_copies > 1
       OR m.order_id IN (SELECT order_id FROM matched GROUP BY order_id HAVING count(*) > 1)

    UNION ALL
    -- 6. gateway mode disagrees with the order mode
    SELECT order_id, payment_id, order_amount, amount_paid, difference, order_status,
           payment_status, 'PAYMENT_MODE_MISMATCH' AS mismatch_reason
    FROM matched
    WHERE payment_mode_pay IS NOT NULL
      AND payment_mode_ord IS NOT NULL
      AND payment_mode_pay <> payment_mode_ord
) ORDER BY mismatch_reason, order_id;
