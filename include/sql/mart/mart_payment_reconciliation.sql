-- Q4 / acceptance criterion 7: one row per order/payment mismatch, with a reason.
-- expected = order_amount for DELIVERED orders (0 otherwise)
-- received = amount_paid for SUCCESS payments (0 otherwise)
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH o AS (
    SELECT order_id, order_status, order_amount, payment_mode FROM v_clean_orders
),
p AS (
    SELECT payment_id, order_id, payment_status, amount_paid, payment_mode
    FROM stg_payments WHERE dq_status = 'PASSED'
),
m AS (
    SELECT
        coalesce(o.order_id, p.order_id)  AS order_id,
        p.payment_id,
        o.order_status, p.payment_status,
        o.payment_mode AS payment_mode_order,
        p.payment_mode AS payment_mode_gateway,
        o.order_amount, p.amount_paid,
        o.order_id IS NOT NULL AS in_orders,
        p.order_id IS NOT NULL AS in_payments,
        round(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END, 2) AS expected_amount,
        round(CASE WHEN p.payment_status = 'SUCCESS' THEN p.amount_paid ELSE 0 END, 2)  AS received_amount
    FROM o FULL OUTER JOIN p ON o.order_id = p.order_id
)
SELECT * FROM (
SELECT
    order_id, payment_id, order_status, payment_status, payment_mode_order, payment_mode_gateway,
    order_amount, amount_paid, expected_amount, received_amount,
    round(expected_amount - received_amount, 2) AS difference,
    nullif(concat_ws('; ',
        CASE WHEN NOT in_orders THEN 'payment for an order not in finance table (quarantined or unknown)' END,
        CASE WHEN NOT in_payments AND order_status = 'DELIVERED' THEN 'DELIVERED order has no payment' END,
        CASE WHEN NOT in_payments AND order_status = 'RETURNED' THEN 'RETURNED order has no payment/refund record' END,
        CASE WHEN in_orders AND in_payments AND order_status = 'DELIVERED' AND payment_status = 'FAILED'
             THEN 'DELIVERED order but payment FAILED' END,
        CASE WHEN in_orders AND in_payments AND order_status = 'DELIVERED' AND payment_status = 'REFUNDED'
             THEN 'DELIVERED order but payment REFUNDED' END,
        CASE WHEN in_orders AND in_payments AND order_status = 'RETURNED' AND payment_status = 'SUCCESS'
             THEN 'RETURNED order not yet refunded' END,
        CASE WHEN in_orders AND in_payments AND order_status = 'CANCELLED' AND payment_status = 'SUCCESS'
             THEN 'CANCELLED order was charged' END,
        CASE WHEN in_orders AND in_payments AND order_status = 'DELIVERED' AND payment_status = 'SUCCESS'
                  AND abs(expected_amount - received_amount) > 0.01
             THEN 'amount_paid differs from order_amount' END,
        CASE WHEN in_orders AND in_payments AND payment_mode_order IS DISTINCT FROM payment_mode_gateway
             THEN 'payment_mode differs between order and gateway' END
    ), '') AS reason
FROM m
)
WHERE reason IS NOT NULL
ORDER BY order_id;
