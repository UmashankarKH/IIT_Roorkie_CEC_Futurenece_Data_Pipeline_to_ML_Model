-- One row per order/payment mismatch, with the reason finance needs to chase it.
-- Mirrors reconcile_payments() in include/nammamart/transforms.py, including the
-- order the reasons are tested in: the first one that applies is the one reported.
--
-- A FULL OUTER JOIN is the point of this table. An inner join would hide the two
-- worst cases: a payment with no matching order, and a delivered order nobody paid for.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH paid AS (
    SELECT order_id,
           round(sum(amount_paid), 2) AS amount_paid,
           count(*)                   AS payment_count
    FROM stg_payments
    WHERE dq_status = 'PASSED' AND payment_status = 'SUCCESS'
    GROUP BY order_id
),
joined AS (
    SELECT
        coalesce(f.order_id, p.order_id) AS order_id,
        f.order_id IS NULL               AS payment_without_order,
        f.order_status,
        f.net_revenue,
        p.amount_paid,
        p.payment_count
    FROM mart_fact_orders f
    FULL OUTER JOIN paid p ON p.order_id = f.order_id
),
classified AS (
    SELECT *,
        round(coalesce(amount_paid, 0) - coalesce(net_revenue, 0), 2) AS variance,
        CASE
            WHEN payment_without_order
                THEN 'payment with no matching clean order'
            WHEN order_status = 'DELIVERED' AND amount_paid IS NULL
                THEN 'delivered order with no successful payment'
            WHEN order_status = 'CANCELLED' AND amount_paid IS NOT NULL
                THEN 'cancelled order that was still paid'
            WHEN amount_paid IS NOT NULL AND net_revenue IS NOT NULL
                 AND abs(amount_paid - net_revenue) > 0.01
                THEN 'paid ' || printf('%+.2f', amount_paid - net_revenue)
                     || ' against the order amount'
            WHEN payment_count > 1
                THEN 'more than one successful payment for the order'
            ELSE NULL
        END AS mismatch_reason
    FROM joined
)
SELECT order_id, order_status, net_revenue, amount_paid, payment_count,
       variance, mismatch_reason
FROM classified
WHERE mismatch_reason IS NOT NULL
ORDER BY abs(variance) DESC, order_id;
