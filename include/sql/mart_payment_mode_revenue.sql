-- Business question 4 (part 1): revenue by payment mode, with the amount that
-- doesn't reconcile with the payment gateway for that mode.

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
WITH revenue AS (
    SELECT payment_mode,
           count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
           round(sum(net_revenue), 2) AS net_revenue
    FROM stg_orders
    WHERE dq_status = 'PASSED'
    GROUP BY payment_mode
),

gaps AS (
    SELECT coalesce(payment_mode, gateway_payment_mode) AS payment_mode,
           count(*) AS mismatches,
           round(sum(unreconciled_amount), 2) AS unreconciled_amount
    FROM mart_payment_reconciliation
    GROUP BY 1
)

SELECT
    r.payment_mode,
    r.delivered_orders,
    r.net_revenue,
    coalesce(g.mismatches, 0) AS mismatches,
    coalesce(g.unreconciled_amount, 0) AS unreconciled_amount
FROM revenue r
LEFT JOIN gaps g USING (payment_mode)
ORDER BY r.net_revenue DESC;
