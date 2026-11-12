-- Business question 4 (ETL): revenue by payment mode, plus the money that does
-- not reconcile with the payment gateway.
WITH revenue_by_mode AS (
    SELECT payment_mode,
           count(*)                   AS delivered_orders,
           round(sum(net_revenue), 2) AS net_revenue
    FROM etl_fact_orders
    WHERE is_revenue
    GROUP BY payment_mode
),
unreconciled AS (
    SELECT round(sum(abs(coalesce(difference, 0))), 2) AS unreconciled_amount,
           count(*)                                    AS mismatch_rows
    FROM etl_payment_reconciliation
)
SELECT r.payment_mode, r.delivered_orders, r.net_revenue,
       u.mismatch_rows, u.unreconciled_amount
FROM revenue_by_mode r CROSS JOIN unreconciled u
ORDER BY r.net_revenue DESC;
