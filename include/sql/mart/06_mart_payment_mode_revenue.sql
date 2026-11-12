-- Business question 4 (part 1): revenue by payment mode, next to the unreconciled amount per mode
CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
WITH rev AS (
    SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
    FROM int_orders_enriched WHERE order_status = 'DELIVERED' GROUP BY payment_mode
),
recon AS (
    SELECT payment_mode, count(*) AS mismatches, round(sum(abs(difference)), 2) AS unreconciled_amount
    FROM mart_payment_reconciliation GROUP BY payment_mode
)
SELECT coalesce(rev.payment_mode, recon.payment_mode) AS payment_mode,
       coalesce(delivered_orders, 0) AS delivered_orders,
       coalesce(net_revenue, 0)      AS net_revenue,
       coalesce(mismatches, 0)       AS mismatches,
       coalesce(unreconciled_amount, 0) AS unreconciled_amount
FROM rev FULL OUTER JOIN recon USING (payment_mode)
ORDER BY net_revenue DESC;
