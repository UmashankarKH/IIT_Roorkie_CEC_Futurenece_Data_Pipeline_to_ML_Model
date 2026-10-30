CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
WITH mode_revenue AS (
    SELECT payment_mode, ROUND(SUM(order_amount), 2) AS delivered_revenue,
           COUNT(*) AS delivered_orders
    FROM stg_orders
    WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED'
    GROUP BY payment_mode
), mode_mismatches AS (
    SELECT COALESCE(payment_mode, 'UNKNOWN') AS payment_mode,
           ROUND(SUM(mismatch_amount), 2) AS unreconciled_amount,
           COUNT(*) AS mismatch_rows
    FROM mart_payment_reconciliation
    GROUP BY COALESCE(payment_mode, 'UNKNOWN')
), all_modes AS (
    SELECT payment_mode FROM mode_revenue
    UNION
    SELECT payment_mode FROM mode_mismatches
)
SELECT a.payment_mode, COALESCE(r.delivered_orders, 0) AS delivered_orders,
       COALESCE(r.delivered_revenue, 0) AS delivered_revenue,
       COALESCE(m.unreconciled_amount, 0) AS unreconciled_amount,
       COALESCE(m.mismatch_rows, 0) AS mismatch_rows
FROM all_modes a
LEFT JOIN mode_revenue r USING (payment_mode)
LEFT JOIN mode_mismatches m USING (payment_mode)
ORDER BY a.payment_mode;
