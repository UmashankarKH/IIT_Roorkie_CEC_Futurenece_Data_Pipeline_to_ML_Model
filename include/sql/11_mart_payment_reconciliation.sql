-- include/sql/11_mart_payment_reconciliation.sql
-- Mart: Revenue by payment mode and reconciliation details with payment gateway.
-- Answers Business Question 4.

CREATE OR REPLACE TABLE mart_payment_reconciliation_details AS
WITH
delivered_orders AS (
    SELECT order_id, order_amount, payment_mode
    FROM stg_orders
    WHERE dq_status = 'PASSED' AND order_status = 'DELIVERED'
),
successful_payments AS (
    SELECT payment_id, order_id, amount_paid, payment_mode
    FROM stg_payments
    WHERE payment_status = 'SUCCESS'
)
-- 1. Delivered orders with no SUCCESS payment
SELECT
    d.order_id,
    p.payment_id,
    d.payment_mode,
    d.order_amount,
    COALESCE(p.amount_paid, 0.0) AS amount_paid,
    d.order_amount AS unreconciled_amount,
    'Delivered order missing SUCCESS payment' AS issue_type
FROM delivered_orders d
LEFT JOIN successful_payments p ON d.order_id = p.order_id
WHERE p.payment_id IS NULL

UNION ALL

-- 2. Delivered orders where payment amount != order amount
SELECT
    d.order_id,
    p.payment_id,
    d.payment_mode,
    d.order_amount,
    p.amount_paid,
    ROUND(ABS(d.order_amount - p.amount_paid), 2) AS unreconciled_amount,
    'Amount mismatch between order and payment' AS issue_type
FROM delivered_orders d
JOIN successful_payments p ON d.order_id = p.order_id
WHERE ROUND(d.order_amount, 2) != ROUND(p.amount_paid, 2)

UNION ALL

-- 3. SUCCESS payment exists for non-delivered order (Cancelled/Returned)
SELECT
    o.order_id,
    p.payment_id,
    p.payment_mode,
    o.order_amount,
    p.amount_paid,
    p.amount_paid AS unreconciled_amount,
    'SUCCESS payment on cancelled/returned order' AS issue_type
FROM stg_orders o
JOIN successful_payments p ON o.order_id = p.order_id
WHERE o.order_status IN ('CANCELLED', 'RETURNED')

UNION ALL

-- 4. SUCCESS payment with no corresponding order in staging
SELECT
    p.order_id,
    p.payment_id,
    p.payment_mode,
    NULL AS order_amount,
    p.amount_paid,
    p.amount_paid AS unreconciled_amount,
    'Payment without valid order' AS issue_type
FROM successful_payments p
LEFT JOIN stg_orders o ON p.order_id = o.order_id
WHERE o.order_id IS NULL;


CREATE OR REPLACE TABLE mart_revenue_by_payment_mode AS
SELECT
    o.payment_mode,
    COUNT(o.order_id) AS delivered_orders,
    ROUND(SUM(o.net_revenue), 2) AS reported_net_revenue,
    COALESCE(ROUND(SUM(r.unreconciled_amount), 2), 0.0) AS unreconciled_amount,
    ROUND(SUM(o.net_revenue) - COALESCE(SUM(r.unreconciled_amount), 0.0), 2) AS reconciled_net_revenue
FROM stg_orders o
LEFT JOIN (
    SELECT order_id, SUM(unreconciled_amount) AS unreconciled_amount
    FROM mart_payment_reconciliation_details
    GROUP BY order_id
) r ON o.order_id = r.order_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY o.payment_mode
ORDER BY reported_net_revenue DESC;
