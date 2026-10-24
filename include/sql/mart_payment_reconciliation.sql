-- include/sql/mart_payment_reconciliation.sql
-- Mart Layer: Payment reconciliation between order management system and payment gateway
-- Answers Business Question 4: Revenue by payment mode and unreconciled amounts

-- 1. Detailed order-level mismatch table
CREATE OR REPLACE TABLE mart_payment_reconciliation_details AS
WITH order_payments AS (
    SELECT
        o.order_id,
        o.payment_mode AS order_payment_mode,
        o.order_amount,
        o.order_status,
        p.payment_id,
        p.payment_mode AS gateway_payment_mode,
        p.amount_paid,
        p.payment_status,
        p.gateway_ref
    FROM stg_orders o
    FULL OUTER JOIN stg_payments p ON o.order_id = p.order_id
)
SELECT
    order_id,
    COALESCE(order_payment_mode, gateway_payment_mode) AS payment_mode,
    order_amount,
    order_status,
    payment_id,
    amount_paid,
    payment_status,
    gateway_ref,
    CASE
        WHEN order_status = 'DELIVERED' AND (payment_status IS NULL OR payment_status != 'SUCCESS') 
            THEN 'DELIVERED_WITHOUT_SUCCESSFUL_PAYMENT'
        WHEN order_status = 'DELIVERED' AND payment_status = 'SUCCESS' AND ABS(order_amount - amount_paid) > 0.05
            THEN 'AMOUNT_MISMATCH'
        WHEN order_status = 'CANCELLED' AND payment_status = 'SUCCESS'
            THEN 'PAYMENT_COLLECTED_FOR_CANCELLED_ORDER'
        WHEN order_id IS NULL OR order_status IS NULL
            THEN 'ORPHAN_GATEWAY_PAYMENT'
        ELSE 'RECONCILED'
    END AS reconciliation_status,
    CASE
        WHEN order_status = 'DELIVERED' AND (payment_status IS NULL OR payment_status != 'SUCCESS') 
            THEN order_amount
        WHEN order_status = 'DELIVERED' AND payment_status = 'SUCCESS' AND ABS(order_amount - amount_paid) > 0.05
            THEN ABS(order_amount - amount_paid)
        WHEN order_status = 'CANCELLED' AND payment_status = 'SUCCESS'
            THEN amount_paid
        WHEN order_id IS NULL OR order_status IS NULL
            THEN amount_paid
        ELSE 0.00
    END AS unreconciled_discrepancy_amount
FROM order_payments
WHERE reconciliation_status != 'RECONCILED';

-- 2. Summary by payment mode
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH delivered_orders AS (
    SELECT
        payment_mode,
        COUNT(*) AS total_delivered_orders,
        ROUND(SUM(net_revenue), 2) AS order_net_revenue
    FROM stg_orders
    WHERE dq_status = 'VALID' AND order_status = 'DELIVERED'
    GROUP BY payment_mode
),
gateway_succ AS (
    SELECT
        payment_mode,
        COUNT(*) AS total_successful_payments,
        ROUND(SUM(amount_paid), 2) AS gateway_collected_amount
    FROM stg_payments
    WHERE payment_status = 'SUCCESS' AND dq_status != 'QUARANTINE'
    GROUP BY payment_mode
),
mismatches AS (
    SELECT
        payment_mode,
        COUNT(*) AS unreconciled_count,
        ROUND(SUM(unreconciled_discrepancy_amount), 2) AS total_unreconciled_amount
    FROM mart_payment_reconciliation_details
    GROUP BY payment_mode
)
SELECT
    COALESCE(d.payment_mode, g.payment_mode, m.payment_mode) AS payment_mode,
    COALESCE(d.total_delivered_orders, 0) AS delivered_orders,
    COALESCE(d.order_net_revenue, 0.0) AS order_net_revenue,
    COALESCE(g.total_successful_payments, 0) AS gateway_successful_payments,
    COALESCE(g.gateway_collected_amount, 0.0) AS gateway_collected_amount,
    COALESCE(m.unreconciled_count, 0) AS unreconciled_orders_count,
    COALESCE(m.total_unreconciled_amount, 0.0) AS total_unreconciled_amount
FROM delivered_orders d
FULL OUTER JOIN gateway_succ g ON d.payment_mode = g.payment_mode
FULL OUTER JOIN mismatches m ON COALESCE(d.payment_mode, g.payment_mode) = m.payment_mode
ORDER BY order_net_revenue DESC;
