CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    s.zone,
    s.store_id,
    s.store_name,
    SUM(o.net_revenue) AS net_revenue,
    COUNT(DISTINCT o.order_id) AS delivered_orders
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY
    CAST(o.order_ts AS DATE),
    s.zone,
    s.store_id,
    s.store_name;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    COALESCE(p.category, 'UNKNOWN') AS category,
    SUM(o.net_revenue) AS total_revenue,
    SUM(o.gross_margin) AS gross_margin,
    COUNT(DISTINCT o.order_id) AS delivered_orders
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY COALESCE(p.category, 'UNKNOWN');

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    AVG(o.delivery_minutes) AS avg_delivery_minutes,
    100.0 * SUM(
        CASE WHEN o.delivery_minutes <= 15 THEN 1 ELSE 0 END
    ) / NULLIF(COUNT(*), 0) AS pct_delivered_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
  AND o.delivery_minutes IS NOT NULL
GROUP BY s.store_id, s.store_name, s.zone;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH successful_payments AS (
    SELECT *
    FROM stg_payments
    WHERE payment_status = 'SUCCESS'
      AND dq_status = 'PASS'
),
payment_checks AS (
    SELECT
        p.payment_id,
        p.order_id,
        p.payment_mode,
        p.amount_paid,
        o.calculated_amount,
        CASE
            WHEN o.order_id IS NULL THEN 'UNMATCHED_ORDER'
            WHEN ABS(p.amount_paid - o.calculated_amount) > 0.02
                THEN 'AMOUNT_MISMATCH'
            ELSE 'RECONCILED'
        END AS reconciliation_status,
        CASE
            WHEN o.order_id IS NULL THEN COALESCE(p.amount_paid, 0)
            WHEN ABS(p.amount_paid - o.calculated_amount) > 0.02
                THEN ABS(p.amount_paid - o.calculated_amount)
            ELSE 0
        END AS unreconciled_amount
    FROM successful_payments p
    LEFT JOIN stg_orders o
        ON p.order_id = o.order_id
       AND o.dq_status = 'PASS'
),
missing_payments AS (
    SELECT
        CAST(NULL AS VARCHAR) AS payment_id,
        o.order_id,
        o.payment_mode,
        CAST(0 AS DECIMAL(12, 2)) AS amount_paid,
        o.calculated_amount,
        'MISSING_SUCCESSFUL_PAYMENT' AS reconciliation_status,
        o.calculated_amount AS unreconciled_amount
    FROM stg_orders o
    LEFT JOIN successful_payments p ON o.order_id = p.order_id
    WHERE o.dq_status = 'PASS'
      AND o.order_status = 'DELIVERED'
      AND p.payment_id IS NULL
)
SELECT * FROM payment_checks
UNION ALL
SELECT * FROM missing_payments;

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.phone,
    c.loyalty_tier,
    SUM(o.net_revenue) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY
    c.customer_id,
    c.full_name,
    c.email,
    c.phone,
    c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
