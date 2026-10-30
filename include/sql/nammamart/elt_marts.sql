-- Rebuild the business-facing ELT marts from the current validated staging view.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    order_date,
    zone,
    store_id,
    store_name,
    COUNT(*) AS delivered_orders,
    ROUND(SUM(net_revenue), 2) AS net_revenue,
    ROUND(AVG(delivery_minutes), 2) AS avg_delivery_minutes,
    ROUND(100.0 * AVG(CASE WHEN delivery_minutes <= 15 THEN 1.0 ELSE 0.0 END), 2) AS pct_within_15_min
FROM stg_orders
WHERE dq_status IN ('PASS', 'WARNING')
  AND order_status = 'DELIVERED'
GROUP BY order_date, zone, store_id, store_name
ORDER BY order_date, zone, store_id;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    category,
    ROUND(SUM(net_revenue), 2) AS net_revenue,
    ROUND(SUM(quantity * cost_price), 2) AS cost_of_goods,
    ROUND(SUM(net_revenue - quantity * cost_price), 2) AS gross_margin
FROM stg_orders
WHERE dq_status IN ('PASS', 'WARNING')
  AND order_status = 'DELIVERED'
GROUP BY category
ORDER BY category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    store_id,
    store_name,
    zone,
    COUNT(*) AS delivered_orders,
    ROUND(AVG(delivery_minutes), 2) AS avg_delivery_minutes,
    ROUND(100.0 * AVG(CASE WHEN delivery_minutes <= 15 THEN 1.0 ELSE 0.0 END), 2) AS pct_within_15_min
FROM stg_orders
WHERE dq_status IN ('PASS', 'WARNING')
  AND order_status = 'DELIVERED'
  AND delivery_minutes IS NOT NULL
GROUP BY store_id, store_name, zone
ORDER BY store_id;

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT payment_mode,
       COUNT(*) AS delivered_orders,
       ROUND(SUM(net_revenue), 2) AS net_revenue
FROM stg_orders
WHERE dq_status IN ('PASS', 'WARNING')
  AND order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY payment_mode;

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT o.customer_id,
       c.loyalty_tier,
       COUNT(*) AS delivered_orders,
       ROUND(SUM(o.net_revenue), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status IN ('PASS', 'WARNING')
WHERE o.dq_status IN ('PASS', 'WARNING')
  AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.loyalty_tier
ORDER BY total_spend DESC, o.customer_id
LIMIT 10;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH payment_issues AS (
    SELECT
        p.order_id,
        p.payment_id,
        p.payment_mode AS payment_mode,
        o.payment_mode AS order_payment_mode,
        o.order_amount,
        p.amount_paid,
        CASE
            WHEN o.order_id IS NULL THEN 'payment references an unknown order'
            WHEN o.order_status = 'CANCELLED' THEN 'payment exists for a cancelled order'
            WHEN p.failure_reason LIKE '%payment_ts is in the future%' THEN 'successful payment timestamp is in the future'
            WHEN p.failure_reason LIKE '%payment_ts is missing or invalid%' THEN 'successful payment timestamp is invalid'
            WHEN ABS(COALESCE(p.amount_paid, 0) - COALESCE(o.order_amount, 0)) > 0.01 THEN 'successful payment amount differs from order_amount'
            WHEN p.payment_mode <> o.payment_mode THEN 'payment mode differs from order'
        END AS reason
    FROM stg_payments p
    LEFT JOIN stg_orders o ON o.order_id = p.order_id
    WHERE o.order_id IS NULL
       OR o.order_status = 'CANCELLED'
       OR (p.payment_status = 'SUCCESS' AND (
              p.failure_reason LIKE '%payment_ts is in the future%'
              OR p.failure_reason LIKE '%payment_ts is missing or invalid%'
              OR
              ABS(COALESCE(p.amount_paid, 0) - COALESCE(o.order_amount, 0)) > 0.01
              OR p.payment_mode <> o.payment_mode
          ))
), missing_success AS (
    SELECT
        o.order_id,
        CAST(NULL AS VARCHAR) AS payment_id,
        CAST(NULL AS VARCHAR) AS payment_mode,
        o.payment_mode AS order_payment_mode,
        o.order_amount,
        CAST(NULL AS DOUBLE) AS amount_paid,
        'delivered order has no successful matching payment' AS reason
    FROM stg_orders o
    WHERE o.dq_status IN ('PASS', 'WARNING')
      AND o.order_status = 'DELIVERED'
      AND NOT EXISTS (
          SELECT 1 FROM stg_payments p
          WHERE p.order_id = o.order_id
            AND p.payment_status = 'SUCCESS'
      )
), mismatches AS (
    SELECT * FROM payment_issues
    UNION ALL
    SELECT * FROM missing_success
)
SELECT order_id, payment_id, payment_mode, order_payment_mode,
       order_amount, amount_paid, reason,
       CASE WHEN reason = 'successful payment amount differs from order_amount'
                  AND order_amount IS NOT NULL AND amount_paid IS NOT NULL
            THEN ABS(order_amount - amount_paid)
            ELSE COALESCE(amount_paid, order_amount, 0) END AS reconciliation_amount,
       CURRENT_TIMESTAMP AS checked_at
FROM mismatches;
