-- include/sql/nammamart/stg_orders.sql
-- Staging layer for orders
-- Casts types, normalises values, applies DQ rules, adds dq_status & failure_reason

CREATE OR REPLACE TABLE stg_orders AS
WITH base AS (
    SELECT
        order_id,
        CAST(order_ts AS TIMESTAMP)                          AS order_ts,
        CAST(order_ts AS DATE)                               AS order_date,
        customer_id,
        store_id,
        product_id,
        CAST(quantity       AS DOUBLE)                       AS quantity,
        CAST(unit_price     AS DOUBLE)                       AS unit_price,
        COALESCE(CAST(discount_pct AS DOUBLE), 0)            AS discount_pct,
        CAST(order_amount   AS DOUBLE)                       AS order_amount,
        TRIM(payment_mode)                                   AS payment_mode_raw,
        CASE
            WHEN LOWER(TRIM(payment_mode)) = 'upi'              THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) = 'card'             THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) = 'cash on delivery' THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) = 'wallet'           THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END                                                  AS payment_mode,
        UPPER(TRIM(order_status))                            AS order_status,
        TRIM(channel)                                        AS channel,
        CAST(delivery_minutes AS DOUBLE)                     AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id
    FROM raw_orders
),
with_derived AS (
    SELECT *,
        ROUND(quantity * unit_price * (1 - discount_pct / 100.0), 2) AS net_revenue_calc,
        ROUND(quantity * unit_price * (1 - discount_pct / 100.0), 2) AS net_revenue,
        CASE WHEN COALESCE(delivery_minutes, 999) <= 15 THEN 1 ELSE 0 END AS within_15_min,
        -- DQ checks
        CASE
            WHEN customer_id IS NULL                                       THEN 'customer_id is null'
            WHEN order_status NOT IN ('DELIVERED','CANCELLED','RETURNED')  THEN 'invalid order_status'
            WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'invalid payment_mode'
            WHEN quantity IS NULL OR quantity <= 0                         THEN 'quantity must be > 0'
            WHEN order_ts > CURRENT_TIMESTAMP                              THEN 'order_ts is in the future'
            WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL   THEN 'DELIVERED missing delivery_minutes'
            WHEN ABS(order_amount - ROUND(quantity * unit_price * (1 - discount_pct/100.0), 2)) > 0.02
                                                                           THEN 'order_amount formula mismatch'
            ELSE NULL
        END AS failure_reason
    FROM base
)
SELECT *,
    CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status
FROM with_derived;
