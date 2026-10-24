-- include/sql/stg_orders.sql
-- Staging layer for orders: deduplication, standardisation, DQ validation, and metric derivation

CREATE OR REPLACE TABLE stg_orders AS
WITH raw_typed AS (
    SELECT
        order_id,
        order_ts AS order_ts_raw,
        TRY_CAST(order_ts AS TIMESTAMP) AS order_ts_parsed,
        customer_id,
        store_id,
        product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
        TRY_CAST(discount_pct AS DECIMAL(10, 2)) AS discount_pct,
        TRY_CAST(order_amount AS DECIMAL(10, 2)) AS order_amount,
        CASE 
            WHEN UPPER(TRIM(payment_mode)) IN ('UPI', 'UPI ') THEN 'UPI'
            WHEN UPPER(TRIM(payment_mode)) IN ('CARD', 'CREDIT CARD', 'DEBIT CARD') THEN 'Card'
            WHEN UPPER(TRIM(payment_mode)) IN ('CASH ON DELIVERY', 'COD') THEN 'Cash on Delivery'
            WHEN UPPER(TRIM(payment_mode)) = 'WALLET' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS clean_payment_mode,
        UPPER(TRIM(order_status)) AS clean_order_status,
        channel,
        TRY_CAST(delivery_minutes AS DECIMAL(10, 2)) AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_orders
),
evaluated AS (
    SELECT
        r.order_id,
        r.order_ts_parsed AS order_ts,
        CAST(r.order_ts_parsed AS DATE) AS order_date,
        r.customer_id,
        r.store_id,
        r.product_id,
        r.quantity,
        r.unit_price,
        r.discount_pct,
        r.order_amount,
        r.clean_payment_mode AS payment_mode,
        r.clean_order_status AS order_status,
        r.channel,
        r.delivery_minutes,
        r._loaded_at,
        r._source_file,
        r._run_id,
        r.rn,
        CASE
            WHEN r.rn > 1 THEN 'DUPLICATE_ORDER_ID'
            WHEN r.customer_id IS NULL THEN 'NULL_CUSTOMER_ID'
            WHEN r.order_ts_raw IS NULL THEN 'NULL_ORDER_TS'
            WHEN r.order_ts_raw IS NOT NULL AND r.order_ts_parsed IS NULL THEN 'UNPARSEABLE_ORDER_TS'
            WHEN r.order_ts_parsed > TIMESTAMP '2026-10-02 23:59:59' THEN 'FUTURE_ORDER_TIMESTAMP'
            WHEN r.clean_order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'INVALID_ORDER_STATUS'
            WHEN r.clean_payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'INVALID_PAYMENT_MODE'
            WHEN r.discount_pct < 0 OR r.discount_pct > 100 THEN 'INVALID_DISCOUNT_PCT'
            WHEN r.quantity <= 0 THEN 'NON_POSITIVE_QUANTITY'
            WHEN r.clean_order_status = 'DELIVERED' AND r.delivery_minutes IS NULL THEN 'DELIVERED_NULL_DELIVERY_MINUTES'
            WHEN r.clean_order_status = 'DELIVERED' AND (r.delivery_minutes < 0 OR r.delivery_minutes > 120) THEN 'IMPROBABLE_DELIVERY_MINUTES'
            WHEN ABS(r.order_amount - ROUND(r.quantity * r.unit_price * (1.0 - r.discount_pct / 100.0), 2)) > 0.05 THEN 'ORDER_AMOUNT_MISMATCH'
            WHEN r.customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE dq_status != 'QUARANTINE') THEN 'UNKNOWN_OR_QUARANTINED_CUSTOMER'
            WHEN r.product_id NOT IN (SELECT product_id FROM stg_products WHERE dq_status != 'QUARANTINE') THEN 'UNKNOWN_OR_QUARANTINED_PRODUCT'
            WHEN r.store_id NOT IN (SELECT store_id FROM stg_stores WHERE dq_status != 'QUARANTINE') THEN 'UNKNOWN_OR_QUARANTINED_STORE'
            ELSE NULL
        END AS failure_reason,
        p.cost_price
    FROM raw_typed r
    LEFT JOIN (SELECT DISTINCT product_id, cost_price FROM stg_products WHERE dq_status != 'QUARANTINE') p 
        ON r.product_id = p.product_id
)
SELECT
    order_id,
    order_ts,
    order_date,
    customer_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    order_amount,
    payment_mode,
    order_status,
    channel,
    delivery_minutes,
    CASE 
        WHEN failure_reason IS NULL AND order_status = 'DELIVERED' THEN order_amount
        ELSE 0.00
    END AS net_revenue,
    CASE
        WHEN failure_reason IS NULL AND order_status = 'DELIVERED' THEN order_amount - (quantity * COALESCE(cost_price, 0))
        ELSE 0.00
    END AS gross_margin,
    CASE
        WHEN failure_reason IS NULL AND order_status = 'DELIVERED' AND delivery_minutes <= 15 THEN TRUE
        ELSE FALSE
    END AS within_15_min,
    _loaded_at,
    _source_file,
    _run_id,
    CASE 
        WHEN failure_reason IS NOT NULL THEN 'QUARANTINE'
        ELSE 'VALID'
    END AS dq_status,
    failure_reason
FROM evaluated;
