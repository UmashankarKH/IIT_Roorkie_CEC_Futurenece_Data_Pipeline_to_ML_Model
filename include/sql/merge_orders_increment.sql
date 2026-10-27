-- include/sql/merge_orders_increment.sql
-- Incremental upsert of orders_2026-10-01_increment into stg_orders
-- 10 returned orders are updated (overwritten with latest status), 150 new orders are inserted

CREATE OR REPLACE TABLE stg_orders_increment_stg AS
WITH ranked AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
        TRIM(COALESCE(customer_id, '')) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(product_id) AS product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        CASE
            WHEN LOWER(TRIM(payment_mode)) IN ('upi') THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) IN ('card') THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) IN ('cash on delivery', 'cod') THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) IN ('wallet') THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        CASE
            WHEN UPPER(TRIM(order_status)) = 'DELIVERED' THEN 'DELIVERED'
            WHEN UPPER(TRIM(order_status)) = 'CANCELLED' THEN 'CANCELLED'
            WHEN UPPER(TRIM(order_status)) = 'RETURNED' THEN 'RETURNED'
            ELSE TRIM(order_status)
        END AS order_status,
        TRIM(channel) AS channel,
        TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY _loaded_at DESC) AS row_num
    FROM raw_orders_increment
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY _loaded_at DESC) = 1
),
validated AS (
    SELECT
        r.*,
        ROUND(r.quantity * r.unit_price * (1.0 - r.discount_pct / 100.0), 2) AS expected_amount,
        (s.store_id IS NOT NULL) AS store_exists,
        (p.product_id IS NOT NULL AND p.dq_status = 'PASSED') AS prod_exists,
        (c.customer_id IS NOT NULL) AS cust_exists
    FROM ranked r
    LEFT JOIN stg_stores s ON r.store_id = s.store_id
    LEFT JOIN stg_products p ON r.product_id = p.product_id
    LEFT JOIN stg_customers c ON r.customer_id = c.customer_id
)
SELECT
    order_id,
    order_ts,
    CAST(order_ts AS DATE) AS order_date,
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
        WHEN order_status = 'DELIVERED' AND delivery_minutes <= 15 THEN TRUE
        WHEN order_status = 'DELIVERED' THEN FALSE
        ELSE NULL
    END AS within_15_min,
    'PASSED' AS dq_status,
    NULL AS failure_reason,
    _loaded_at,
    _run_id
FROM validated;

-- Delete older matching rows to apply status updates idempotently
DELETE FROM stg_orders
WHERE order_id IN (SELECT order_id FROM stg_orders_increment_stg);

-- Insert all updated and new rows
INSERT INTO stg_orders
SELECT * FROM stg_orders_increment_stg;
