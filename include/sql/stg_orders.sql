-- include/sql/stg_orders.sql
CREATE OR REPLACE TABLE stg_orders AS
WITH latest_raw AS (
    SELECT *
    FROM raw_orders
    WHERE _source_file = 'orders.csv'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _source_file, _row_id ORDER BY _loaded_at DESC) = 1
),
ranked AS (
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
        ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY _row_id) AS row_num
    FROM latest_raw
),
valid_masters AS (
    SELECT DISTINCT store_id FROM stg_stores WHERE dq_status = 'PASSED'
),
valid_prods AS (
    SELECT DISTINCT product_id FROM stg_products WHERE dq_status = 'PASSED'
),
valid_custs AS (
    SELECT DISTINCT customer_id FROM stg_customers WHERE dq_status = 'PASSED'
),
validated AS (
    SELECT
        r.*,
        ROUND(r.quantity * r.unit_price * (1.0 - r.discount_pct / 100.0), 2) AS expected_amount,
        (s.store_id IS NOT NULL) AS store_exists,
        (p.product_id IS NOT NULL) AS prod_exists,
        (c.customer_id IS NOT NULL) AS cust_exists
    FROM ranked r
    LEFT JOIN valid_masters s ON r.store_id = s.store_id
    LEFT JOIN valid_prods p ON r.product_id = p.product_id
    LEFT JOIN valid_custs c ON r.customer_id = c.customer_id
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
    CASE
        WHEN row_num > 1 
          OR customer_id = '' 
          OR order_ts IS NULL 
          OR (order_status = 'DELIVERED' AND delivery_minutes IS NULL)
          OR order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')
          OR payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
          OR order_ts > TIMESTAMP '2026-10-01 23:59:59'
          OR quantity <= 0
          OR discount_pct < 0 OR discount_pct > 100
          OR ABS(order_amount - expected_amount) > 0.02
          OR (order_status = 'DELIVERED' AND (delivery_minutes <= 0 OR delivery_minutes >= 120))
          OR NOT store_exists
          OR NOT prod_exists
          OR NOT cust_exists
        THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CASE
        WHEN row_num > 1 THEN 'uniqueness:duplicate_order_id'
        WHEN customer_id = '' THEN 'completeness:null_customer_id'
        WHEN order_ts IS NULL THEN 'completeness:null_or_invalid_order_ts'
        WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'completeness:missing_delivery_minutes'
        WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'validity:invalid_order_status'
        WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'validity:invalid_payment_mode'
        WHEN order_ts > TIMESTAMP '2026-10-01 23:59:59' THEN 'timeliness:future_order_ts'
        WHEN quantity <= 0 THEN 'accuracy:quantity_le_zero'
        WHEN discount_pct < 0 OR discount_pct > 100 THEN 'validity:discount_pct_out_of_range'
        WHEN ABS(order_amount - expected_amount) > 0.02 THEN 'accuracy:order_amount_mismatch'
        WHEN order_status = 'DELIVERED' AND (delivery_minutes <= 0 OR delivery_minutes >= 120) THEN 'accuracy:delivery_minutes_out_of_range'
        WHEN NOT store_exists THEN 'consistency:unknown_store_id'
        WHEN NOT prod_exists THEN 'consistency:unknown_product_id'
        WHEN NOT cust_exists THEN 'consistency:unknown_customer_id'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _run_id
FROM validated;
