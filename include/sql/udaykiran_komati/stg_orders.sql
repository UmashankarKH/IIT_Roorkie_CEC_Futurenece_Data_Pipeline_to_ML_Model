CREATE OR REPLACE TABLE stg_orders AS
WITH latest AS (
    SELECT *,
           row_number() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_orders
),
std AS (
    SELECT
        order_id,
        order_ts,
        customer_id,
        store_id,
        product_id,
        try_cast(quantity AS DOUBLE) AS quantity,
        try_cast(unit_price AS DOUBLE) AS unit_price,
        try_cast(discount_pct AS DOUBLE) AS discount_pct,
        try_cast(order_amount AS DOUBLE) AS order_amount,
        CASE lower(replace(trim(payment_mode), ' ', ''))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'wallet' THEN 'Wallet'
            WHEN 'cashondelivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'cashh' THEN 'Cash on Delivery'
            ELSE trim(payment_mode)
        END AS payment_mode,
        upper(trim(order_status)) AS order_status,
        trim(channel) AS channel,
        try_cast(delivery_minutes AS DOUBLE) AS delivery_minutes,
        _source_file
    FROM latest
    WHERE rn = 1
)
SELECT
    order_id,
    order_ts,
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
    round(quantity * unit_price * (1 - discount_pct / 100.0), 2) AS expected_amount,
    CASE
        WHEN customer_id IS NULL OR trim(customer_id) = '' THEN 'FAIL'
        WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'FAIL'
        WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'FAIL'
        WHEN quantity IS NULL OR quantity <= 0 THEN 'FAIL'
        WHEN discount_pct < 0 OR discount_pct > 100 THEN 'FAIL'
        WHEN abs(order_amount - round(quantity * unit_price * (1 - discount_pct / 100.0), 2)) > 0.02 THEN 'FAIL'
        WHEN product_id NOT IN (SELECT product_id FROM stg_products) THEN 'FAIL'
        WHEN store_id NOT IN (SELECT store_id FROM stg_stores) THEN 'FAIL'
        WHEN customer_id NOT IN (SELECT customer_id FROM stg_customers) THEN 'FAIL'
        WHEN try_cast(order_ts AS TIMESTAMP) IS NULL THEN 'FAIL'
        WHEN try_cast(order_ts AS TIMESTAMP) > TIMESTAMP '2026-09-30 23:59:59'
             AND coalesce(_source_file, '') NOT LIKE '%increment%' THEN 'FAIL'
        WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'FAIL'
        WHEN order_status = 'DELIVERED' AND delivery_minutes > 120 THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    NULLIF(
        concat(
            CASE WHEN customer_id IS NULL OR trim(customer_id) = '' THEN 'missing customer_id;' ELSE '' END,
            CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'invalid order_status;' ELSE '' END,
            CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'invalid payment_mode;' ELSE '' END,
            CASE WHEN quantity IS NULL OR quantity <= 0 THEN 'quantity must be > 0;' ELSE '' END,
            CASE WHEN abs(order_amount - round(quantity * unit_price * (1 - discount_pct / 100.0), 2)) > 0.02 THEN 'order_amount mismatch;' ELSE '' END,
            CASE WHEN product_id NOT IN (SELECT product_id FROM stg_products) THEN 'unknown product_id;' ELSE '' END,
            CASE WHEN store_id NOT IN (SELECT store_id FROM stg_stores) THEN 'unknown store_id;' ELSE '' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes > 120 THEN 'delivery_minutes above 120;' ELSE '' END
        ),
        ''
    ) AS failure_reason
FROM std;
