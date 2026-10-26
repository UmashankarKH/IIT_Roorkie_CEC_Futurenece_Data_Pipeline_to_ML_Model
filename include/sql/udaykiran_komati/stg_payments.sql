CREATE OR REPLACE TABLE stg_payments AS
WITH latest AS (
    SELECT *,
           row_number() OVER (PARTITION BY payment_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_payments
),
std AS (
    SELECT
        payment_id,
        order_id,
        payment_ts,
        CASE lower(replace(trim(payment_mode), ' ', ''))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'wallet' THEN 'Wallet'
            WHEN 'cashondelivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'cashh' THEN 'Cash on Delivery'
            ELSE trim(payment_mode)
        END AS payment_mode,
        try_cast(amount_paid AS DOUBLE) AS amount_paid,
        upper(trim(payment_status)) AS payment_status,
        gateway_ref,
        rn
    FROM latest
    WHERE rn = 1
)
SELECT
    payment_id,
    order_id,
    payment_ts,
    payment_mode,
    amount_paid,
    payment_status,
    gateway_ref,
    CASE
        WHEN payment_id IS NULL OR trim(payment_id) = '' THEN 'FAIL'
        WHEN order_id NOT IN (SELECT order_id FROM stg_orders) THEN 'FAIL'
        WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN order_id NOT IN (SELECT order_id FROM stg_orders) THEN 'unknown order_id'
        WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'invalid payment_mode'
        ELSE NULL
    END AS failure_reason
FROM std s;
