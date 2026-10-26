CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(order_ts AS TIMESTAMP) AS order_ts,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(store_id AS VARCHAR) AS store_id,
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price AS DOUBLE) AS unit_price,
    CAST(discount_pct AS DOUBLE) AS discount_pct,
    CAST(order_amount AS DOUBLE) AS order_amount,
    CASE
        WHEN payment_mode IS NULL THEN 'UNKNOWN'
        WHEN LOWER(TRIM(payment_mode)) IN ('upi', 'upi ', 'upi') THEN 'UPI'
        ELSE TRIM(payment_mode)
    END AS payment_mode,
    UPPER(TRIM(order_status)) AS order_status,
    TRIM(channel) AS channel,
    CAST(delivery_minutes AS INTEGER) AS delivery_minutes,
    CASE WHEN UPPER(TRIM(order_status)) IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'pass' ELSE 'fail' END AS dq_status,
    CASE WHEN UPPER(TRIM(order_status)) IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN NULL ELSE 'invalid order_status' END AS failure_reason
FROM raw_orders;

CREATE TABLE IF NOT EXISTS stg_payments AS
SELECT
    CAST(payment_id AS VARCHAR) AS payment_id,
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(payment_ts AS TIMESTAMP) AS payment_ts,
    CASE
        WHEN payment_mode IS NULL THEN 'UNKNOWN'
        WHEN LOWER(TRIM(payment_mode)) IN ('upi', 'upi ', 'upi') THEN 'UPI'
        ELSE TRIM(payment_mode)
    END AS payment_mode,
    CAST(amount_paid AS DOUBLE) AS amount_paid,
    UPPER(TRIM(payment_status)) AS payment_status,
    TRIM(gateway_ref) AS gateway_ref
FROM raw_payments;

CREATE TABLE IF NOT EXISTS stg_customers AS
SELECT
    CAST(customer_id AS VARCHAR) AS customer_id,
    TRIM(full_name) AS full_name,
    CASE WHEN email IS NULL THEN NULL ELSE lower(TRIM(email)) END AS email,
    CASE WHEN phone IS NULL THEN NULL ELSE '98******21' END AS phone,
    TRIM(city) AS city,
    TRIM(area) AS area,
    CAST(pincode AS VARCHAR) AS pincode,
    CAST(signup_date AS DATE) AS signup_date,
    UPPER(TRIM(loyalty_tier)) AS loyalty_tier,
    UPPER(TRIM(is_active)) AS is_active
FROM raw_customers;

CREATE TABLE IF NOT EXISTS stg_products AS
SELECT
    CAST(product_id AS VARCHAR) AS product_id,
    TRIM(product_name) AS product_name,
    TRIM(category) AS category,
    TRIM(sub_category) AS sub_category,
    TRIM(brand) AS brand,
    CAST(mrp AS DOUBLE) AS mrp,
    CAST(cost_price AS DOUBLE) AS cost_price,
    TRIM(unit) AS unit,
    CAST(is_perishable AS BOOLEAN) AS is_perishable,
    CAST(launch_date AS DATE) AS launch_date
FROM raw_products;

CREATE TABLE IF NOT EXISTS stg_stores AS
SELECT
    CAST(store_id AS VARCHAR) AS store_id,
    TRIM(store_name) AS store_name,
    CASE WHEN zone IS NULL OR TRIM(zone) = '' THEN 'Unknown' ELSE TRIM(zone) END AS zone,
    TRIM(area) AS area,
    CAST(pincode AS VARCHAR) AS pincode,
    CAST(open_date AS DATE) AS open_date,
    TRIM(manager_name) AS manager_name,
    CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day
FROM raw_stores;
