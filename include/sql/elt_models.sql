CREATE OR REPLACE TABLE stg_stores AS
SELECT 
    store_id,
    TRIM(store_name) AS store_name,
    UPPER(TRIM(zone)) AS zone,
    TRIM(area) AS area,
    pincode,
    TRY_CAST(open_date AS DATE) AS open_date,
    TRIM(manager_name) AS manager_name,
    TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day
FROM raw_stores;

CREATE OR REPLACE TABLE stg_products AS
SELECT 
    product_id,
    TRIM(product_name) AS product_name,
    TRIM(category) AS category,
    TRIM(sub_category) AS sub_category,
    TRIM(brand) AS brand,
    TRY_CAST(mrp AS DOUBLE) AS mrp,
    TRY_CAST(cost_price AS DOUBLE) AS cost_price,
    TRIM(unit) AS unit,
    TRY_CAST(is_perishable AS BOOLEAN) AS is_perishable,
    TRY_CAST(launch_date AS DATE) AS launch_date
FROM raw_products;

CREATE OR REPLACE TABLE stg_customers AS
SELECT 
    customer_id,
    TRIM(full_name) AS full_name,
    CASE 
        WHEN email LIKE '%@%' THEN SUBSTR(email, 1, 2) || '***@' || SPLIT_PART(email, '@', 2)
        ELSE 'masked_email'
    END AS email_masked,
    CASE 
        WHEN LENGTH(TRIM(phone)) >= 10 THEN SUBSTR(TRIM(phone), 1, 2) || '******' || SUBSTR(TRIM(phone), -2)
        ELSE '**********'
    END AS phone_masked,
    TRIM(city) AS city,
    TRIM(area) AS area,
    pincode,
    TRY_CAST(signup_date AS DATE) AS signup_date,
    UPPER(TRIM(loyalty_tier)) AS loyalty_tier,
    TRY_CAST(is_active AS BOOLEAN) AS is_active
FROM raw_customers;

CREATE OR REPLACE TABLE stg_orders AS
SELECT 
    order_id,
    TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
    TRY_CAST(TRY_CAST(order_ts AS TIMESTAMP) AS DATE) AS order_date,
    customer_id,
    store_id,
    product_id,
    TRY_CAST(quantity AS INTEGER) AS quantity,
    TRY_CAST(unit_price AS DOUBLE) AS unit_price,
    TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
    TRY_CAST(order_amount AS DOUBLE) AS order_amount,
    UPPER(TRIM(payment_mode)) AS payment_mode,
    UPPER(TRIM(order_status)) AS order_status,
    TRIM(channel) AS channel,
    TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
    CASE 
        WHEN customer_id IS NULL OR store_id IS NULL OR product_id IS NULL THEN 'FAILED_REF_KEY'
        WHEN UPPER(TRIM(order_status)) NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'INVALID_STATUS'
        WHEN TRY_CAST(quantity AS INTEGER) <= 0 THEN 'INVALID_QUANTITY'
        WHEN TRY_CAST(order_ts AS TIMESTAMP) IS NULL THEN 'INVALID_TIMESTAMP'
        ELSE 'PASSED'
    END AS dq_status,
    CASE 
        WHEN TRY_CAST(delivery_minutes AS DOUBLE) <= 15 THEN 1 ELSE 0 
    END AS within_15_min
FROM raw_orders;

CREATE OR REPLACE TABLE stg_payments AS
SELECT 
    payment_id,
    order_id,
    TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
    UPPER(TRIM(payment_mode)) AS payment_mode,
    TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
    UPPER(TRIM(payment_status)) AS payment_status,
    TRIM(gateway_ref) AS gateway_ref
FROM raw_payments;

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT 
    o.order_date,
    s.zone,
    s.store_id,
    s.store_name,
    COUNT(o.order_id) AS total_delivered_orders,
    ROUND(SUM(o.order_amount), 2) AS daily_net_revenue,
    ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes,
    ROUND(100.0 * SUM(o.within_15_min) / COUNT(o.order_id), 2) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone, s.store_id, s.store_name;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT 
    p.category,
    COUNT(o.order_id) AS orders_count,
    ROUND(SUM(o.order_amount), 2) AS category_revenue,
    ROUND(SUM(o.order_amount - (o.quantity * p.cost_price)), 2) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY p.category;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT 
    o.order_id,
    o.order_amount,
    p.amount_paid,
    p.payment_status,
    CASE 
        WHEN p.payment_id IS NULL THEN 'MISSING_PAYMENT_RECORD'
        WHEN p.payment_status != 'SUCCESS' THEN 'PAYMENT_NOT_SUCCESS'
        WHEN ROUND(o.order_amount, 2) != ROUND(p.amount_paid, 2) THEN 'AMOUNT_MISMATCH'
        ELSE 'RECONCILED'
    END AS reconciliation_status
FROM stg_orders o
LEFT JOIN stg_payments p ON o.order_id = p.order_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED';

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT 
    c.customer_id,
    c.full_name,
    c.email_masked,
    c.loyalty_tier,
    ROUND(SUM(o.order_amount), 2) AS total_spend,
    COUNT(o.order_id) AS total_orders
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.email_masked, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
