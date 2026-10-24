-- Check-level quality scorecard for the ELT pipeline, written to dq_results.
-- Each row is one rule: check_name, dimension, severity, rows_checked and
-- rows_failed. Check names match data_quality.py so the ETL and ELT
-- scorecards can be compared rule by rule. Requires the restricted raw-PII
-- file to be attached as ``pii`` (nammamart_elt.execute_sql_layers does this).
WITH current_orders AS (
    -- The latest load of every order_id, keeping duplicates inside that load:
    -- the same rows the ETL validates after its increment upsert.
    SELECT *
    FROM raw_orders
    QUALIFY _loaded_at = max(_loaded_at) OVER (PARTITION BY order_id)
), o AS (
    SELECT
        order_id,
        customer_id,
        store_id,
        product_id,
        try_cast(order_ts AS TIMESTAMP) AS order_ts,
        try_cast(quantity AS INTEGER) AS quantity,
        try_cast(unit_price AS DECIMAL(12, 2)) AS unit_price,
        try_cast(discount_pct AS DECIMAL(6, 2)) AS discount_pct,
        try_cast(order_amount AS DECIMAL(12, 2)) AS order_amount,
        CASE
            WHEN lower(trim(payment_mode)) = 'upi' THEN 'UPI'
            WHEN lower(trim(payment_mode)) = 'card' THEN 'Card'
            WHEN lower(trim(payment_mode)) IN ('cod', 'cashh', 'cash on delivery') THEN 'Cash on Delivery'
            WHEN lower(trim(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE trim(payment_mode)
        END AS payment_mode,
        upper(trim(order_status)) AS order_status,
        try_cast(delivery_minutes AS INTEGER) AS delivery_minutes,
        count(*) OVER (PARTITION BY order_id) AS id_count
    FROM current_orders
), cutoff AS (
    -- End of the latest export day (data_quality.DATA_AS_OF).
    SELECT least(CAST(current_timestamp AS TIMESTAMP), TIMESTAMP '2026-10-02 00:00:00') AS ts
), p AS (
    SELECT
        payment_id,
        order_id,
        gateway_ref,
        try_cast(amount_paid AS DECIMAL(12, 2)) AS amount_paid,
        upper(trim(payment_status)) AS payment_status,
        count(*) OVER (PARTITION BY payment_id) AS id_count
    FROM raw_payments
), c AS (
    SELECT *, count(*) OVER (PARTITION BY customer_id) AS id_count FROM pii.raw_customers
), pr AS (
    SELECT
        product_id,
        category,
        try_cast(mrp AS DECIMAL(12, 2)) AS mrp,
        try_cast(cost_price AS DECIMAL(12, 2)) AS cost_price,
        count(*) OVER (PARTITION BY product_id) AS id_count
    FROM raw_products
), s AS (
    SELECT
        store_id,
        zone,
        pincode,
        try_cast(capacity_orders_per_day AS INTEGER) AS capacity,
        count(*) OVER (PARTITION BY store_id) AS id_count
    FROM raw_stores
)
-- Orders
SELECT 'orders.csv:customer_id:Missing customer_id' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity,
       count(*) AS rows_checked, count(*) FILTER (WHERE customer_id = '') AS rows_failed FROM o
UNION ALL SELECT 'orders.csv:order_id:Duplicate order_id', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE id_count > 1) FROM o
UNION ALL SELECT 'orders.csv:order_status:Invalid order status', 'Validity', 'Critical', count(*),
       count(*) FILTER (WHERE coalesce(order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED'), true)) FROM o
UNION ALL SELECT 'orders.csv:payment_mode:Invalid payment mode', 'Validity', 'Critical', count(*),
       count(*) FILTER (WHERE coalesce(payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), true)) FROM o
UNION ALL SELECT 'orders.csv:quantity:Quantity is not positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE quantity <= 0) FROM o
UNION ALL SELECT 'orders.csv:order_amount:Amount does not match pricing formula', 'Accuracy', 'Critical', count(*),
       count(*) FILTER (WHERE abs(order_amount - round(quantity * unit_price * (1 - discount_pct / 100), 2)) > 0.01) FROM o
UNION ALL SELECT 'orders.csv:delivery_minutes:Delivered order has no delivery time', 'Completeness', 'Critical', count(*),
       count(*) FILTER (WHERE order_status = 'DELIVERED' AND delivery_minutes IS NULL) FROM o
UNION ALL SELECT 'orders.csv:delivery_minutes:Delivery time exceeds 120 minutes', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE delivery_minutes > 120) FROM o
UNION ALL SELECT 'orders.csv:discount_pct:Discount outside 0-100', 'Validity', 'Critical', count(*),
       count(*) FILTER (WHERE NOT coalesce(discount_pct BETWEEN 0 AND 100, false)) FROM o
UNION ALL SELECT 'orders.csv:order_ts:order_ts is not a valid timestamp', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE order_ts IS NULL) FROM o
UNION ALL SELECT 'orders.csv:order_ts:Future-dated order', 'Timeliness', 'Critical', count(*),
       count(*) FILTER (WHERE order_ts > (SELECT ts FROM cutoff)) FROM o
UNION ALL SELECT 'orders.csv:order_ts:Latest order older than 1 day', 'Timeliness', 'Warning', count(*),
       CASE WHEN coalesce(max(order_ts) FILTER (WHERE order_ts <= (SELECT ts FROM cutoff)) >= CAST(current_timestamp AS TIMESTAMP) - INTERVAL 1 DAY, false)
            THEN 0 ELSE 1 END FROM o
UNION ALL SELECT 'orders.csv:customer_id:Unknown customer_id', 'Consistency', 'Critical', count(*),
       count(*) FILTER (WHERE customer_id NOT IN (SELECT customer_id FROM pii.raw_customers)) FROM o
UNION ALL SELECT 'orders.csv:product_id:Unknown product_id', 'Consistency', 'Critical', count(*),
       count(*) FILTER (WHERE product_id NOT IN (SELECT product_id FROM raw_products)) FROM o
UNION ALL SELECT 'orders.csv:store_id:Unknown store_id', 'Consistency', 'Critical', count(*),
       count(*) FILTER (WHERE store_id NOT IN (SELECT store_id FROM raw_stores)) FROM o
-- Payments
UNION ALL SELECT 'payments.csv:payment_id:Duplicate payment_id', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE id_count > 1) FROM p
UNION ALL SELECT 'payments.csv:gateway_ref:Missing gateway reference', 'Completeness', 'Warning', count(*), count(*) FILTER (WHERE gateway_ref = '') FROM p
UNION ALL SELECT 'payments.csv:amount_paid:Payment amount is not positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE amount_paid <= 0) FROM p
UNION ALL SELECT 'payments.csv:order_id:Unknown order_id', 'Consistency', 'Critical', count(*),
       count(*) FILTER (WHERE order_id NOT IN (SELECT order_id FROM o)) FROM p
UNION ALL SELECT 'payments.csv:amount_paid:Successful payment does not equal order amount', 'Consistency', 'Critical', count(*),
       count(*) FILTER (WHERE p.payment_status = 'SUCCESS' AND abs(p.amount_paid - o.order_amount) > 0.01)
       FROM p LEFT JOIN o ON o.order_id = p.order_id
-- Customers (counts only; raw values never leave the restricted file)
UNION ALL SELECT 'customers.csv:customer_id:Duplicate customer_id', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE id_count > 1) FROM c
UNION ALL SELECT 'customers.csv:email:Invalid email format', 'Validity', 'Warning', count(*),
       count(*) FILTER (WHERE NOT regexp_full_match(email, '[^@\s]+@[^@\s]+\.[^@\s]+')) FROM c
UNION ALL SELECT 'customers.csv:phone:Phone is not a 10-digit number', 'Validity', 'Warning', count(*),
       count(*) FILTER (WHERE NOT regexp_full_match(phone, '\d{10}')) FROM c
UNION ALL SELECT 'customers.csv:loyalty_tier:Loyalty tier casing is not standard', 'Consistency', 'Warning', count(*),
       count(*) FILTER (WHERE loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum')) FROM c
UNION ALL SELECT 'customers.csv:signup_date:Future signup_date', 'Timeliness', 'Warning', count(*),
       count(*) FILTER (WHERE try_cast(signup_date AS TIMESTAMP) > (SELECT ts FROM cutoff)) FROM c
UNION ALL SELECT 'customers.csv:pincode:Pincode is not six digits', 'Validity', 'Warning', count(*),
       count(*) FILTER (WHERE NOT regexp_full_match(pincode, '\d{6}')) FROM c
-- Products
UNION ALL SELECT 'products.csv:product_id:Duplicate product_id', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE id_count > 1) FROM pr
UNION ALL SELECT 'products.csv:category:Missing category', 'Completeness', 'Critical', count(*), count(*) FILTER (WHERE category = '') FROM pr
UNION ALL SELECT 'products.csv:mrp:MRP is not positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE mrp <= 0) FROM pr
UNION ALL SELECT 'products.csv:cost_price:Cost price exceeds MRP', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE cost_price > mrp) FROM pr
-- Stores
UNION ALL SELECT 'stores.csv:store_id:Duplicate store_id', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE id_count > 1) FROM s
UNION ALL SELECT 'stores.csv:zone:Missing zone', 'Completeness', 'Critical', count(*), count(*) FILTER (WHERE zone = '') FROM s
UNION ALL SELECT 'stores.csv:pincode:Pincode is not six digits', 'Validity', 'Warning', count(*),
       count(*) FILTER (WHERE NOT regexp_full_match(pincode, '\d{6}')) FROM s
UNION ALL SELECT 'stores.csv:capacity_orders_per_day:Store capacity is not positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE capacity <= 0) FROM s
UNION ALL SELECT 'stores.csv:capacity_orders_per_day:Daily orders exceed store capacity', 'Accuracy', 'Warning', count(*),
       count(*) FILTER (WHERE d.daily_orders > s.capacity)
       FROM (SELECT store_id, CAST(order_ts AS DATE) AS order_day, count(*) AS daily_orders
             FROM o WHERE order_ts IS NOT NULL GROUP BY 1, 2) d
       JOIN s ON s.store_id = d.store_id
       WHERE s.capacity > 0
-- Next-day increment (rows appended by the incremental load; 0 before it runs)
UNION ALL SELECT 'orders_2026-10-01_increment.csv:order_amount:Amount does not match pricing formula', 'Accuracy', 'Critical', count(*),
       count(*) FILTER (WHERE abs(try_cast(order_amount AS DECIMAL(12, 2))
           - round(try_cast(quantity AS INTEGER) * try_cast(unit_price AS DECIMAL(12, 2)) * (1 - try_cast(discount_pct AS DECIMAL(6, 2)) / 100), 2)) > 0.01)
       FROM raw_orders WHERE _source_file = 'orders_2026-10-01_increment.csv'
UNION ALL SELECT 'orders_2026-10-01_increment.csv:order_status:Invalid order status', 'Validity', 'Critical', count(*),
       count(*) FILTER (WHERE coalesce(upper(trim(order_status)) NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED'), true))
       FROM raw_orders WHERE _source_file = 'orders_2026-10-01_increment.csv'
