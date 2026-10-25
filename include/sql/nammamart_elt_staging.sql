-- NammaMart ELT staging layer.
-- Raw data stays untouched; all casts, standardization, masking, deduplication,
-- and row-level quality status decisions happen in this SQL transformation.
--
-- Every staging table is built from the *current* snapshot of each source
-- file: the rows written by the most recent load of that file. The raw layer
-- stays append-only, so an older run (or an experiment mutation) can never
-- change the current state that the marts publish.

CREATE OR REPLACE TABLE stg_customers AS
WITH latest_load AS (
    SELECT MAX(_loaded_at) AS latest_loaded_at FROM raw_customers
), current_rows AS (
    SELECT r.* FROM raw_customers r
    JOIN latest_load l ON r._loaded_at = l.latest_loaded_at
), source_rows AS (
    SELECT *, COUNT(DISTINCT md5(concat_ws('|', customer_id, full_name, email, phone, city, area, pincode, signup_date, loyalty_tier, is_active))) OVER (PARTITION BY customer_id) AS source_duplicate_versions
    FROM current_rows
), ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY TRY_CAST(_loaded_at AS TIMESTAMP) DESC, _run_id DESC) AS record_rank
    FROM source_rows
), typed AS (
    SELECT
        NULLIF(TRIM(customer_id), '') AS customer_id,
        NULLIF(TRIM(full_name), '') AS full_name,
        CASE WHEN regexp_matches(TRIM(email), '^[^@\s]+@[^@\s]+\.[^@\s]+$')
             THEN LEFT(TRIM(email), 2) || '***@' || split_part(TRIM(email), '@', 2)
             ELSE '***' END AS email,
        CASE WHEN LENGTH(regexp_replace(TRIM(phone), '[^0-9]', '', 'g')) >= 2
             THEN repeat('*', GREATEST(LENGTH(regexp_replace(TRIM(phone), '[^0-9]', '', 'g')) - 2, 0)) || RIGHT(regexp_replace(TRIM(phone), '[^0-9]', '', 'g'), 2)
             ELSE '***' END AS phone,
        NULLIF(TRIM(city), '') AS city,
        NULLIF(TRIM(area), '') AS area,
        TRY_CAST(NULLIF(TRIM(pincode), '') AS INTEGER) AS pincode,
        TRY_CAST(NULLIF(TRIM(signup_date), '') AS DATE) AS signup_date,
        CASE UPPER(TRIM(loyalty_tier)) WHEN 'BRONZE' THEN 'Bronze' WHEN 'SILVER' THEN 'Silver' WHEN 'GOLD' THEN 'Gold' WHEN 'PLATINUM' THEN 'Platinum' ELSE TRIM(loyalty_tier) END AS loyalty_tier,
        UPPER(TRIM(is_active)) AS is_active,
        _loaded_at, _source_file, _run_id, source_duplicate_versions
    FROM ranked
    WHERE record_rank = 1
)
SELECT *, CASE WHEN customer_id IS NULL OR signup_date IS NULL OR signup_date > CURRENT_DATE OR source_duplicate_versions > 1 THEN 'FAIL' ELSE 'PASS' END AS dq_status,
       CASE WHEN customer_id IS NULL THEN 'customer_id missing' WHEN signup_date IS NULL THEN 'invalid signup_date' WHEN signup_date > CURRENT_DATE THEN 'future signup_date' WHEN source_duplicate_versions > 1 THEN 'duplicate customer_id' ELSE NULL END AS failure_reason
FROM typed;

CREATE OR REPLACE TABLE stg_products AS
WITH latest_load AS (
    SELECT MAX(_loaded_at) AS latest_loaded_at FROM raw_products
), current_rows AS (
    SELECT r.* FROM raw_products r
    JOIN latest_load l ON r._loaded_at = l.latest_loaded_at
), source_rows AS (
    SELECT *, COUNT(DISTINCT md5(concat_ws('|', product_id, product_name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date))) OVER (PARTITION BY product_id) AS source_duplicate_versions
    FROM current_rows
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY TRY_CAST(_loaded_at AS TIMESTAMP) DESC, _run_id DESC) AS record_rank
    FROM source_rows
), typed AS (
    SELECT NULLIF(TRIM(product_id), '') AS product_id, NULLIF(TRIM(product_name), '') AS product_name,
           NULLIF(TRIM(category), '') AS category, NULLIF(TRIM(sub_category), '') AS sub_category,
           NULLIF(TRIM(brand), '') AS brand, TRY_CAST(NULLIF(TRIM(mrp), '') AS DECIMAL(12, 2)) AS mrp,
           TRY_CAST(NULLIF(TRIM(cost_price), '') AS DECIMAL(12, 2)) AS cost_price,
           NULLIF(TRIM(unit), '') AS unit, LOWER(TRIM(is_perishable)) = 'true' AS is_perishable,
           TRY_CAST(NULLIF(TRIM(launch_date), '') AS DATE) AS launch_date,
           _loaded_at, _source_file, _run_id, source_duplicate_versions
    FROM ranked WHERE record_rank = 1
)
SELECT *, CASE WHEN product_id IS NULL OR category IS NULL OR mrp IS NULL OR mrp <= 0 OR cost_price > mrp OR source_duplicate_versions > 1 THEN 'FAIL' ELSE 'PASS' END AS dq_status,
       CASE WHEN product_id IS NULL THEN 'product_id missing' WHEN category IS NULL THEN 'category missing' WHEN mrp IS NULL OR mrp <= 0 THEN 'invalid mrp' WHEN cost_price > mrp THEN 'cost_price exceeds mrp' WHEN source_duplicate_versions > 1 THEN 'duplicate product_id' ELSE NULL END AS failure_reason
FROM typed;

CREATE OR REPLACE TABLE stg_stores AS
WITH latest_load AS (
    SELECT MAX(_loaded_at) AS latest_loaded_at FROM raw_stores
), current_rows AS (
    SELECT r.* FROM raw_stores r
    JOIN latest_load l ON r._loaded_at = l.latest_loaded_at
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY TRY_CAST(_loaded_at AS TIMESTAMP) DESC, _run_id DESC) AS record_rank
    FROM current_rows
), typed AS (
    SELECT NULLIF(TRIM(store_id), '') AS store_id, NULLIF(TRIM(store_name), '') AS store_name,
           NULLIF(TRIM(zone), '') AS zone, NULLIF(TRIM(area), '') AS area,
           TRY_CAST(NULLIF(TRIM(pincode), '') AS INTEGER) AS pincode,
           TRY_CAST(NULLIF(TRIM(open_date), '') AS DATE) AS open_date,
           NULLIF(TRIM(manager_name), '') AS manager_name,
           TRY_CAST(NULLIF(TRIM(capacity_orders_per_day), '') AS INTEGER) AS capacity_orders_per_day,
           _loaded_at, _source_file, _run_id
    FROM ranked WHERE record_rank = 1
)
SELECT *, CASE WHEN store_id IS NULL OR zone IS NULL OR open_date IS NULL THEN 'FAIL' ELSE 'PASS' END AS dq_status,
       CASE WHEN store_id IS NULL THEN 'store_id missing' WHEN zone IS NULL THEN 'zone missing' WHEN open_date IS NULL THEN 'invalid open_date' ELSE NULL END AS failure_reason
FROM typed;

CREATE OR REPLACE TABLE stg_payments AS
WITH latest_load AS (
    SELECT MAX(_loaded_at) AS latest_loaded_at FROM raw_payments
), current_rows AS (
    SELECT r.* FROM raw_payments r
    JOIN latest_load l ON r._loaded_at = l.latest_loaded_at
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY TRY_CAST(_loaded_at AS TIMESTAMP) DESC, _run_id DESC) AS record_rank
    FROM current_rows
), typed AS (
    SELECT NULLIF(TRIM(payment_id), '') AS payment_id, NULLIF(TRIM(order_id), '') AS order_id,
           TRY_CAST(NULLIF(TRIM(payment_ts), '') AS TIMESTAMP) AS payment_ts,
           CASE LOWER(TRIM(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cod' THEN 'Cash on Delivery' WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' ELSE TRIM(payment_mode) END AS payment_mode,
           TRY_CAST(NULLIF(TRIM(amount_paid), '') AS DECIMAL(12, 2)) AS amount_paid,
           UPPER(TRIM(payment_status)) AS payment_status, NULLIF(TRIM(gateway_ref), '') AS gateway_ref,
           _loaded_at, _source_file, _run_id
    FROM ranked WHERE record_rank = 1
)
SELECT *, CASE WHEN payment_id IS NULL OR order_id IS NULL OR payment_ts IS NULL OR payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'FAIL' ELSE 'PASS' END AS dq_status,
       CASE WHEN payment_id IS NULL THEN 'payment_id missing' WHEN order_id IS NULL THEN 'order_id missing' WHEN payment_ts IS NULL THEN 'invalid payment_ts' WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'invalid payment_mode' ELSE NULL END AS failure_reason
FROM typed;

CREATE OR REPLACE TABLE stg_orders_snapshot AS
WITH latest_load AS (
    SELECT _source_file, MAX(_loaded_at) AS latest_loaded_at
    FROM raw_orders
    GROUP BY _source_file
), current_rows AS (
    SELECT r.*
    FROM raw_orders r
    JOIN latest_load l
      ON r._source_file = l._source_file
     AND r._loaded_at = l.latest_loaded_at
), source_rows AS (
    SELECT *, COUNT(DISTINCT md5(concat_ws('|', order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes))) OVER (PARTITION BY order_id) AS source_duplicate_versions
    FROM current_rows
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY TRY_CAST(_loaded_at AS TIMESTAMP) DESC, _run_id DESC) AS record_rank
    FROM source_rows
), typed AS (
    SELECT NULLIF(TRIM(order_id), '') AS order_id, TRY_CAST(NULLIF(TRIM(order_ts), '') AS TIMESTAMP) AS order_ts,
           NULLIF(TRIM(customer_id), '') AS customer_id, NULLIF(TRIM(store_id), '') AS store_id,
           NULLIF(TRIM(product_id), '') AS product_id, TRY_CAST(NULLIF(TRIM(quantity), '') AS INTEGER) AS quantity,
           TRY_CAST(NULLIF(TRIM(unit_price), '') AS DECIMAL(12, 2)) AS unit_price,
           TRY_CAST(NULLIF(TRIM(discount_pct), '') AS DECIMAL(6, 2)) AS discount_pct,
           TRY_CAST(NULLIF(TRIM(order_amount), '') AS DECIMAL(12, 2)) AS order_amount,
           CASE LOWER(TRIM(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cod' THEN 'Cash on Delivery' WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' ELSE TRIM(payment_mode) END AS payment_mode,
           UPPER(TRIM(order_status)) AS order_status, TRIM(channel) AS channel,
           TRY_CAST(NULLIF(TRIM(delivery_minutes), '') AS INTEGER) AS delivery_minutes,
           _loaded_at, _source_file, _run_id, source_duplicate_versions
    FROM ranked WHERE record_rank = 1
), checked AS (
    SELECT t.*,
           CASE WHEN t.order_id IS NULL THEN 'order_id missing'
                WHEN t.source_duplicate_versions > 1 AND t._source_file = 'orders.csv' THEN 'duplicate order_id'
                WHEN order_ts IS NULL THEN 'invalid order_ts'
                WHEN order_ts > TIMESTAMP '2026-10-09 23:59:59' THEN 'future order_ts'
                WHEN t.customer_id IS NULL OR c.customer_id IS NULL THEN 'customer_id missing or unknown'
                WHEN t.store_id IS NULL OR s.store_id IS NULL THEN 'store_id missing or unknown'
                WHEN t.product_id IS NULL OR p.product_id IS NULL THEN 'product_id missing or unknown'
                WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'invalid order_status'
                WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'invalid payment_mode'
                WHEN quantity IS NULL OR quantity <= 0 THEN 'quantity must be positive'
                WHEN discount_pct IS NULL OR discount_pct NOT BETWEEN 0 AND 100 THEN 'discount_pct outside 0-100'
                WHEN ABS(order_amount - ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)) > 0.01 THEN 'order_amount formula mismatch'
                WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'DELIVERED order missing delivery_minutes'
                WHEN delivery_minutes >= 120 THEN 'delivery_minutes >= 120'
                ELSE NULL END AS failure_reason
    FROM typed t
    LEFT JOIN stg_customers c ON t.customer_id = c.customer_id AND c.dq_status = 'PASS'
    LEFT JOIN stg_stores s ON t.store_id = s.store_id AND s.dq_status = 'PASS'
    LEFT JOIN stg_products p ON t.product_id = p.product_id AND p.dq_status = 'PASS'
)
SELECT * EXCLUDE (source_duplicate_versions), CAST(order_ts AS DATE) AS order_date,
       CASE WHEN order_status = 'DELIVERED' THEN order_amount ELSE 0 END AS net_revenue,
       CASE WHEN order_status = 'DELIVERED' AND delivery_minutes <= 15 THEN TRUE ELSE FALSE END AS within_15_min,
       CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status
FROM checked;
