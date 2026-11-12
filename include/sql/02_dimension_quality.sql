-- Critical dimension checks: quarantine failures and exclude from staging.

-- Preserve business-key failures separately so reruns do not duplicate them.
CREATE OR REPLACE TEMP TABLE customers_key_quarantine AS
SELECT d.*, 'conflicting_customer_id' AS quarantine_reason
FROM customers_distinct d
JOIN customers_conflict_keys k USING (customer_id)
UNION ALL
SELECT d.*, 'missing_customer_id' AS quarantine_reason
FROM customers_distinct d
WHERE customer_id IS NULL OR TRIM(customer_id) = '';

CREATE OR REPLACE TEMP TABLE products_key_quarantine AS
SELECT d.*, 'conflicting_product_id' AS quarantine_reason
FROM products_distinct d
JOIN products_conflict_keys k USING (product_id)
UNION ALL
SELECT d.*, 'missing_product_id' AS quarantine_reason
FROM products_distinct d
WHERE product_id IS NULL OR TRIM(product_id) = '';

CREATE OR REPLACE TEMP TABLE stores_key_quarantine AS
SELECT d.*, 'conflicting_store_id' AS quarantine_reason
FROM stores_distinct d
JOIN stores_conflict_keys k USING (store_id)
UNION ALL
SELECT d.*, 'missing_store_id' AS quarantine_reason
FROM stores_distinct d
WHERE store_id IS NULL OR TRIM(store_id) = '';

-- Customer signup dates: future dates are Critical failures.
CREATE OR REPLACE TABLE customers_quarantine AS
SELECT * FROM customers_key_quarantine
UNION ALL
SELECT c.*, 'future_signup_date' AS quarantine_reason
FROM customers_curated c
WHERE TRY_CAST(c.signup_date AS DATE) > CURRENT_DATE;

-- Product pricing: retain every failure reason on the affected row.
CREATE OR REPLACE TABLE products_quarantine AS
SELECT * FROM products_key_quarantine
UNION ALL
SELECT
    p.*,
    CONCAT_WS(',',
        CASE WHEN TRY_CAST(p.mrp AS DECIMAL(12,2)) IS NULL
                   OR TRY_CAST(p.mrp AS DECIMAL(12,2)) <= 0
             THEN 'invalid_mrp' END,
        CASE WHEN TRY_CAST(p.cost_price AS DECIMAL(12,2)) IS NULL
                   OR TRY_CAST(p.cost_price AS DECIMAL(12,2)) < 0
                   OR (TRY_CAST(p.mrp AS DECIMAL(12,2)) IS NOT NULL
                       AND TRY_CAST(p.cost_price AS DECIMAL(12,2))
                           > TRY_CAST(p.mrp AS DECIMAL(12,2)))
             THEN 'invalid_cost_price' END
    ) AS quarantine_reason
FROM products_curated p
WHERE TRY_CAST(p.mrp AS DECIMAL(12,2)) IS NULL
   OR TRY_CAST(p.mrp AS DECIMAL(12,2)) <= 0
   OR TRY_CAST(p.cost_price AS DECIMAL(12,2)) IS NULL
   OR TRY_CAST(p.cost_price AS DECIMAL(12,2)) < 0
   OR TRY_CAST(p.cost_price AS DECIMAL(12,2))
      > TRY_CAST(p.mrp AS DECIMAL(12,2));

-- Store capacity: missing or non-positive capacity is Critical.
CREATE OR REPLACE TABLE stores_quarantine AS
SELECT * FROM stores_key_quarantine
UNION ALL
SELECT s.*, 'invalid_capacity_orders_per_day' AS quarantine_reason
FROM stores_curated s
WHERE TRY_CAST(s.capacity_orders_per_day AS INTEGER) IS NULL
   OR TRY_CAST(s.capacity_orders_per_day AS INTEGER) <= 0;

-- Rebuild staging tables, excluding Critical failures.
CREATE OR REPLACE TABLE stg_customers AS
SELECT
    TRIM(customer_id) AS customer_id,
    NULLIF(TRIM(full_name), '') AS full_name,
    CASE
        WHEN email IS NULL OR TRIM(email) = ''
          OR UPPER(TRIM(email)) = 'N/A' THEN NULL
        WHEN STRPOS(TRIM(email), '@') > 1
          THEN SUBSTR(TRIM(email), 1, 2) || '***@'
               || SPLIT_PART(TRIM(email), '@', 2)
        ELSE NULL
    END AS email,
    CASE
        WHEN REGEXP_REPLACE(COALESCE(TRIM(phone), ''),
                            '[^0-9]', '', 'g') ~ '^[0-9]{10}$'
        THEN LEFT(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), 2)
             || '******' ||
             RIGHT(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), 2)
        ELSE NULL
    END AS phone,
    NULLIF(TRIM(city), '') AS city,
    NULLIF(TRIM(area), '') AS area,
    TRIM(pincode) AS pincode,
    TRY_CAST(signup_date AS DATE) AS signup_date,
    UPPER(TRIM(loyalty_tier)) AS loyalty_tier,
    TRY_CAST(is_active AS BOOLEAN) AS is_active,
    'PASS' AS dq_status,
    CAST(NULL AS VARCHAR) AS failure_reason
FROM customers_curated
WHERE TRY_CAST(signup_date AS DATE) IS NULL
   OR TRY_CAST(signup_date AS DATE) <= CURRENT_DATE;

CREATE OR REPLACE TABLE stg_products AS
SELECT
    TRIM(product_id) AS product_id,
    NULLIF(TRIM(product_name), '') AS product_name,
    COALESCE(NULLIF(UPPER(TRIM(category)), ''), 'UNKNOWN') AS category,
    NULLIF(TRIM(sub_category), '') AS sub_category,
    NULLIF(TRIM(brand), '') AS brand,
    TRY_CAST(mrp AS DECIMAL(12,2)) AS mrp,
    TRY_CAST(cost_price AS DECIMAL(12,2)) AS cost_price,
    NULLIF(TRIM(unit), '') AS unit,
    TRY_CAST(is_perishable AS BOOLEAN) AS is_perishable,
    TRY_CAST(launch_date AS DATE) AS launch_date,
    'PASS' AS dq_status,
    CAST(NULL AS VARCHAR) AS failure_reason
FROM products_curated
WHERE TRY_CAST(mrp AS DECIMAL(12,2)) > 0
  AND TRY_CAST(cost_price AS DECIMAL(12,2)) >= 0
  AND TRY_CAST(cost_price AS DECIMAL(12,2))
      <= TRY_CAST(mrp AS DECIMAL(12,2));

CREATE OR REPLACE TABLE stg_stores AS
SELECT
    TRIM(store_id) AS store_id,
    NULLIF(TRIM(store_name), '') AS store_name,
    COALESCE(NULLIF(UPPER(TRIM(zone)), ''), 'UNKNOWN') AS zone,
    NULLIF(TRIM(area), '') AS area,
    TRIM(pincode) AS pincode,
    TRY_CAST(open_date AS DATE) AS open_date,
    NULLIF(TRIM(manager_name), '') AS manager_name,
    TRY_CAST(capacity_orders_per_day AS INTEGER)
        AS capacity_orders_per_day,
    'PASS' AS dq_status,
    CAST(NULL AS VARCHAR) AS failure_reason
FROM stores_curated
WHERE TRY_CAST(capacity_orders_per_day AS INTEGER) > 0;
