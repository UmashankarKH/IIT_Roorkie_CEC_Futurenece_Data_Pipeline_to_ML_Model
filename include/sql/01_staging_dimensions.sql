-- NammaMart ELT: dimension staging
-- Inputs: raw_customers_current, raw_products_current,
--         raw_stores_current (current-run temporary tables).

-- =========================================================
-- CUSTOMERS
-- =========================================================
CREATE OR REPLACE TABLE customers_distinct AS
SELECT DISTINCT * FROM raw_customers_current;

CREATE OR REPLACE TABLE customers_conflict_keys AS
SELECT customer_id
FROM customers_distinct
WHERE customer_id IS NOT NULL AND TRIM(customer_id) <> ''
GROUP BY customer_id
HAVING COUNT(*) > 1;

CREATE OR REPLACE TABLE customers_quarantine AS
SELECT d.*, 'conflicting_customer_id' AS quarantine_reason
FROM customers_distinct d
JOIN customers_conflict_keys k USING (customer_id)
UNION ALL
SELECT d.*, 'missing_customer_id' AS quarantine_reason
FROM customers_distinct d
WHERE customer_id IS NULL OR TRIM(customer_id) = '';

CREATE OR REPLACE TABLE customers_curated AS
SELECT d.*
FROM customers_distinct d
LEFT JOIN customers_conflict_keys k USING (customer_id)
WHERE d.customer_id IS NOT NULL
  AND TRIM(d.customer_id) <> ''
  AND k.customer_id IS NULL;

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
        WHEN REGEXP_REPLACE(COALESCE(TRIM(phone), ''), '[^0-9]', '', 'g')
             ~ '^[0-9]{10}$'
        THEN LEFT(
            REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), 2
        ) || '******' || RIGHT(
            REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), 2
        )
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
FROM customers_curated;

-- =========================================================
-- PRODUCTS
-- =========================================================
CREATE OR REPLACE TABLE products_distinct AS
SELECT DISTINCT * FROM raw_products_current;

CREATE OR REPLACE TABLE products_conflict_keys AS
SELECT product_id
FROM products_distinct
WHERE product_id IS NOT NULL AND TRIM(product_id) <> ''
GROUP BY product_id
HAVING COUNT(*) > 1;

CREATE OR REPLACE TABLE products_quarantine AS
SELECT d.*, 'conflicting_product_id' AS quarantine_reason
FROM products_distinct d
JOIN products_conflict_keys k USING (product_id)
UNION ALL
SELECT d.*, 'missing_product_id' AS quarantine_reason
FROM products_distinct d
WHERE product_id IS NULL OR TRIM(product_id) = '';

CREATE OR REPLACE TABLE products_curated AS
SELECT d.*
FROM products_distinct d
LEFT JOIN products_conflict_keys k USING (product_id)
WHERE d.product_id IS NOT NULL
  AND TRIM(d.product_id) <> ''
  AND k.product_id IS NULL;

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
FROM products_curated;

-- =========================================================
-- STORES
-- =========================================================
CREATE OR REPLACE TABLE stores_distinct AS
SELECT DISTINCT * FROM raw_stores_current;

CREATE OR REPLACE TABLE stores_conflict_keys AS
SELECT store_id
FROM stores_distinct
WHERE store_id IS NOT NULL AND TRIM(store_id) <> ''
GROUP BY store_id
HAVING COUNT(*) > 1;

CREATE OR REPLACE TABLE stores_quarantine AS
SELECT d.*, 'conflicting_store_id' AS quarantine_reason
FROM stores_distinct d
JOIN stores_conflict_keys k USING (store_id)
UNION ALL
SELECT d.*, 'missing_store_id' AS quarantine_reason
FROM stores_distinct d
WHERE store_id IS NULL OR TRIM(store_id) = '';

CREATE OR REPLACE TABLE stores_curated AS
SELECT d.*
FROM stores_distinct d
LEFT JOIN stores_conflict_keys k USING (store_id)
WHERE d.store_id IS NOT NULL
  AND TRIM(d.store_id) <> ''
  AND k.store_id IS NULL;

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
FROM stores_curated;
