-- Clean and type the three dimensions. Raw tables remain unchanged.
CREATE OR REPLACE TEMP TABLE _customers_ranked AS
WITH latest_copy AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY _source_file, _source_row_number
        ORDER BY _loaded_at DESC, _run_id DESC
    ) AS _copy_rank
    FROM raw_customers
), stable AS (
    SELECT * EXCLUDE (_copy_rank) FROM latest_copy WHERE _copy_rank = 1
)
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY NULLIF(TRIM(customer_id), '') ORDER BY _source_row_number) AS _key_rank,
       COUNT(*) OVER (PARTITION BY NULLIF(TRIM(customer_id), '')) AS _duplicate_count
FROM stable;

CREATE OR REPLACE TABLE stg_customers AS
WITH canonical AS (
    SELECT * FROM _customers_ranked WHERE _key_rank = 1
), typed AS (
    SELECT
        NULLIF(TRIM(customer_id), '') AS customer_id,
        CASE WHEN REGEXP_MATCHES(TRIM(email), '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$')
             THEN SUBSTR(TRIM(email), 1, 2) || '***@' || SPLIT_PART(TRIM(email), '@', 2)
             ELSE '***' END AS masked_email,
        CASE WHEN REGEXP_MATCHES(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), '^[0-9]{10}$')
             THEN SUBSTR(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), 1, 2) || '******' || RIGHT(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), 2)
             ELSE '***' END AS masked_phone,
        NULLIF(TRIM(city), '') AS city,
        NULLIF(TRIM(area), '') AS area,
        NULLIF(TRIM(pincode), '') AS pincode,
        TRY_CAST(NULLIF(TRIM(signup_date), '') AS TIMESTAMP) AS signup_date,
        INITCAP(LOWER(TRIM(loyalty_tier))) AS loyalty_tier,
        _source_file,
        _source_row_number,
        _duplicate_count
    FROM canonical
), evaluated AS (
    SELECT *,
        CONCAT_WS('; ',
            CASE WHEN customer_id IS NULL THEN 'customer_id is required' END,
            CASE WHEN pincode IS NULL OR NOT REGEXP_MATCHES(pincode, '^[0-9]{6}$') THEN 'pincode must contain six digits' END,
            CASE WHEN signup_date IS NULL THEN 'signup_date is missing or invalid' END,
            CASE WHEN signup_date > CURRENT_TIMESTAMP THEN 'signup_date is in the future' END
        ) AS critical_reason,
        CONCAT_WS('; ',
            CASE WHEN masked_email = '***' THEN 'email format should be reviewed' END,
            CASE WHEN masked_phone = '***' THEN 'phone should contain 10 digits' END,
            CASE WHEN city IS NULL THEN 'city is missing' END,
            CASE WHEN loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN 'loyalty_tier should be reviewed' END
        ) AS warning_reason
    FROM typed
)
SELECT
    customer_id, masked_email, masked_phone, loyalty_tier,
    _source_file, _source_row_number, _duplicate_count,
    CONCAT_WS('; ', NULLIF(critical_reason, ''), NULLIF(warning_reason, '')) AS failure_reason,
    CASE WHEN critical_reason <> '' THEN 'FAIL'
         WHEN warning_reason <> '' THEN 'WARNING'
         ELSE 'PASS' END AS dq_status
FROM evaluated;

CREATE OR REPLACE TEMP TABLE _products_ranked AS
WITH latest_copy AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY _source_file, _source_row_number
        ORDER BY _loaded_at DESC, _run_id DESC
    ) AS _copy_rank
    FROM raw_products
), stable AS (
    SELECT * EXCLUDE (_copy_rank) FROM latest_copy WHERE _copy_rank = 1
)
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY NULLIF(TRIM(product_id), '') ORDER BY _source_row_number) AS _key_rank,
       COUNT(*) OVER (PARTITION BY NULLIF(TRIM(product_id), '')) AS _duplicate_count
FROM stable;

CREATE OR REPLACE TABLE stg_products AS
WITH canonical AS (
    SELECT * FROM _products_ranked WHERE _key_rank = 1
), typed AS (
    SELECT
        NULLIF(TRIM(product_id), '') AS product_id,
        NULLIF(TRIM(product_name), '') AS product_name,
        NULLIF(TRIM(category), '') AS category,
        NULLIF(TRIM(sub_category), '') AS sub_category,
        NULLIF(TRIM(brand), '') AS brand,
        TRY_CAST(NULLIF(TRIM(mrp), '') AS DOUBLE) AS mrp,
        TRY_CAST(NULLIF(TRIM(cost_price), '') AS DOUBLE) AS cost_price,
        NULLIF(TRIM(unit), '') AS unit,
        UPPER(TRIM(is_perishable)) AS is_perishable,
        TRY_CAST(NULLIF(TRIM(launch_date), '') AS DATE) AS launch_date,
        _source_file, _source_row_number, _duplicate_count
    FROM canonical
), evaluated AS (
    SELECT *, CONCAT_WS('; ',
        CASE WHEN product_id IS NULL THEN 'product_id is required' END,
        CASE WHEN product_name IS NULL THEN 'product_name is required' END,
        CASE WHEN category IS NULL THEN 'category is required' END,
        CASE WHEN mrp IS NULL OR mrp <= 0 THEN 'mrp must be greater than zero' END,
        CASE WHEN cost_price IS NULL OR mrp IS NULL OR cost_price > mrp THEN 'cost_price must not exceed mrp' END,
        CASE WHEN launch_date IS NULL THEN 'launch_date is missing or invalid' END
    ) AS critical_reason
    FROM typed
)
SELECT product_id, product_name, category, sub_category, brand, mrp, cost_price,
       unit, is_perishable, launch_date, _source_file, _source_row_number,
       _duplicate_count, critical_reason AS failure_reason,
       CASE WHEN critical_reason <> '' THEN 'FAIL' ELSE 'PASS' END AS dq_status
FROM evaluated;

CREATE OR REPLACE TEMP TABLE _stores_ranked AS
WITH latest_copy AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY _source_file, _source_row_number
        ORDER BY _loaded_at DESC, _run_id DESC
    ) AS _copy_rank
    FROM raw_stores
), stable AS (
    SELECT * EXCLUDE (_copy_rank) FROM latest_copy WHERE _copy_rank = 1
)
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY NULLIF(TRIM(store_id), '') ORDER BY _source_row_number) AS _key_rank,
       COUNT(*) OVER (PARTITION BY NULLIF(TRIM(store_id), '')) AS _duplicate_count
FROM stable;

CREATE OR REPLACE TABLE stg_stores AS
WITH canonical AS (
    SELECT * FROM _stores_ranked WHERE _key_rank = 1
), typed AS (
    SELECT
        NULLIF(TRIM(store_id), '') AS store_id,
        NULLIF(TRIM(store_name), '') AS store_name,
        INITCAP(LOWER(TRIM(zone))) AS zone,
        NULLIF(TRIM(area), '') AS area,
        NULLIF(TRIM(pincode), '') AS pincode,
        TRY_CAST(NULLIF(TRIM(open_date), '') AS DATE) AS open_date,
        TRY_CAST(NULLIF(TRIM(capacity_orders_per_day), '') AS INTEGER) AS capacity_orders_per_day,
        _source_file, _source_row_number, _duplicate_count
    FROM canonical
), evaluated AS (
    SELECT *, CONCAT_WS('; ',
        CASE WHEN store_id IS NULL THEN 'store_id is required' END,
        CASE WHEN store_name IS NULL THEN 'store_name is required' END,
        CASE WHEN zone IS NULL OR zone NOT IN ('North', 'South', 'East', 'West') THEN 'zone is invalid' END,
        CASE WHEN pincode IS NULL OR NOT REGEXP_MATCHES(pincode, '^[0-9]{6}$') THEN 'pincode must contain six digits' END,
        CASE WHEN capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0 THEN 'capacity_orders_per_day must be positive' END,
        CASE WHEN open_date IS NULL THEN 'open_date is missing or invalid' END
    ) AS critical_reason
    FROM typed
)
SELECT store_id, store_name, zone, area, pincode, open_date, capacity_orders_per_day,
       _source_file, _source_row_number, _duplicate_count,
       critical_reason AS failure_reason,
       CASE WHEN critical_reason <> '' THEN 'FAIL' ELSE 'PASS' END AS dq_status
FROM evaluated;
