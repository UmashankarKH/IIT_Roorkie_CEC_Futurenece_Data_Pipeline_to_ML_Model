-- include/sql/stg_customers.sql
CREATE OR REPLACE TABLE stg_customers AS
WITH latest_raw AS (
    SELECT *
    FROM raw_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _source_file, _row_id ORDER BY _loaded_at DESC) = 1
),
ranked AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(full_name) AS full_name,
        TRIM(COALESCE(email, '')) AS raw_email,
        TRIM(COALESCE(phone, '')) AS raw_phone,
        TRIM(COALESCE(city, 'Bengaluru')) AS city,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(signup_date AS DATE) AS signup_date,
        CASE
            WHEN UPPER(TRIM(loyalty_tier)) = 'GOLD' THEN 'Gold'
            WHEN UPPER(TRIM(loyalty_tier)) = 'SILVER' THEN 'Silver'
            WHEN UPPER(TRIM(loyalty_tier)) = 'BRONZE' THEN 'Bronze'
            WHEN UPPER(TRIM(loyalty_tier)) = 'PLATINUM' THEN 'Platinum'
            ELSE 'Bronze'
        END AS loyalty_tier,
        UPPER(TRIM(is_active)) AS is_active,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY _row_id) AS row_num
    FROM latest_raw
)
SELECT
    customer_id,
    full_name,
    CASE
        WHEN raw_email = '' OR UPPER(raw_email) IN ('N/A', 'NULL') THEN 'masked@example.com'
        WHEN POSITION('@' IN raw_email) > 2 THEN SUBSTR(raw_email, 1, 2) || '***@' || SPLIT_PART(raw_email, '@', 2)
        WHEN POSITION('@' IN raw_email) > 0 THEN SUBSTR(raw_email, 1, 1) || '***@' || SPLIT_PART(raw_email, '@', 2)
        ELSE 'masked@example.com'
    END AS masked_email,
    CASE
        WHEN LENGTH(REGEXP_REPLACE(raw_phone, '^\+91[-\s]?', '')) >= 4 THEN
            SUBSTR(REGEXP_REPLACE(raw_phone, '^\+91[-\s]?', ''), 1, 2) || '******' || SUBSTR(REGEXP_REPLACE(raw_phone, '^\+91[-\s]?', ''), -2)
        ELSE '99******00'
    END AS masked_phone,
    city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
    CASE
        WHEN row_num > 1 THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CASE
        WHEN row_num > 1 THEN 'uniqueness:duplicate_customer_id'
        WHEN raw_email NOT LIKE '%@%.%' THEN 'validity:invalid_email_format'
        WHEN LENGTH(REGEXP_REPLACE(raw_phone, '^\+91[-\s]?', '')) != 10 THEN 'validity:invalid_phone_digits'
        WHEN signup_date > DATE '2026-10-01' THEN 'timeliness:future_signup_date'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _run_id
FROM ranked;
