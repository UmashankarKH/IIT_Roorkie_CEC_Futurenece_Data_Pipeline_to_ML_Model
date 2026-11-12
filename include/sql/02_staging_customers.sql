-- include/sql/02_staging_customers.sql
-- Staging Customers: Cast types, deduplicate, mask PII, standardize loyalty_tier, flag DQ.

CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(full_name) AS full_name,
        TRIM(email) AS raw_email,
        TRIM(phone) AS raw_phone,
        TRIM(city) AS city,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(signup_date AS DATE) AS signup_date,
        CASE LOWER(TRIM(loyalty_tier))
            WHEN 'bronze' THEN 'Bronze'
            WHEN 'silver' THEN 'Silver'
            WHEN 'gold' THEN 'Gold'
            WHEN 'platinum' THEN 'Platinum'
            ELSE UPPER(SUBSTR(TRIM(loyalty_tier), 1, 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2))
        END AS loyalty_tier,
        UPPER(TRIM(is_active)) AS is_active,
        _loaded_at,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY _loaded_at DESC) AS rnk
    FROM raw_customers
)
SELECT
    customer_id,
    full_name,
    -- PII Masking compliant with DPDP Act 2023
    CASE
        WHEN raw_email IS NULL OR raw_email = '' OR LOWER(raw_email) IN ('nan', 'none') THEN ''
        WHEN raw_email LIKE '%@%' THEN SUBSTR(SPLIT_PART(raw_email, '@', 1), 1, 2) || '***@' || SPLIT_PART(raw_email, '@', 2)
        ELSE '***'
    END AS masked_email,
    CASE
        WHEN raw_phone IS NULL OR raw_phone = '' THEN ''
        WHEN LENGTH(REGEXP_REPLACE(raw_phone, '\D', '', 'g')) >= 10 THEN
            SUBSTR(REGEXP_REPLACE(raw_phone, '\D', '', 'g'), 1, 2) || '******' || SUBSTR(REGEXP_REPLACE(raw_phone, '\D', '', 'g'), -2)
        ELSE '******'
    END AS masked_phone,
    COALESCE(city, 'Bengaluru') AS city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
    CASE
        WHEN signup_date > DATE '2026-10-02' THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CASE
        WHEN signup_date > DATE '2026-10-02' THEN 'Timeliness: future signup_date'
        ELSE ''
    END AS failure_reason,
    _loaded_at,
    _run_id
FROM ranked
WHERE rnk = 1;
