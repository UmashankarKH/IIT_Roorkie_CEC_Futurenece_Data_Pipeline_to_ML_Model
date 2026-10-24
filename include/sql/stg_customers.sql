-- include/sql/stg_customers.sql
-- Staging layer for customers: deduplication, PII masking, DQ checks, type casting

CREATE OR REPLACE TABLE stg_customers AS
WITH deduped AS (
    SELECT 
        customer_id,
        full_name,
        email,
        phone,
        city,
        area,
        pincode,
        TRY_CAST(signup_date AS DATE) AS signup_date_parsed,
        signup_date AS signup_date_raw,
        UPPER(TRIM(COALESCE(loyalty_tier, 'BRONZE'))) AS loyalty_tier,
        UPPER(TRIM(COALESCE(is_active, 'Y'))) AS is_active,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) AS rn
    FROM raw_customers
),
evaluated AS (
    SELECT
        customer_id,
        full_name,
        -- PII Masking: email
        CASE 
            WHEN email IS NULL OR email NOT LIKE '%@%' THEN 'masked@nammamart.internal'
            ELSE SUBSTR(email, 1, 2) || '***@' || SPLIT_PART(email, '@', 2)
        END AS email_masked,
        -- PII Masking: phone (10 digits -> first 2, 6 stars, last 2)
        CASE 
            WHEN LENGTH(TRIM(CAST(phone AS VARCHAR))) = 10 
                THEN SUBSTR(TRIM(CAST(phone AS VARCHAR)), 1, 2) || '******' || SUBSTR(TRIM(CAST(phone AS VARCHAR)), 9, 2)
            ELSE '******' || RIGHT(TRIM(CAST(phone AS VARCHAR)), 2)
        END AS phone_masked,
        COALESCE(city, 'Bengaluru') AS city,
        area,
        pincode,
        signup_date_parsed AS signup_date,
        loyalty_tier,
        is_active,
        _loaded_at,
        _source_file,
        _run_id,
        rn,
        -- DQ Evaluation
        CASE 
            WHEN rn > 1 THEN 'DUPLICATE_CUSTOMER_ID'
            WHEN signup_date_parsed > DATE '2026-10-02' THEN 'FUTURE_SIGNUP_DATE'
            ELSE NULL
        END AS critical_failure,
        CASE
            WHEN email IS NULL OR email NOT LIKE '%@%.%' THEN 'INVALID_OR_MISSING_EMAIL'
            WHEN LENGTH(TRIM(CAST(phone AS VARCHAR))) != 10 THEN 'INVALID_PHONE_LENGTH'
            WHEN LENGTH(TRIM(CAST(pincode AS VARCHAR))) != 6 THEN 'INVALID_PINCODE_LENGTH'
            ELSE NULL
        END AS warning_reason
    FROM deduped
)
SELECT
    customer_id,
    full_name,
    email_masked,
    phone_masked,
    city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
    _loaded_at,
    _source_file,
    _run_id,
    CASE 
        WHEN critical_failure IS NOT NULL THEN 'QUARANTINE'
        WHEN warning_reason IS NOT NULL THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    COALESCE(critical_failure, warning_reason) AS failure_reason
FROM evaluated;
