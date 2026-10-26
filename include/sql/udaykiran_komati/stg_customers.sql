CREATE OR REPLACE TABLE stg_customers AS
WITH latest AS (
    SELECT *,
           row_number() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_customers
)
SELECT
    customer_id,
    trim(full_name) AS full_name,
    CASE
        WHEN position('@' IN email) > 0 AND position('.' IN email) > position('@' IN email)
            THEN substring(email, 1, 2) || '***' || substring(email, position('@' IN email))
        ELSE '***'
    END AS email_masked,
    CASE
        WHEN length(regexp_replace(phone, '[^0-9]', '', 'g')) >= 4
            THEN substring(regexp_replace(phone, '[^0-9]', '', 'g'), 1, 2)
                 || '******'
                 || substring(regexp_replace(phone, '[^0-9]', '', 'g'), length(regexp_replace(phone, '[^0-9]', '', 'g')) - 1)
        ELSE '**********'
    END AS phone_masked,
    trim(city) AS city,
    trim(area) AS area,
    trim(pincode) AS pincode,
    signup_date,
    CASE lower(trim(loyalty_tier))
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'silver' THEN 'Silver'
        WHEN 'gold' THEN 'Gold'
        WHEN 'platinum' THEN 'Platinum'
        ELSE trim(loyalty_tier)
    END AS loyalty_tier,
    is_active,
    CASE
        WHEN customer_id IS NULL OR trim(customer_id) = '' THEN 'FAIL'
        WHEN try_cast(signup_date AS DATE) > DATE '2026-09-30' THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN try_cast(signup_date AS DATE) > DATE '2026-09-30' THEN 'future signup_date'
        ELSE NULL
    END AS failure_reason
FROM latest
WHERE rn = 1;
