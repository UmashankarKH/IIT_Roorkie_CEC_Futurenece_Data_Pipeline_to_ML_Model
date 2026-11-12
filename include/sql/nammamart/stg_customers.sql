-- include/sql/nammamart/stg_customers.sql
-- Mask email and phone, normalise loyalty_tier, dedupe

CREATE OR REPLACE TABLE stg_customers AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_customers
),
base AS (
    SELECT
        customer_id,
        full_name,
        -- Mask email: first 2 chars + *** + @domain
        CASE
            WHEN email IS NOT NULL AND POSITION('@' IN email) > 0
            THEN SUBSTRING(email, 1, 2) || '***@' || SUBSTRING(email, POSITION('@' IN email)+1)
            ELSE email
        END                                     AS email,
        -- Mask phone: first 2 + ****** + last 2
        CASE
            WHEN LENGTH(phone) >= 4
            THEN SUBSTRING(phone,1,2) || '******' || SUBSTRING(phone, LENGTH(phone)-1)
            ELSE '***'
        END                                     AS phone,
        city,
        area,
        pincode,
        CAST(signup_date AS DATE)               AS signup_date,
        INITCAP(TRIM(loyalty_tier))             AS loyalty_tier,
        is_active,
        _loaded_at,
        _source_file,
        _run_id
    FROM deduped
    WHERE rn = 1
),
checked AS (
    SELECT *,
        CASE
            WHEN customer_id IS NULL           THEN 'customer_id is null'
            WHEN signup_date > CURRENT_DATE    THEN 'signup_date is in the future'
            ELSE NULL
        END AS failure_reason
    FROM base
)
SELECT *,
    CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status
FROM checked;
