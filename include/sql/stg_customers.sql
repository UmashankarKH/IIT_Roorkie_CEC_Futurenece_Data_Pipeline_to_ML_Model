-- Staging: customers received in THIS run.
-- Personal data is masked here, so nothing downstream of raw ever sees a full
-- email, phone number or surname:
--   email  jane.doe42@example.com -> ja***@example.com
--   phone  9876543210                 -> 98******10
--   name   Jane Doe                   -> Jane D.
-- Same customer_id twice: keep the first row in the file, the rest fail customer_id_unique.

CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
    SELECT
        *,
        trim(email) AS email_t,
        trim(phone) AS phone_t,
        try_strptime(trim(signup_date), '%Y-%m-%d') AS signup_parsed,
        upper(left(trim(loyalty_tier), 1)) || lower(substr(trim(loyalty_tier), 2)) AS tier_std,
        CAST(getvariable('base_business_date') AS DATE) + INTERVAL 1 DAY + INTERVAL 9 HOUR AS as_of,
        row_number() OVER (PARTITION BY customer_id ORDER BY _row_num) AS dup_rank
    FROM raw_customers
    WHERE _run_id = getvariable('run_id')
),

checked AS (
    SELECT
        *,
        list_filter([
            CASE WHEN coalesce(trim(customer_id), '') = '' THEN 'customer_id_not_null' END,
            CASE WHEN dup_rank > 1 THEN 'customer_id_unique' END,
            CASE WHEN signup_parsed IS NULL THEN 'signup_date_valid' END,
            CASE WHEN tier_std NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN 'loyalty_tier_valid' END,
            CASE WHEN signup_parsed > as_of THEN 'signup_date_not_future' END
        ], x -> x IS NOT NULL) AS dq_failed,
        list_filter([
            CASE WHEN coalesce(trim(city), '') = '' THEN 'city_present' END,
            CASE WHEN NOT regexp_full_match(coalesce(email_t, ''), '[^@\s]+@[^@\s]+\.[A-Za-z]{2,}') THEN 'email_format' END,
            CASE WHEN NOT regexp_full_match(coalesce(phone_t, ''), '[6-9]\d{9}') THEN 'phone_10_digits' END,
            CASE WHEN NOT regexp_full_match(coalesce(trim(pincode), ''), '\d{6}') THEN 'pincode_6_digits' END,
            CASE WHEN loyalty_tier <> tier_std THEN 'loyalty_tier_casing' END
        ], x -> x IS NOT NULL) AS dq_warnings
    FROM src
)

SELECT
    customer_id,
    CASE
        WHEN len(string_split(trim(full_name), ' ')) = 1 THEN trim(full_name)
        ELSE string_split(trim(full_name), ' ')[1] || ' ' || left(string_split(trim(full_name), ' ')[-1], 1) || '.'
    END AS customer_name,
    CASE
        WHEN position('@' IN email_t) > 0 THEN left(email_t, 2) || '***' || substr(email_t, position('@' IN email_t))
    END AS email_masked,
    CASE
        WHEN coalesce(phone_t, '') = '' THEN NULL
        WHEN length(phone_t) <= 4 THEN repeat('*', length(phone_t))
        ELSE left(phone_t, 2) || repeat('*', length(phone_t) - 4) || right(phone_t, 2)
    END AS phone_masked,
    nullif(trim(city), '') AS city,
    area,
    pincode,
    CAST(signup_parsed AS DATE) AS signup_date,
    tier_std AS loyalty_tier,
    is_active = 'Y' AS is_active,
    CASE WHEN len(dq_failed) = 0 THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    array_to_string(dq_failed, '; ') AS failure_reason,
    array_to_string(dq_warnings, '; ') AS warnings,
    dq_failed,
    dq_warnings,
    _row_num,
    _source_file,
    _run_id
FROM checked;
