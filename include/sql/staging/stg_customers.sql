-- Staging customers. Same rules as customer_checks() in include/nammamart/quality.py.
--
-- Part E, governance: the PII columns (full_name, email, phone) are masked HERE, in
-- the first transform after raw, and the raw columns are not selected forward. Only
-- raw_customers holds the unmasked values, and access to the raw schema is what the
-- governance answer in SUBMISSION.md asks to be restricted.

CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(customer_id)
                              ORDER BY _loaded_at DESC, rowid DESC) AS dup_rank
    FROM raw_customers
),
typed AS (
    SELECT
        trim(customer_id)                                       AS customer_id,
        trim(full_name)                                         AS full_name,
        trim(email)                                             AS email,
        trim(phone)                                             AS phone,
        title_case(nullif(trim(city), ''))                      AS city,
        title_case(trim(area))                                  AS area,
        trim(pincode)                                           AS pincode,
        CAST(try_cast(signup_date AS TIMESTAMP) AS DATE)         AS signup_date,
        try_cast(signup_date AS TIMESTAMP)                      AS signup_ts,
        trim(loyalty_tier)                                      AS loyalty_tier_source,
        upper(trim(loyalty_tier))                               AS loyalty_tier,
        upper(trim(is_active)) = 'Y'                            AS is_active,
        regexp_replace(coalesce(phone, ''), '[^0-9]', '', 'g')  AS phone_digits,
        regexp_replace(coalesce(pincode, ''), '[^0-9]', '', 'g') AS pincode_digits,
        dup_rank, _run_id, _source_file, _loaded_at
    FROM ranked
),
masked AS (
    -- The macros are in include/sql/staging/macros.sql and match transforms.py.
    SELECT *,
        mask_name(full_name)   AS customer_name_masked,
        mask_email(email)      AS email_masked,
        mask_phone(phone)      AS phone_masked
    FROM typed
),
flagged AS (
    SELECT *,
        list_filter([
            CASE WHEN customer_id IS NULL OR customer_id = ''
                 THEN 'Critical: customer_id is missing' END,
            CASE WHEN dup_rank > 1
                 THEN 'Critical: duplicate customer_id, later row kept' END,
            CASE WHEN signup_ts > TIMESTAMP '2026-10-02 00:00:00'
                 THEN 'Critical: signup_date is in the future' END
        ], lambda x: x IS NOT NULL) AS critical_reasons,
        list_filter([
            CASE WHEN NOT regexp_matches(coalesce(email, ''), '^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$')
                 THEN 'Warning: email is not a valid address' END,
            CASE WHEN length(phone_digits) <> 10
                 THEN 'Warning: phone is not 10 digits' END,
            CASE WHEN length(pincode_digits) <> 6
                 THEN 'Warning: pincode is not 6 digits' END,
            CASE WHEN loyalty_tier_source IS NOT NULL
                  AND loyalty_tier_source NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum')
                 THEN 'Warning: loyalty_tier casing needed standardising' END,
            CASE WHEN city IS NULL THEN 'Warning: city is missing' END
        ], lambda x: x IS NOT NULL) AS warning_reasons
    FROM masked
)
SELECT
    customer_id, customer_name_masked, email_masked, phone_masked,
    city, area, pincode, signup_date, loyalty_tier, is_active,
    CASE WHEN len(critical_reasons) > 0 THEN 'REJECTED' ELSE 'PASSED' END AS dq_status,
    array_to_string(list_concat(critical_reasons, warning_reasons), ' | ') AS failure_reason,
    array_to_string(warning_reasons, ' | ')                               AS dq_warnings,
    _run_id, _source_file, _loaded_at
FROM flagged;
