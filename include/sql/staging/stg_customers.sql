-- stg_customers: deduplicated customer master. PII is masked here; the raw
-- name/email/phone never leave raw_customers.
CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
    SELECT * FROM raw_customers WHERE _run_id = {{run_id}}
),
typed AS (
    SELECT
        nullif(trim(customer_id), '')                    AS customer_id,
        nullif(trim(full_name), '')                      AS full_name,
        nullif(trim(email), '')                          AS email,
        nullif(trim(phone), '')                          AS phone,
        nullif(trim(city), '')                           AS city,
        trim(area)                                       AS area,
        nullif(trim(pincode), '')                        AS pincode,
        try_strptime(trim(signup_date), '%Y-%m-%d')::DATE AS signup_date_raw,
        nullif(trim(loyalty_tier), '')                   AS tier_raw,
        upper(left(trim(loyalty_tier), 1)) || lower(substr(trim(loyalty_tier), 2)) AS loyalty_tier,
        trim(is_active) = 'Y'                            AS is_active,
        _row_num, _source_file, _run_id
    FROM src
),
flags AS (
    SELECT *,
        customer_id IS NULL                                                         AS f_cust_id_not_null,
        customer_id IS NOT NULL
            AND row_number() OVER (PARTITION BY customer_id ORDER BY _row_num) > 1  AS f_cust_id_unique,
        city IS NULL                                                                AS f_cust_city_present,
        NOT coalesce(regexp_matches(email, '^[^@\s]+@[^@\s]+\.[^@\s]+$'), false)   AS f_cust_email_valid,
        NOT coalesce(regexp_full_match(phone, '\d{10}'), false)                    AS f_cust_phone_valid,
        NOT coalesce(regexp_full_match(pincode, '\d{6}'), false)                   AS f_cust_pincode_valid,
        coalesce(loyalty_tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')
                 AND tier_raw <> loyalty_tier, false)                               AS f_cust_tier_casing,
        coalesce(signup_date_raw >= CAST({{cutoff}} AS TIMESTAMP)::DATE, false)    AS f_cust_signup_not_future
    FROM typed
)
SELECT
    customer_id,
    mask_name(full_name)  AS customer_name_masked,
    mask_email(email)     AS email_masked,
    mask_phone(phone)     AS phone_masked,
    city, area, pincode,
    CASE WHEN f_cust_signup_not_future THEN NULL ELSE signup_date_raw END AS signup_date,
    loyalty_tier, is_active,
    f_cust_id_not_null, f_cust_id_unique, f_cust_city_present, f_cust_email_valid,
    f_cust_phone_valid, f_cust_pincode_valid, f_cust_tier_casing, f_cust_signup_not_future,
    nullif(concat_ws('; ',
        CASE WHEN f_cust_id_not_null THEN 'customer_id is null' END,
        CASE WHEN f_cust_id_unique THEN 'duplicate customer_id' END), '')            AS failure_reason,
    nullif(concat_ws('; ',
        CASE WHEN f_cust_city_present THEN 'city is blank' END,
        CASE WHEN f_cust_email_valid THEN 'email format invalid' END,
        CASE WHEN f_cust_phone_valid THEN 'phone is not 10 digits' END,
        CASE WHEN f_cust_pincode_valid THEN 'pincode is not 6 digits' END,
        CASE WHEN f_cust_tier_casing THEN 'loyalty_tier casing not standard' END,
        CASE WHEN f_cust_signup_not_future THEN 'signup_date is in the future' END), '') AS dq_warnings,
    CASE WHEN f_cust_id_not_null OR f_cust_id_unique THEN 'FAILED' ELSE 'PASSED' END AS dq_status,
    _row_num, _source_file, _run_id
FROM flags;
