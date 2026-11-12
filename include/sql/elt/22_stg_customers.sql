-- Staging: customers. PII is masked here: raw email/phone are NOT carried into staging or marts.
CREATE OR REPLACE TABLE stg_customers AS
WITH std AS (
    SELECT _row_num, _source_file, _run_id,
           trim(customer_id) AS customer_id, trim(full_name) AS full_name, trim(email) AS email, trim(phone) AS phone,
           trim(city) AS city, trim(area) AS area, trim(pincode) AS pincode,
           try_strptime(trim(signup_date), '%Y-%m-%d') AS signup_date, trim(loyalty_tier) AS loyalty_tier_raw,
           trim(is_active) AS is_active
    FROM v_latest_raw_customers
), flags AS (
    SELECT *,
        coalesce(customer_id, '') = ''                                       AS chk_customers_customer_id_not_null,
        coalesce(city, '') = ''                                              AS chk_customers_city_not_null,
        row_number() OVER (PARTITION BY customer_id ORDER BY _row_num) > 1   AS chk_customers_customer_id_unique,
        NOT regexp_full_match(coalesce(email, ''), '[^@\s]+@[^@\s]+\.[A-Za-z]{2,}') AS chk_customers_email_format,
        NOT regexp_full_match(coalesce(phone, ''), '\d{10}')                 AS chk_customers_phone_10_digits,
        NOT regexp_full_match(coalesce(pincode, ''), '\d{6}')                AS chk_customers_pincode_6_digits,
        signup_date IS NULL                                                  AS chk_customers_signup_date_parses,
        coalesce(is_active, '') NOT IN ('Y', 'N')                            AS chk_customers_is_active_valid,
        coalesce(loyalty_tier_raw, '') NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum')
                                                                             AS chk_customers_loyalty_tier_casing,
        coalesce(signup_date > TIMESTAMP '{{as_of}}', false)                 AS chk_customers_signup_date_not_future
    FROM std
)
SELECT _row_num, _source_file, _run_id, customer_id, full_name,
    CASE WHEN email LIKE '%@%' THEN left(split_part(email, '@', 1), 2) || '***@' || split_part(email, '@', 2)
         ELSE '***' END AS email_masked,
    CASE WHEN regexp_full_match(phone, '\d{10}') THEN left(phone, 2) || '******' || right(phone, 2)
         ELSE '**********' END AS phone_masked,
    city, area, pincode, signup_date,
    upper(left(loyalty_tier_raw, 1)) || lower(substr(loyalty_tier_raw, 2)) AS loyalty_tier, is_active,
    chk_customers_customer_id_not_null, chk_customers_city_not_null, chk_customers_customer_id_unique,
    chk_customers_email_format, chk_customers_phone_10_digits, chk_customers_pincode_6_digits,
    chk_customers_signup_date_parses, chk_customers_is_active_valid, chk_customers_loyalty_tier_casing,
    chk_customers_signup_date_not_future,
    concat_ws('; ',
        CASE WHEN chk_customers_customer_id_not_null THEN 'customers_customer_id_not_null' END,
        CASE WHEN chk_customers_customer_id_unique THEN 'customers_customer_id_unique' END,
        CASE WHEN chk_customers_signup_date_parses THEN 'customers_signup_date_parses' END,
        CASE WHEN chk_customers_signup_date_not_future THEN 'customers_signup_date_not_future' END) AS failure_reason,
    concat_ws('; ',
        CASE WHEN chk_customers_city_not_null THEN 'customers_city_not_null' END,
        CASE WHEN chk_customers_email_format THEN 'customers_email_format' END,
        CASE WHEN chk_customers_phone_10_digits THEN 'customers_phone_10_digits' END,
        CASE WHEN chk_customers_pincode_6_digits THEN 'customers_pincode_6_digits' END,
        CASE WHEN chk_customers_is_active_valid THEN 'customers_is_active_valid' END,
        CASE WHEN chk_customers_loyalty_tier_casing THEN 'customers_loyalty_tier_casing' END) AS warning_reason,
    CASE WHEN chk_customers_customer_id_not_null OR chk_customers_customer_id_unique
              OR chk_customers_signup_date_parses OR chk_customers_signup_date_not_future THEN 'FAIL'
         WHEN chk_customers_city_not_null OR chk_customers_email_format OR chk_customers_phone_10_digits
              OR chk_customers_pincode_6_digits OR chk_customers_is_active_valid
              OR chk_customers_loyalty_tier_casing THEN 'WARN' ELSE 'PASS' END AS dq_status
FROM flags;
