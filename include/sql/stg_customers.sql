-- stg_customers: cleaned, typed and quality-checked copy of raw_customers for ONE run.
-- Rows that fail a Critical check stay here with dq_status = 'FAIL' (marts ignore them).
CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
  SELECT trim(customer_id) AS customer_id, trim(full_name) AS full_name, trim(email) AS email_raw, trim(phone) AS phone_raw,
    trim(city) AS city, trim(area) AS area, trim(pincode) AS pincode, trim(signup_date) AS signup_date_raw,
    trim(loyalty_tier) AS tier_raw, trim(is_active) AS is_active_raw,
    _source_row, _source_file, _run_id
  FROM raw_customers
  WHERE _run_id = '{{run_id}}'
),
typed AS (
  SELECT src.*,
    CASE lower(tier_raw) WHEN 'bronze' THEN 'Bronze' WHEN 'silver' THEN 'Silver' WHEN 'gold' THEN 'Gold' WHEN 'platinum' THEN 'Platinum' END AS loyalty_tier,
    CAST(try_strptime(signup_date_raw, '%Y-%m-%d') AS DATE) AS signup_date,
    row_number() OVER (PARTITION BY customer_id ORDER BY _source_row) AS _rn
  FROM src
),
flagged AS (
  SELECT typed.*,
    coalesce((customer_id = ''), false) AS chk_customer_id_not_null,
    coalesce((full_name = ''), false) AS chk_full_name_not_null,
    coalesce((city = ''), false) AS chk_city_not_null,
    coalesce((customer_id <> '' AND _rn > 1), false) AS chk_customer_id_unique,
    coalesce((NOT regexp_matches(email_raw, '^[\w.+-]+@[\w-]+(\.[\w-]+)+$')), false) AS chk_email_valid,
    coalesce((NOT regexp_matches(phone_raw, '^\d{10}$')), false) AS chk_phone_is_10_digits,
    coalesce((NOT regexp_matches(pincode, '^\d{6}$')), false) AS chk_pincode_is_6_digits,
    coalesce((loyalty_tier IS NULL), false) AS chk_loyalty_tier_valid,
    coalesce((loyalty_tier IS NOT NULL AND tier_raw <> loyalty_tier), false) AS chk_loyalty_tier_needs_standardising,
    coalesce((is_active_raw NOT IN ('Y', 'N')), false) AS chk_is_active_valid,
    coalesce((signup_date IS NULL), false) AS chk_signup_date_valid,
    coalesce((CAST(signup_date AS TIMESTAMP) > CAST('{{as_of}}' AS TIMESTAMP)), false) AS chk_signup_date_not_future
  FROM typed
),
scored AS (
  SELECT flagged.*,
    concat_ws(', ', CASE WHEN chk_customer_id_not_null THEN 'customer_id_not_null' END, CASE WHEN chk_customer_id_unique THEN 'customer_id_unique' END, CASE WHEN chk_loyalty_tier_valid THEN 'loyalty_tier_valid' END, CASE WHEN chk_signup_date_valid THEN 'signup_date_valid' END, CASE WHEN chk_signup_date_not_future THEN 'signup_date_not_future' END) AS failed_checks,
    concat_ws('; ', CASE WHEN chk_customer_id_not_null THEN 'customer_id is missing' END, CASE WHEN chk_customer_id_unique THEN 'duplicate customer_id (repeat of an earlier row)' END, CASE WHEN chk_loyalty_tier_valid THEN 'loyalty_tier is not Bronze/Silver/Gold/Platinum' END, CASE WHEN chk_signup_date_valid THEN 'signup_date is missing or not a valid date' END, CASE WHEN chk_signup_date_not_future THEN 'signup_date is in the future' END) AS failure_reason,
    concat_ws('; ', CASE WHEN chk_full_name_not_null THEN 'full_name is missing' END, CASE WHEN chk_city_not_null THEN 'city is missing' END, CASE WHEN chk_email_valid THEN 'email is missing or malformed' END, CASE WHEN chk_phone_is_10_digits THEN 'phone is not exactly 10 digits' END, CASE WHEN chk_pincode_is_6_digits THEN 'pincode is not exactly 6 digits' END, CASE WHEN chk_loyalty_tier_needs_standardising THEN 'loyalty_tier casing will be standardised' END, CASE WHEN chk_is_active_valid THEN 'is_active is not Y/N' END) AS dq_warnings
  FROM flagged
)
SELECT
  customer_id, full_name,
  CASE WHEN trim(email_raw) LIKE '%@%' THEN substr(split_part(trim(email_raw), '@', 1), 1, 2) || '***@' || split_part(trim(email_raw), '@', 2) ELSE '***' END AS email,
  CASE WHEN length(regexp_replace(coalesce(phone_raw, ''), '[^0-9]', '', 'g')) = 10 THEN substr(regexp_replace(coalesce(phone_raw, ''), '[^0-9]', '', 'g'), 1, 2) || '******' || substr(regexp_replace(coalesce(phone_raw, ''), '[^0-9]', '', 'g'), 9, 2) WHEN length(regexp_replace(coalesce(phone_raw, ''), '[^0-9]', '', 'g')) >= 4 THEN repeat('*', length(regexp_replace(coalesce(phone_raw, ''), '[^0-9]', '', 'g')) - 2) || substr(regexp_replace(coalesce(phone_raw, ''), '[^0-9]', '', 'g'), length(regexp_replace(coalesce(phone_raw, ''), '[^0-9]', '', 'g')) - 1, 2) ELSE '***' END AS phone,
  city, area, pincode, signup_date, loyalty_tier,
  (is_active_raw = 'Y') AS is_active,
  (failed_checks = '') AS dim_usable,
  chk_customer_id_not_null, chk_full_name_not_null, chk_city_not_null, chk_customer_id_unique, chk_email_valid, chk_phone_is_10_digits, chk_pincode_is_6_digits, chk_loyalty_tier_valid, chk_loyalty_tier_needs_standardising, chk_is_active_valid, chk_signup_date_valid, chk_signup_date_not_future,
  failed_checks, failure_reason, dq_warnings,
  CASE WHEN failed_checks = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
  _source_row, _source_file, _run_id
FROM scored;
