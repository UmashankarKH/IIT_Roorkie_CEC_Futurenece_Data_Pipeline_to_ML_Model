-- Staging for customers: trim, dedupe, type, mask PII, apply quality rules. Placeholders: {run_id} {dag_id} {as_of}
DELETE FROM dq_results WHERE run_id = '{run_id}' AND dag_id = '{dag_id}' AND check_name LIKE 'customers.%';

CREATE OR REPLACE TEMP TABLE flags AS
WITH src AS (
    SELECT _row_id, _source_file,
           NULLIF(TRIM(customer_id), '') AS customer_id, NULLIF(TRIM(full_name), '') AS full_name,
           NULLIF(TRIM(email), '') AS email, NULLIF(TRIM(phone), '') AS phone, NULLIF(TRIM(city), '') AS city,
           NULLIF(TRIM(area), '') AS area, NULLIF(TRIM(pincode), '') AS pincode,
           NULLIF(TRIM(signup_date), '') AS signup_date, NULLIF(TRIM(loyalty_tier), '') AS loyalty_tier,
           NULLIF(TRIM(is_active), '') AS is_active
    FROM raw_customers WHERE _run_id = '{run_id}'
), typed AS (
    SELECT *, CAST(TRY_STRPTIME(signup_date, '%Y-%m-%d') AS DATE) AS signup_dt FROM src
)
SELECT *,
    customer_id IS NULL AS f_customer_id_present,
    customer_id IS NOT NULL AND ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _row_id) > 1 AS f_customer_id_unique,
    signup_dt IS NULL AS f_signup_date_valid,
    COALESCE(signup_dt > DATE '{as_of}', FALSE) AS f_signup_date_not_future,
    NOT REGEXP_FULL_MATCH(COALESCE(email, ''), '[^@\s]+@[^@\s]+\.[^@\s]+') AS f_email_format,
    NOT REGEXP_FULL_MATCH(COALESCE(phone, ''), '\d{10}') AS f_phone_10_digits,
    NOT REGEXP_FULL_MATCH(COALESCE(pincode, ''), '\d{6}') AS f_pincode_6_digits,
    NOT COALESCE(loyalty_tier IN ('Bronze', 'Silver', 'Gold', 'Platinum'), FALSE) AS f_loyalty_tier_casing,
    city IS NULL AS f_city_present
FROM typed;

CREATE OR REPLACE TABLE stg_customers AS
WITH s AS (
    SELECT *, REGEXP_REPLACE(COALESCE(phone, ''), '\D', '', 'g') AS phone_digits,
           (f_customer_id_present OR f_customer_id_unique OR f_signup_date_valid OR f_signup_date_not_future) AS critical,
           (f_email_format OR f_phone_10_digits OR f_pincode_6_digits OR f_loyalty_tier_casing OR f_city_present) AS warn
    FROM flags
)
SELECT customer_id, full_name,
       CASE WHEN email LIKE '%@%' THEN LEFT(SPLIT_PART(email, '@', 1), 2) || '***@' || SUBSTR(email, INSTR(email, '@') + 1) END AS email_masked,
       CASE WHEN LENGTH(phone_digits) = 10 THEN LEFT(phone_digits, 2) || '******' || RIGHT(phone_digits, 2) END AS phone_masked,
       city, area, pincode, signup_dt AS signup_date,
       UPPER(LEFT(loyalty_tier, 1)) || LOWER(SUBSTR(loyalty_tier, 2)) AS loyalty_tier, is_active,
       CASE WHEN critical THEN 'FAILED' WHEN warn THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_customer_id_present THEN 'customer_id missing' END,
           CASE WHEN f_customer_id_unique THEN 'duplicate customer_id' END,
           CASE WHEN f_signup_date_valid THEN 'signup_date not a valid date' END,
           CASE WHEN f_signup_date_not_future THEN 'signup_date in the future' END), '') AS failure_reason,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_email_format THEN 'invalid email' END,
           CASE WHEN f_phone_10_digits THEN 'invalid phone' END,
           CASE WHEN f_pincode_6_digits THEN 'invalid pincode' END,
           CASE WHEN f_loyalty_tier_casing THEN 'loyalty_tier casing' END,
           CASE WHEN f_city_present THEN 'city missing' END), '') AS warning_reason,
       _row_id, _source_file
FROM s;

INSERT INTO dq_results
WITH agg AS (
    SELECT 'customers' AS dataset, COUNT(*) AS n,
        COUNT(*) FILTER (WHERE f_customer_id_present) AS customer_id_present,
        COUNT(*) FILTER (WHERE f_customer_id_unique) AS customer_id_unique,
        COUNT(*) FILTER (WHERE f_signup_date_valid) AS signup_date_valid,
        COUNT(*) FILTER (WHERE f_signup_date_not_future) AS signup_date_not_future,
        COUNT(*) FILTER (WHERE f_email_format) AS email_format,
        COUNT(*) FILTER (WHERE f_phone_10_digits) AS phone_10_digits,
        COUNT(*) FILTER (WHERE f_pincode_6_digits) AS pincode_6_digits,
        COUNT(*) FILTER (WHERE f_loyalty_tier_casing) AS loyalty_tier_casing,
        COUNT(*) FILTER (WHERE f_city_present) AS city_present
    FROM flags
), u AS (UNPIVOT agg ON COLUMNS(* EXCLUDE (dataset, n)) INTO NAME check_name VALUE failed),
meta(check_name, dimension, severity) AS (VALUES
    ('customer_id_present', 'Completeness', 'Critical'), ('customer_id_unique', 'Uniqueness', 'Critical'),
    ('signup_date_valid', 'Validity', 'Critical'), ('signup_date_not_future', 'Timeliness', 'Critical'),
    ('email_format', 'Validity', 'Warning'), ('phone_10_digits', 'Validity', 'Warning'),
    ('pincode_6_digits', 'Validity', 'Warning'), ('loyalty_tier_casing', 'Consistency', 'Warning'),
    ('city_present', 'Completeness', 'Warning'))
SELECT '{run_id}', '{dag_id}', u.dataset || '.' || u.check_name, m.dimension, m.severity, u.n, u.failed,
       CASE WHEN u.failed = 0 THEN 'PASS' WHEN m.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       TIMEZONE('Asia/Kolkata', NOW())
FROM u JOIN meta m USING (check_name);
