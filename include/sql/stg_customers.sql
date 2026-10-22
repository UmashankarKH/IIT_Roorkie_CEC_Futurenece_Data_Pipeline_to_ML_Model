CREATE OR REPLACE TABLE stg_customers AS
WITH batch_ranked AS (
    SELECT _source_file, _run_id, MAX(_loaded_at) AS batch_at,
           ROW_NUMBER() OVER (PARTITION BY _source_file ORDER BY MAX(_loaded_at) DESC) AS batch_rank
    FROM raw_customers
    GROUP BY _source_file, _run_id
), latest_batches AS (
    SELECT _source_file, _run_id FROM batch_ranked WHERE batch_rank = 1
), ranked AS (
    SELECT r.*,
           COUNT(*) OVER (PARTITION BY r.customer_id, r._source_file) AS source_key_count,
           ROW_NUMBER() OVER (PARTITION BY r.customer_id ORDER BY r._loaded_at ASC) AS key_rank
    FROM raw_customers r
    JOIN latest_batches b USING (_source_file, _run_id)
), typed AS (
    SELECT
        NULLIF(TRIM(customer_id), '') AS customer_id,
        CASE WHEN COALESCE(full_name, '') = '' THEN '' ELSE '***' END AS masked_full_name,
        CASE
            WHEN COALESCE(email, '') = '' THEN ''
            WHEN STRPOS(email, '@') = 0 THEN '***'
            ELSE (CASE WHEN LENGTH(SPLIT_PART(email, '@', 1)) > 2
                       THEN SUBSTR(SPLIT_PART(email, '@', 1), 1, 2)
                       ELSE SUBSTR(SPLIT_PART(email, '@', 1), 1, 1) END)
                 || '***@' || SPLIT_PART(email, '@', 2)
        END AS email,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g')) = 10
            THEN SUBSTR(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 1, 2) || '******'
                 || SUBSTR(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 9, 2)
            WHEN COALESCE(phone, '') = '' THEN ''
            ELSE '***MASKED***'
        END AS phone,
        NULLIF(TRIM(city), '') AS city,
        NULLIF(TRIM(area), '') AS area,
        NULLIF(TRIM(pincode), '') AS pincode,
        TRY_CAST(NULLIF(TRIM(signup_date), '') AS DATE) AS signup_date,
        CASE UPPER(TRIM(loyalty_tier))
            WHEN 'BRONZE' THEN 'Bronze' WHEN 'SILVER' THEN 'Silver'
            WHEN 'GOLD' THEN 'Gold' WHEN 'PLATINUM' THEN 'Platinum'
            ELSE TRIM(loyalty_tier)
        END AS loyalty_tier,
        CASE WHEN LOWER(TRIM(is_active)) IN ('y', 'yes', 'true', '1') THEN TRUE
             WHEN LOWER(TRIM(is_active)) IN ('n', 'no', 'false', '0') THEN FALSE
             ELSE NULL END AS is_active,
        source_key_count, _loaded_at, _source_file, _run_id,
        CASE WHEN COALESCE(email, '') = '' OR REGEXP_FULL_MATCH(TRIM(email), '[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+') THEN FALSE ELSE TRUE END AS bad_email,
        CASE WHEN COALESCE(phone, '') = '' OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) = 10 THEN FALSE ELSE TRUE END AS bad_phone
    FROM ranked
    WHERE key_rank = 1
), checked AS (
    SELECT *,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN customer_id IS NULL THEN 'customer_id is missing' END,
            CASE WHEN loyalty_tier IS NULL OR loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN 'invalid loyalty_tier' END,
            CASE WHEN signup_date IS NULL THEN 'signup_date is missing or invalid' END,
            CASE WHEN signup_date > CAST(TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP) AS DATE) THEN 'signup_date is in the future' END,
            CASE WHEN is_active IS NULL THEN 'is_active is not a recognised boolean' END
        ), '') AS critical_reason,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN source_key_count > 1 THEN 'duplicate customer_id deduplicated to first source row' END,
            CASE WHEN bad_email THEN 'email failed basic format check' END,
            CASE WHEN bad_phone THEN 'phone is not 10 digits' END,
            CASE WHEN pincode IS NOT NULL AND NOT REGEXP_FULL_MATCH(pincode, '[0-9]{6}') THEN 'pincode is not six digits' END
        ), '') AS warning_reason
    FROM typed
)
SELECT customer_id, masked_full_name, email, phone, city, area, pincode,
       signup_date, loyalty_tier, is_active,
       CASE WHEN critical_reason IS NOT NULL THEN 'FAIL'
            WHEN warning_reason IS NOT NULL THEN 'WARN' ELSE 'PASS' END AS dq_status,
       NULLIF(CONCAT_WS('; ', critical_reason, warning_reason), '') AS failure_reason,
       _source_file, _run_id, source_key_count AS source_row_count
FROM checked;
