-- Staging: customers. Personal data is masked here; full_name, raw email and raw phone stop at raw.
CREATE OR REPLACE TABLE stg_customers AS
WITH base AS (
    SELECT
        trim(customer_id)                             AS customer_id,
        trim(email)                                   AS email_raw,
        trim(phone)                                   AS phone_raw,
        regexp_replace(phone, '[^0-9]', '', 'g')      AS phone_digits,
        trim(city)                                    AS city,
        trim(area)                                    AS area,
        trim(pincode)                                 AS pincode,
        TRY_CAST(trim(signup_date) AS DATE)           AS signup_date,
        trim(loyalty_tier)                            AS tier_raw,
        trim(is_active)                               AS is_active,
        _source_file, _run_id, _loaded_at,
        row_number() OVER (PARTITION BY trim(customer_id) ORDER BY rowid) AS rn
    FROM raw_customers
    WHERE _run_id = '{{run_id}}'
),
checked AS (
    SELECT *,
        concat_ws(';',
            CASE WHEN customer_id = '' THEN 'customer_id_not_null' END,
            CASE WHEN customer_id <> '' AND rn > 1 THEN 'customer_id_unique' END,
            CASE WHEN signup_date > current_date THEN 'signup_date_not_future' END
        ) AS failure_reason,
        concat_ws(';',
            CASE WHEN NOT regexp_full_match(email_raw, '[\w.+\-]+@[\w\-]+(\.[\w\-]+)+') THEN 'email_format' END,
            CASE WHEN NOT regexp_full_match(phone_raw, '[0-9]{10}') THEN 'phone_10_digits' END,
            CASE WHEN NOT regexp_full_match(pincode, '[0-9]{6}') THEN 'pincode_6_digits' END,
            CASE WHEN tier_raw NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN 'loyalty_tier_casing' END
        ) AS warning_reason
    FROM base
)
SELECT
    customer_id,
    CASE WHEN strpos(email_raw, '@') = 0 THEN '***'
         ELSE left(split_part(email_raw, '@', 1), 2) || '***@' || substr(email_raw, strpos(email_raw, '@') + 1)
    END AS email_masked,
    CASE WHEN length(phone_digits) < 6 THEN '***'
         ELSE left(phone_digits, 2) || repeat('*', length(phone_digits) - 4) || right(phone_digits, 2)
    END AS phone_masked,
    city, area, pincode, signup_date,
    upper(left(tier_raw, 1)) || lower(substr(tier_raw, 2)) AS loyalty_tier,
    is_active,
    CASE WHEN failure_reason <> '' THEN 'REJECTED'
         WHEN warning_reason <> '' THEN 'WARNING'
         ELSE 'PASSED' END AS dq_status,
    failure_reason, warning_reason,
    _source_file, _run_id, _loaded_at
FROM checked;