-- include/sql/staging/stg_customers_view.sql
-- ELT layer 2: STAGING (assignment Part B) - customers
--
-- SQL-only cleaning of the customer master:
--   * collapse identical re-loads, then keep the newest row per customer_id
--     (older conflicting copies stay, marked SUPERSEDED - never deleted)
--   * standardise loyalty_tier casing and city
--   * mask the personal data columns (name, email, phone) so no reporting
--     table can ever show them (Part E.4)
--   * apply the Part D quality rules
CREATE OR REPLACE VIEW v_stg_customers AS
WITH source_rows AS (
    SELECT customer_id, full_name, email, phone, city, area, pincode, signup_date,
           loyalty_tier, is_active,
           max(_loaded_at) AS _loaded_at, arg_max(_source_file, _loaded_at) AS _source_file,
           arg_max(_run_id, _loaded_at)      AS _run_id, min(_source_row_num) AS _source_row_num,
           count(*) AS _raw_copies
    FROM raw_customers
    GROUP BY customer_id, full_name, email, phone, city, area, pincode, signup_date,
             loyalty_tier, is_active
),
versioned AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(customer_id)
                              ORDER BY try_cast(signup_date AS DATE) DESC NULLS LAST,
                                       _loaded_at DESC, _source_row_num DESC) AS _version_rank
    FROM source_rows
),
typed AS (
    SELECT
        nullif(trim(customer_id), '')                                AS customer_id,
        nullif(trim(full_name), '')                                  AS full_name,
        nullif(trim(email), '')                                      AS email,
        nullif(trim(phone), '')                                      AS phone,
        upper(substr(nullif(trim(city), ''), 1, 1))
            || lower(substr(nullif(trim(city), ''), 2))              AS city,
        upper(substr(nullif(trim(area), ''), 1, 1))
            || lower(substr(nullif(trim(area), ''), 2))              AS area,
        nullif(trim(pincode), '')                                    AS pincode,
        try_cast(signup_date AS DATE)                                AS signup_date,
        upper(nullif(trim(loyalty_tier), ''))                        AS loyalty_tier,
        CASE upper(nullif(trim(is_active), ''))
             WHEN 'TRUE' THEN 'Y' WHEN 'YES' THEN 'Y' WHEN '1' THEN 'Y' WHEN 'Y' THEN 'Y'
             WHEN 'FALSE' THEN 'N' WHEN 'NO' THEN 'N' WHEN '0' THEN 'N' WHEN 'N' THEN 'N'
        END                                                          AS is_active,
        _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
    FROM versioned
),
checked AS (
    SELECT *,
        customer_id IS NULL                                          AS chk_rule_043,
        city IS NULL                                                 AS chk_rule_022,
        (email IS NOT NULL
         AND NOT regexp_matches(email, '^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$'))
                                                                     AS chk_rule_040,
        (phone IS NOT NULL AND length(regexp_replace(phone, '[^0-9]', '', 'g')) <> 10)
                                                                     AS chk_rule_041,
        (pincode IS NOT NULL AND length(regexp_replace(pincode, '[^0-9]', '', 'g')) <> 6)
                                                                     AS chk_rule_042,
        (loyalty_tier IS NOT NULL
         AND loyalty_tier NOT IN ('BRONZE', 'SILVER', 'GOLD', 'PLATINUM'))
                                                                     AS chk_rule_024,
        (signup_date IS NOT NULL AND signup_date > as_of()::DATE) AS chk_rule_023
    FROM typed
)
SELECT
    customer_id,
    -- PII masking, done once here so every downstream table is safe by default
    CASE WHEN full_name IS NULL THEN NULL
         WHEN position(' ' IN full_name) = 0 THEN full_name
         ELSE split_part(full_name, ' ', 1) || ' '
              || upper(substr(split_part(full_name, ' ', 2), 1, 1)) || '.'
    END                                                              AS customer_name_masked,
    CASE WHEN email IS NULL THEN NULL
         WHEN position('@' IN email) = 0 THEN 'INVALID_EMAIL'
         ELSE substr(email, 1, 2) || '***@' || split_part(email, '@', 2)
    END                                                              AS email_masked,
    CASE WHEN phone IS NULL THEN NULL
         WHEN length(regexp_replace(phone, '[^0-9]', '', 'g')) < 4
              THEN repeat('*', length(regexp_replace(phone, '[^0-9]', '', 'g')))
         ELSE substr(regexp_replace(phone, '[^0-9]', '', 'g'), 1, 2)
              || repeat('*', length(regexp_replace(phone, '[^0-9]', '', 'g')) - 4)
              || right(regexp_replace(phone, '[^0-9]', '', 'g'), 2)
    END                                                              AS phone_masked,
    coalesce(city, 'Unknown')                                        AS city,
    area, pincode, signup_date,
    coalesce(loyalty_tier, 'BRONZE')                                 AS loyalty_tier,
    coalesce(is_active, 'N')                                         AS is_active,
    chk_rule_043, chk_rule_022, chk_rule_040, chk_rule_041, chk_rule_042,
    chk_rule_024, chk_rule_023,
    CASE WHEN _version_rank > 1 THEN 'SUPERSEDED'
         WHEN chk_rule_043 THEN 'QUARANTINED'
         ELSE 'PASS' END                                             AS dq_status,
    concat_ws(' | ',
        CASE WHEN _version_rank > 1 THEN 'RULE-021: customer_id must be unique (superseded copy) [Uniqueness/Critical]' END,
        CASE WHEN chk_rule_043 THEN 'RULE-043: customer_id must be present [Completeness/Critical]' END
    )                                                                AS failure_reason,
    concat_ws(' | ',
        CASE WHEN chk_rule_022 THEN 'RULE-022: city should not be blank [Completeness/Warning]' END,
        CASE WHEN chk_rule_040 THEN 'RULE-040: email must look like name@domain.tld [Validity/Warning]' END,
        CASE WHEN chk_rule_041 THEN 'RULE-041: phone must contain 10 digits [Validity/Warning]' END,
        CASE WHEN chk_rule_042 THEN 'RULE-042: pincode must contain 6 digits [Validity/Warning]' END,
        CASE WHEN chk_rule_024 THEN 'RULE-024: loyalty_tier must be in the approved list [Validity/Warning]' END,
        CASE WHEN chk_rule_023 THEN 'RULE-023: signup_date must not be future-dated [Timeliness/Warning]' END
    )                                                                AS dq_warnings,
    _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
FROM checked;
