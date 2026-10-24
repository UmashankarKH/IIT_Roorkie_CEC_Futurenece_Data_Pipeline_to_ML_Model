-- Cast customer fields, mask PII and retain a row-level quality outcome.
CREATE OR REPLACE TABLE stg_customers AS
SELECT
    customer_id,
    full_name,
    -- Same rule as masking.mask_email: keep 2 characters of the local part
    -- (1 if it is that short) and the domain; no '@' means fully masked.
    CASE WHEN strpos(trim(email), '@') > 0
         THEN substr(trim(email), 1, CASE WHEN strpos(trim(email), '@') - 1 > 2 THEN 2 ELSE 1 END)
              || '***@' || substr(trim(email), strpos(trim(email), '@') + 1)
         ELSE '***' END AS email_masked,
    CASE WHEN length(regexp_replace(phone, '[^0-9]', '', 'g')) >= 4
         THEN substr(regexp_replace(phone, '[^0-9]', '', 'g'), 1, 2)
              || repeat('*', greatest(length(regexp_replace(phone, '[^0-9]', '', 'g')) - 4, 0))
              || right(regexp_replace(phone, '[^0-9]', '', 'g'), 2)
         ELSE '***' END AS phone_masked,
    city,
    area,
    pincode,
    try_cast(signup_date AS DATE) AS signup_date,
    -- One casing for every tier (Bronze, Silver, Gold, Platinum), as in the ETL.
    upper(substr(trim(loyalty_tier), 1, 1)) || lower(substr(trim(loyalty_tier), 2)) AS loyalty_tier,
    is_active,
    CASE WHEN customer_id IS NOT NULL AND customer_id <> '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    CASE WHEN customer_id IS NOT NULL AND customer_id <> '' THEN NULL ELSE 'Missing customer_id' END AS failure_reason
-- raw_customers lives in the restricted raw-PII file, attached as ``pii``.
FROM pii.raw_customers
-- rowid preserves CSV order, so the first source row wins exactly as in the ETL.
QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY _loaded_at, rowid) = 1;
