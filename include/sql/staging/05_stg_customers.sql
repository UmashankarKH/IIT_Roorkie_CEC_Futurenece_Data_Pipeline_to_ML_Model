-- Customer staging: every row kept (bad ones flagged), personal data masked, attributes repaired.
-- Repairs match build_dim_customer() in the ETL so both pipelines report the same values.
CREATE OR REPLACE TABLE stg_customers AS
SELECT
    customer_id,
    nm_mask_name(full_name)     AS customer_name_masked,
    nm_mask_email(email)        AS email_masked,
    nm_mask_phone(phone_norm)   AS phone_masked,
    city, area, pincode,
    CAST(CASE WHEN signup_date < TIMESTAMP '{{ next_day }}' THEN signup_date END AS DATE) AS signup_date, -- future -> NULL (DI-28)
    coalesce(loyalty_tier, 'UNKNOWN') AS loyalty_tier,
    nm_bool(is_active)          AS is_active,
    dq_status, failure_reason, warning_reason,
    _row AS _source_row, _source_file, _run_id, _loaded_at
FROM stg_customers_flagged;
