-- Personal data (full_name, email, phone) is still clear text here: this table is internal
-- and replaced every run. stg_customers (the table people query) holds masked values only.
CREATE OR REPLACE TABLE stg_customers_std AS
SELECT
    _row,
    nm_text(customer_id)    AS customer_id,
    nm_text(full_name)      AS full_name,
    nm_text(email)          AS email,
    nm_text(phone)          AS phone,
    nm_phone_norm(phone)    AS phone_norm,
    nm_text(city)           AS city,
    nm_text(area)           AS area,
    nm_text(pincode)        AS pincode,
    nm_ts(signup_date)      AS signup_date,
    nm_raw(signup_date)     AS signup_date_raw,
    nm_tier(loyalty_tier)   AS loyalty_tier,
    nm_raw(loyalty_tier)    AS loyalty_tier_raw,
    nm_text(is_active)      AS is_active,
    _source_file, _run_id, _loaded_at
FROM raw_customers
WHERE _run_id = '{{ run_id }}';
