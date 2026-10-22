-- Bonus: SCD Type 2 customer dimension. One row per version of a customer.
-- Tracked (Type 2, a change opens a new version): loyalty_tier, area, pincode, is_active
-- Overwritten in place (Type 1): customer_name, email_masked, phone_masked, city
-- valid_from / valid_to are load times; the current version has valid_to = 9999-12-31.
-- Re-running on the same file changes nothing.

CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
    customer_sk    BIGINT,      -- surrogate key, one per version
    customer_id    VARCHAR,
    customer_name  VARCHAR,
    email_masked   VARCHAR,
    phone_masked   VARCHAR,
    city           VARCHAR,
    area           VARCHAR,
    pincode        VARCHAR,
    loyalty_tier   VARCHAR,
    is_active      BOOLEAN,
    valid_from     TIMESTAMP,
    valid_to       TIMESTAMP,
    is_current     BOOLEAN,
    _run_id        VARCHAR
);

CREATE OR REPLACE TEMP TABLE scd_incoming AS
SELECT customer_id, customer_name, email_masked, phone_masked, city, area, pincode, loyalty_tier, is_active
FROM stg_customers
WHERE dq_status = 'PASSED';

-- 1. Close the current version of every customer whose tracked attributes changed
UPDATE dim_customer_scd2 AS d
SET valid_to = now()::TIMESTAMP, is_current = false
FROM scd_incoming AS i
WHERE d.customer_id = i.customer_id
  AND d.is_current
  AND (d.loyalty_tier IS DISTINCT FROM i.loyalty_tier
       OR d.area IS DISTINCT FROM i.area
       OR d.pincode IS DISTINCT FROM i.pincode
       OR d.is_active IS DISTINCT FROM i.is_active);

-- 2. Type 1 columns: just keep the current version up to date
UPDATE dim_customer_scd2 AS d
SET customer_name = i.customer_name, email_masked = i.email_masked, phone_masked = i.phone_masked, city = i.city
FROM scd_incoming AS i
WHERE d.customer_id = i.customer_id AND d.is_current;

-- 3. Open a new version for new customers and for the ones closed in step 1
INSERT INTO dim_customer_scd2
SELECT
    (SELECT coalesce(max(customer_sk), 0) FROM dim_customer_scd2) + row_number() OVER (ORDER BY i.customer_id),
    i.customer_id, i.customer_name, i.email_masked, i.phone_masked, i.city, i.area, i.pincode,
    i.loyalty_tier, i.is_active,
    now()::TIMESTAMP,
    TIMESTAMP '9999-12-31 00:00:00',
    true,
    getvariable('run_id')
FROM scd_incoming AS i
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer_scd2 AS d WHERE d.customer_id = i.customer_id AND d.is_current
);
