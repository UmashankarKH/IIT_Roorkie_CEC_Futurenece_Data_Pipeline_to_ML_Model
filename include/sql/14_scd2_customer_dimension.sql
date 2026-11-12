-- include/sql/14_scd2_customer_dimension.sql
-- Bonus: Slowly Changing Dimension (SCD) Type 2 for Customers
-- Tracks customer loyalty tier changes over time with effective dates and current flag.

CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
    customer_sk BIGINT,
    customer_id VARCHAR,
    full_name VARCHAR,
    masked_email VARCHAR,
    masked_phone VARCHAR,
    city VARCHAR,
    area VARCHAR,
    pincode VARCHAR,
    loyalty_tier VARCHAR,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE dim_customer_scd2 AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id, signup_date) AS customer_sk,
    customer_id,
    full_name,
    masked_email,
    masked_phone,
    city,
    area,
    pincode,
    loyalty_tier,
    signup_date AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current
FROM stg_customers;
