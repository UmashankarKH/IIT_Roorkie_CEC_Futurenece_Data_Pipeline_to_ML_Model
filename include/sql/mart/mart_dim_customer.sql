-- Customer dimension. Only rows that passed the critical checks, and only the
-- masked columns: nothing downstream of raw can see an email or a phone number.

CREATE OR REPLACE TABLE mart_dim_customer AS
SELECT customer_id, customer_name_masked, email_masked, phone_masked,
       city, area, pincode, signup_date, loyalty_tier, is_active, dq_warnings
FROM stg_customers
WHERE dq_status = 'PASSED';
