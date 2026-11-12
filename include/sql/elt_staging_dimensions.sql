CREATE OR REPLACE TABLE stg_customers AS
SELECT DISTINCT customer_id,
       CASE WHEN strpos(email,'@')>0 THEN substr(email,1,2)||'***@'||split_part(email,'@',2) ELSE '***' END email_masked,
       CASE WHEN length(regexp_replace(phone,'[^0-9]','','g'))>=4 THEN substr(regexp_replace(phone,'[^0-9]','','g'),1,2)||'******'||right(regexp_replace(phone,'[^0-9]','','g'),2) ELSE '***' END phone_masked,
       city,area,pincode,signup_date,upper(trim(loyalty_tier)) loyalty_tier,is_active,
       CASE WHEN coalesce(trim(customer_id),'')='' THEN 'Missing customer_id'
            WHEN coalesce(trim(city),'')='' THEN 'Missing city'
            WHEN try_cast(signup_date AS DATE) IS NULL THEN 'Invalid signup_date'
            WHEN strpos(email,'@')=0 THEN 'Invalid email format'
            WHEN length(regexp_replace(phone,'[^0-9]','','g')) NOT IN (10,11,12) THEN 'Invalid phone format'
            ELSE NULL END failure_reason,
       CASE WHEN coalesce(trim(customer_id),'')='' OR coalesce(trim(city),'')='' OR try_cast(signup_date AS DATE) IS NULL THEN 'FAIL'
            WHEN strpos(email,'@')=0 OR length(regexp_replace(phone,'[^0-9]','','g')) NOT IN (10,11,12) THEN 'WARN'
            ELSE 'PASS' END dq_status
FROM raw_customers;

CREATE OR REPLACE TABLE stg_products AS
SELECT DISTINCT product_id,trim(product_name) product_name,trim(category) category,sub_category,brand,
       try_cast(mrp AS DOUBLE) mrp,try_cast(cost_price AS DOUBLE) cost_price,unit,is_perishable,launch_date,
       CASE WHEN coalesce(trim(product_id),'')='' THEN 'Missing product_id'
            WHEN coalesce(trim(category),'')='' THEN 'Missing category'
            WHEN try_cast(mrp AS DOUBLE) IS NULL OR try_cast(mrp AS DOUBLE)<=0 THEN 'Invalid MRP'
            WHEN try_cast(cost_price AS DOUBLE) IS NULL OR try_cast(cost_price AS DOUBLE)<=0 THEN 'Invalid cost price'
            WHEN try_cast(cost_price AS DOUBLE)>try_cast(mrp AS DOUBLE) THEN 'Cost price exceeds MRP'
            ELSE NULL END failure_reason,
       CASE WHEN coalesce(trim(product_id),'')='' OR coalesce(trim(category),'')=''
              OR try_cast(mrp AS DOUBLE) IS NULL OR try_cast(mrp AS DOUBLE)<=0
              OR try_cast(cost_price AS DOUBLE) IS NULL OR try_cast(cost_price AS DOUBLE)<=0
              OR try_cast(cost_price AS DOUBLE)>try_cast(mrp AS DOUBLE) THEN 'FAIL' ELSE 'PASS' END dq_status
FROM raw_products;

CREATE OR REPLACE TABLE stg_stores AS
SELECT DISTINCT store_id,trim(store_name) store_name,upper(trim("zone")) AS zone,area,pincode,open_date,manager_name,
       try_cast(capacity_orders_per_day AS INTEGER) capacity_orders_per_day,
       CASE WHEN coalesce(trim(store_id),'')='' THEN 'Missing store_id'
            WHEN coalesce(trim(store_name),'')='' THEN 'Missing store_name'
            WHEN coalesce(trim("zone"),'')='' THEN 'Missing zone'
            WHEN try_cast(capacity_orders_per_day AS INTEGER) IS NULL OR try_cast(capacity_orders_per_day AS INTEGER)<=0 THEN 'Invalid store capacity'
            ELSE NULL END failure_reason,
       CASE WHEN coalesce(trim(store_id),'')='' OR coalesce(trim(store_name),'')='' OR coalesce(trim("zone"),'')=''
              OR try_cast(capacity_orders_per_day AS INTEGER) IS NULL OR try_cast(capacity_orders_per_day AS INTEGER)<=0
            THEN 'FAIL' ELSE 'PASS' END dq_status
FROM raw_stores;

CREATE OR REPLACE TABLE stg_payments AS
SELECT payment_id,order_id,payment_ts,upper(trim(payment_mode)) payment_mode,
       try_cast(amount_paid AS DOUBLE) amount_paid,upper(trim(payment_status)) payment_status,gateway_ref,
       CASE WHEN coalesce(trim(payment_id),'')='' THEN 'Missing payment_id'
            WHEN coalesce(trim(order_id),'')='' THEN 'Missing order_id'
            WHEN try_cast(payment_ts AS TIMESTAMP) IS NULL THEN 'Invalid payment timestamp'
            WHEN try_cast(amount_paid AS DOUBLE) IS NULL OR try_cast(amount_paid AS DOUBLE)<0 THEN 'Invalid payment amount'
            WHEN upper(trim(payment_mode)) NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET') THEN 'Invalid payment mode'
            WHEN upper(trim(payment_status)) NOT IN ('SUCCESS','FAILED','PENDING') THEN 'Invalid payment status'
            ELSE NULL END failure_reason,
       CASE WHEN coalesce(trim(payment_id),'')='' OR coalesce(trim(order_id),'')=''
              OR try_cast(payment_ts AS TIMESTAMP) IS NULL
              OR try_cast(amount_paid AS DOUBLE) IS NULL OR try_cast(amount_paid AS DOUBLE)<0
              OR upper(trim(payment_mode)) NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET')
              OR upper(trim(payment_status)) NOT IN ('SUCCESS','FAILED','PENDING') THEN 'FAIL' ELSE 'PASS' END dq_status
FROM raw_payments;
