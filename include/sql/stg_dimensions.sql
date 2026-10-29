CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
 SELECT *, row_number() OVER (PARTITION BY trim(customer_id) ORDER BY _loaded_at DESC) rn
 FROM raw_customers WHERE nullif(trim(customer_id),'') IS NOT NULL
)
SELECT trim(customer_id) customer_id,
       CASE WHEN nullif(trim(email),'') IS NULL THEN NULL
            WHEN strpos(trim(email),'@') > 0 THEN substr(trim(email),1,1) || '***@' || lower(split_part(trim(email),'@',2))
            ELSE '***' END AS masked_email,
       CASE WHEN nullif(regexp_replace(coalesce(phone,''),'[^0-9]','','g'),'') IS NULL THEN NULL
            ELSE repeat('*', greatest(length(regexp_replace(phone,'[^0-9]','','g'))-4,0)) || right(regexp_replace(phone,'[^0-9]','','g'),4) END AS masked_phone,
       nullif(trim(city),'') city, nullif(trim(area),'') area, nullif(trim(pincode),'') pincode,
       try_cast(signup_date AS DATE) signup_date, lower(trim(loyalty_tier)) AS loyalty_tier,
       lower(trim(is_active)) is_active
FROM ranked WHERE rn=1;

CREATE OR REPLACE TABLE stg_products AS
WITH ranked AS (
 SELECT *, row_number() OVER (
   PARTITION BY trim(product_id)
   ORDER BY CASE WHEN lower(product_name) LIKE '%(old)%' THEN 1 ELSE 0 END, _loaded_at DESC
 ) rn
 FROM raw_products WHERE nullif(trim(product_id),'') IS NOT NULL
)
SELECT trim(product_id) product_id, nullif(trim(product_name),'') product_name,
       nullif(lower(trim(category)),'') category, nullif(trim(sub_category),'') sub_category,
       nullif(trim(brand),'') brand, try_cast(mrp AS DOUBLE) mrp, try_cast(cost_price AS DOUBLE) cost_price,
       nullif(trim(unit),'') unit, lower(trim(is_perishable)) is_perishable, try_cast(launch_date AS DATE) launch_date
FROM ranked WHERE rn=1;

CREATE OR REPLACE TABLE stg_stores AS
WITH ranked AS (
 SELECT *, row_number() OVER (PARTITION BY trim(store_id) ORDER BY _loaded_at DESC) rn
 FROM raw_stores WHERE nullif(trim(store_id),'') IS NOT NULL
)
SELECT trim(store_id) store_id, nullif(trim(store_name),'') store_name,
        nullif(lower(trim(zone)),'') AS zone,nullif(trim(area),'') AS area,
       nullif(trim(pincode),'') pincode, try_cast(open_date AS DATE) open_date,
       try_cast(capacity_orders_per_day AS INTEGER) capacity_orders_per_day
FROM ranked WHERE rn=1;
