-- Store dimension. Manager names are already masked in staging.

CREATE OR REPLACE TABLE mart_dim_store AS
SELECT store_id, store_name, zone, area, pincode, open_date,
       capacity_orders_per_day, manager_name_masked, dq_warnings
FROM stg_stores
WHERE dq_status = 'PASSED';
