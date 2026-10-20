CREATE OR REPLACE TABLE stg_stores AS
WITH t AS (SELECT *, TRY_CAST(NULLIF(TRIM(capacity_orders_per_day),'') AS INTEGER) capacity_num FROM raw_stores),
c AS (SELECT *, CASE WHEN COALESCE(TRIM(store_id),'')='' THEN 'missing_store_id'
 WHEN COALESCE(TRIM(store_name),'')='' THEN 'missing_store_name'
 WHEN COALESCE(TRIM(zone),'')='' THEN 'missing_zone' ELSE NULL END reject_reason FROM t)
SELECT *, capacity_num AS capacity_orders_per_day_clean,
 (reject_reason IS NULL) AS is_valid FROM c;
