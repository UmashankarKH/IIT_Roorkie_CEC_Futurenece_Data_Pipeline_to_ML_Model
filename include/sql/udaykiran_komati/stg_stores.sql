CREATE OR REPLACE TABLE stg_stores AS
WITH latest AS (
    SELECT *,
           row_number() OVER (PARTITION BY store_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_stores
)
SELECT
    store_id,
    trim(store_name) AS store_name,
    coalesce(nullif(trim(zone), ''), trim(area)) AS zone,
    trim(area) AS area,
    trim(pincode) AS pincode,
    open_date,
    trim(manager_name) AS manager_name,
    try_cast(capacity_orders_per_day AS DOUBLE) AS capacity_orders_per_day,
    CASE
        WHEN store_id IS NULL OR trim(store_id) = '' THEN 'FAIL'
        WHEN try_cast(capacity_orders_per_day AS DOUBLE) < 0 THEN 'WARN'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN store_id IS NULL OR trim(store_id) = '' THEN 'missing store_id'
        WHEN try_cast(capacity_orders_per_day AS DOUBLE) < 0 THEN 'negative capacity'
        ELSE NULL
    END AS failure_reason
FROM latest
WHERE rn = 1;
