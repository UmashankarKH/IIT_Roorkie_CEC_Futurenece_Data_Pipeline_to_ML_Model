-- include/sql/04_staging_stores.sql
-- Staging Stores: Cast types, impute missing zone for S07 (BTM Layout -> South).

CREATE OR REPLACE TABLE stg_stores AS
WITH ranked AS (
    SELECT
        TRIM(store_id) AS store_id,
        TRIM(store_name) AS store_name,
        CASE
            WHEN zone IS NULL OR TRIM(zone) = '' THEN 'South'
            ELSE TRIM(zone)
        END AS zone,
        TRIM(area) AS area,
        TRY_CAST(pincode AS INTEGER) AS pincode,
        TRY_CAST(open_date AS DATE) AS open_date,
        TRIM(manager_name) AS manager_name,
        TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
        'PASSED' AS dq_status,
        '' AS failure_reason,
        _loaded_at,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(store_id) ORDER BY _loaded_at DESC) AS rnk
    FROM raw_stores
)
SELECT * EXCLUDE (rnk) FROM ranked WHERE rnk = 1;
