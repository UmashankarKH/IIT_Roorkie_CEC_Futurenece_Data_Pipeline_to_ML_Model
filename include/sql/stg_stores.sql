-- include/sql/stg_stores.sql
-- Staging layer for stores: standardisation, imputation, type casting

CREATE OR REPLACE TABLE stg_stores AS
SELECT
    store_id,
    store_name,
    COALESCE(zone, 'South') AS zone,
    zone AS zone_raw,
    area,
    pincode,
    TRY_CAST(open_date AS DATE) AS open_date,
    manager_name,
    CASE 
        WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 400
        ELSE TRY_CAST(capacity_orders_per_day AS INTEGER)
    END AS capacity_orders_per_day,
    capacity_orders_per_day AS capacity_raw,
    _loaded_at,
    _source_file,
    _run_id,
    CASE 
        WHEN zone IS NULL THEN 'WARNING'
        WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    CASE 
        WHEN zone IS NULL THEN 'IMPUTED_MISSING_ZONE'
        WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 'IMPUTED_ZERO_CAPACITY'
        ELSE NULL
    END AS failure_reason
FROM raw_stores;
