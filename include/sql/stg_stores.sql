-- include/sql/stg_stores.sql
CREATE OR REPLACE TABLE stg_stores AS
WITH latest_raw AS (
    SELECT *
    FROM raw_stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _source_file, _row_id ORDER BY _loaded_at DESC) = 1
),
ranked AS (
    SELECT
        TRIM(store_id) AS store_id,
        TRIM(store_name) AS store_name,
        CASE
            WHEN TRIM(COALESCE(zone, '')) = '' AND area = 'BTM Layout' THEN 'South'
            ELSE TRIM(COALESCE(zone, ''))
        END AS zone,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(open_date AS DATE) AS open_date,
        TRIM(manager_name) AS manager_name,
        TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(store_id) ORDER BY _row_id) AS row_num
    FROM latest_raw
)
SELECT
    store_id,
    store_name,
    zone,
    area,
    pincode,
    open_date,
    manager_name,
    capacity_orders_per_day,
    CASE
        WHEN row_num > 1 THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CASE
        WHEN row_num > 1 THEN 'uniqueness:duplicate_store_id'
        WHEN zone = '' THEN 'completeness:null_zone'
        WHEN capacity_orders_per_day <= 0 THEN 'accuracy:capacity_le_zero'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _run_id
FROM ranked;
