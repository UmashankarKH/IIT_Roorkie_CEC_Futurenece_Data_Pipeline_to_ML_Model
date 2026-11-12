-- include/sql/nammamart/stg_stores.sql

CREATE OR REPLACE TABLE stg_stores AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_stores
),
base AS (
    SELECT
        store_id,
        store_name,
        INITCAP(TRIM(zone))                            AS zone,
        area,
        pincode,
        CAST(open_date AS DATE)                        AS open_date,
        manager_name,
        CAST(capacity_orders_per_day AS INTEGER)       AS capacity_orders_per_day,
        _loaded_at,
        _source_file,
        _run_id
    FROM deduped
    WHERE rn = 1
),
checked AS (
    SELECT *,
        CASE
            WHEN zone IS NULL OR TRIM(zone) = '' THEN 'zone is null or empty'
            WHEN zone NOT IN ('North','South','East','West') THEN 'zone value not in allowed list'
            ELSE NULL
        END AS failure_reason
    FROM base
)
SELECT *,
    CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status
FROM checked;
