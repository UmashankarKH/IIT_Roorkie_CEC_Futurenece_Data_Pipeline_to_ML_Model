-- Staging: stores received in THIS run.
-- Casts types, fills a missing zone with 'Unknown', and records every quality check:
--   dq_failed   = critical checks that failed  -> row is not used by the marts
--   dq_warnings = warning checks that failed   -> row is used, problem is logged

CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
    SELECT
        *,
        try_strptime(trim(open_date), '%Y-%m-%d') AS open_date_parsed,
        try_cast(trim(capacity_orders_per_day) AS INTEGER) AS capacity,
        CAST(getvariable('base_business_date') AS DATE) + INTERVAL 1 DAY + INTERVAL 9 HOUR AS as_of,
        row_number() OVER (PARTITION BY store_id ORDER BY _row_num) AS dup_rank
    FROM raw_stores
    WHERE _run_id = getvariable('run_id')
),

checked AS (
    SELECT
        *,
        list_filter([
            CASE WHEN coalesce(trim(store_id), '') = '' THEN 'store_id_not_null' END,
            CASE WHEN dup_rank > 1 THEN 'store_id_unique' END,
            CASE WHEN trim(zone) <> '' AND trim(zone) NOT IN ('North', 'South', 'East', 'West') THEN 'zone_valid' END,
            CASE WHEN open_date_parsed IS NULL OR open_date_parsed > as_of THEN 'open_date_not_future' END
        ], x -> x IS NOT NULL) AS dq_failed,
        list_filter([
            CASE WHEN coalesce(trim(zone), '') = '' THEN 'zone_present' END,
            CASE WHEN NOT regexp_full_match(coalesce(trim(pincode), ''), '\d{6}') THEN 'pincode_6_digits' END,
            CASE WHEN capacity IS NULL OR capacity <= 0 THEN 'capacity_positive' END
        ], x -> x IS NOT NULL) AS dq_warnings
    FROM src
)

SELECT
    store_id,
    store_name,
    coalesce(nullif(trim(zone), ''), 'Unknown') AS zone,
    area,
    pincode,
    CAST(open_date_parsed AS DATE) AS open_date,
    manager_name,
    capacity AS capacity_orders_per_day,
    CASE WHEN len(dq_failed) = 0 THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    array_to_string(dq_failed, '; ') AS failure_reason,
    array_to_string(dq_warnings, '; ') AS warnings,
    dq_failed,
    dq_warnings,
    _row_num,
    _source_file,
    _run_id
FROM checked;
