-- stg_stores: typed, standardised store master with DQ flags (f_* = check FAILED).
CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
    SELECT * FROM raw_stores WHERE _run_id = {{run_id}}
),
typed AS (
    SELECT
        nullif(trim(store_id), '')                         AS store_id,
        trim(store_name)                                   AS store_name,
        nullif(trim(zone), '')                             AS zone_raw,
        trim(area)                                         AS area,
        trim(pincode)                                      AS pincode,
        try_strptime(trim(open_date), '%Y-%m-%d')::DATE    AS open_date,
        trim(manager_name)                                 AS manager_name,
        try_cast(trim(capacity_orders_per_day) AS DOUBLE)  AS capacity_orders_per_day,
        _row_num, _source_file, _run_id
    FROM src
),
flags AS (
    SELECT *,
        store_id IS NOT NULL
            AND row_number() OVER (PARTITION BY store_id ORDER BY _row_num) > 1   AS f_store_id_unique,
        zone_raw IS NULL                                                         AS f_store_zone_present,
        NOT coalesce(regexp_full_match(pincode, '\d{6}'), false)                AS f_store_pincode_valid,
        NOT coalesce(capacity_orders_per_day > 0, false)                         AS f_store_capacity_positive
    FROM typed
)
SELECT
    store_id, store_name, coalesce(zone_raw, 'Unknown') AS zone, area, pincode, open_date,
    manager_name, capacity_orders_per_day,
    f_store_id_unique, f_store_zone_present, f_store_pincode_valid, f_store_capacity_positive,
    nullif(concat_ws('; ',
        CASE WHEN f_store_id_unique THEN 'duplicate store_id' END), '')            AS failure_reason,
    nullif(concat_ws('; ',
        CASE WHEN f_store_zone_present THEN 'zone is blank (set to Unknown)' END,
        CASE WHEN f_store_pincode_valid THEN 'pincode is not 6 digits' END,
        CASE WHEN f_store_capacity_positive THEN 'capacity_orders_per_day <= 0' END), '') AS dq_warnings,
    CASE WHEN f_store_id_unique THEN 'FAILED' ELSE 'PASSED' END                    AS dq_status,
    _row_num, _source_file, _run_id
FROM flags;
