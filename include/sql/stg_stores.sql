-- Staging: stores. Cast, standardise, dedupe, apply quality rules. No rows are deleted.
CREATE OR REPLACE TABLE stg_stores AS
WITH base AS (
    SELECT
        trim(store_id)                                   AS store_id,
        trim(store_name)                                 AS store_name,
        upper(left(trim(zone), 1)) || lower(substr(trim(zone), 2)) AS zone,
        trim(area)                                       AS area,
        trim(pincode)                                    AS pincode,
        TRY_CAST(trim(open_date) AS DATE)                AS open_date,
        trim(manager_name)                               AS manager_name,
        TRY_CAST(trim(capacity_orders_per_day) AS INTEGER) AS capacity_orders_per_day,
        _source_file, _run_id, _loaded_at,
        row_number() OVER (PARTITION BY trim(store_id) ORDER BY rowid) AS rn
    FROM raw_stores
    WHERE _run_id = '{{run_id}}'
),
checked AS (
    SELECT *,
        -- Critical rules: the row is REJECTED
        concat_ws(';',
            CASE WHEN store_id = '' THEN 'store_id_not_null' END,
            CASE WHEN store_id <> '' AND rn > 1 THEN 'store_id_unique' END,
            CASE WHEN zone NOT IN ('North', 'South', 'East', 'West') THEN 'zone_allowed' END
        ) AS failure_reason,
        -- Warning rules: the row still loads, but is flagged
        concat_ws(';',
            CASE WHEN NOT regexp_full_match(pincode, '[0-9]{6}') THEN 'pincode_6_digits' END,
            CASE WHEN capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0
                 THEN 'capacity_positive' END
        ) AS warning_reason
    FROM base
)
SELECT
    store_id, store_name, zone, area, pincode, open_date, manager_name,
    capacity_orders_per_day,
    CASE WHEN failure_reason <> '' THEN 'REJECTED'
         WHEN warning_reason <> '' THEN 'WARNING'
         ELSE 'PASSED' END AS dq_status,
    failure_reason, warning_reason,
    _source_file, _run_id, _loaded_at
FROM checked;