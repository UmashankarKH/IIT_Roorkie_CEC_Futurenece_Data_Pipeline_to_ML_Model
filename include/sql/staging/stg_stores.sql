-- Staging stores. Same rules as store_checks() in include/nammamart/quality.py.
-- manager_name is PII too, so it is masked here and the raw column is not carried forward.

CREATE OR REPLACE TABLE stg_stores AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(store_id)
                              ORDER BY _loaded_at DESC, rowid DESC) AS dup_rank
    FROM raw_stores
),
typed AS (
    SELECT
        trim(store_id)                                   AS store_id,
        trim(store_name)                                 AS store_name,
        title_case(nullif(trim(zone), ''))               AS zone,
        title_case(trim(area))                           AS area,
        trim(pincode)                                    AS pincode,
        CAST(try_cast(open_date AS TIMESTAMP) AS DATE)    AS open_date,
        try_cast(capacity_orders_per_day AS INTEGER)     AS capacity_orders_per_day,
        trim(manager_name)                               AS manager_name,
        dup_rank, _run_id, _source_file, _loaded_at
    FROM ranked
),
masked AS (
    SELECT *, mask_name(manager_name) AS manager_name_masked
    FROM typed
),
flagged AS (
    SELECT *,
        list_filter([
            CASE WHEN store_id IS NULL OR store_id = ''
                 THEN 'Critical: store_id is missing' END,
            CASE WHEN dup_rank > 1
                 THEN 'Critical: duplicate store_id, later row kept' END
        ], lambda x: x IS NOT NULL) AS critical_reasons,
        list_filter([
            CASE WHEN capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0
                 THEN 'Warning: capacity_orders_per_day is zero or missing' END,
            CASE WHEN zone IS NULL THEN 'Warning: zone is missing' END
        ], lambda x: x IS NOT NULL) AS warning_reasons
    FROM masked
)
SELECT
    store_id, store_name, zone, area, pincode, open_date,
    capacity_orders_per_day, manager_name_masked,
    CASE WHEN len(critical_reasons) > 0 THEN 'REJECTED' ELSE 'PASSED' END AS dq_status,
    array_to_string(list_concat(critical_reasons, warning_reasons), ' | ') AS failure_reason,
    array_to_string(warning_reasons, ' | ')                               AS dq_warnings,
    _run_id, _source_file, _loaded_at
FROM flagged;
