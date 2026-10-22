CREATE OR REPLACE TABLE stg_stores AS
WITH batch_ranked AS (
    SELECT _source_file, _run_id, MAX(_loaded_at) AS batch_at,
           ROW_NUMBER() OVER (PARTITION BY _source_file ORDER BY MAX(_loaded_at) DESC) AS batch_rank
    FROM raw_stores
    GROUP BY _source_file, _run_id
), latest_batches AS (
    SELECT _source_file, _run_id FROM batch_ranked WHERE batch_rank = 1
), typed AS (
    SELECT
        NULLIF(TRIM(store_id), '') AS store_id,
        NULLIF(TRIM(store_name), '') AS store_name,
        UPPER(TRIM(zone)) AS zone,
        NULLIF(TRIM(area), '') AS area,
        NULLIF(TRIM(pincode), '') AS pincode,
        TRY_CAST(NULLIF(TRIM(open_date), '') AS DATE) AS open_date,
        TRY_CAST(NULLIF(TRIM(capacity_orders_per_day), '') AS INTEGER) AS capacity_orders_per_day,
        _source_file, _run_id
    FROM raw_stores r
    JOIN latest_batches b USING (_source_file, _run_id)
), checked AS (
    SELECT *,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN store_id IS NULL THEN 'store_id is missing' END,
            CASE WHEN zone IS NULL THEN 'zone is missing' END,
            CASE WHEN zone IS NOT NULL AND zone NOT IN ('North', 'South', 'East', 'West') THEN 'invalid zone' END,
            CASE WHEN open_date IS NULL THEN 'open_date is missing or invalid' END
        ), '') AS critical_reason,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN pincode IS NOT NULL AND NOT REGEXP_FULL_MATCH(pincode, '[0-9]{6}') THEN 'pincode is not six digits' END
        ), '') AS warning_reason
    FROM typed
)
SELECT store_id, store_name, zone, area, pincode, open_date,
       capacity_orders_per_day,
       CASE WHEN critical_reason IS NOT NULL THEN 'FAIL'
            WHEN warning_reason IS NOT NULL THEN 'WARN' ELSE 'PASS' END AS dq_status,
       NULLIF(CONCAT_WS('; ', critical_reason, warning_reason), '') AS failure_reason,
       _source_file, _run_id, 1::BIGINT AS source_row_count
FROM checked;
