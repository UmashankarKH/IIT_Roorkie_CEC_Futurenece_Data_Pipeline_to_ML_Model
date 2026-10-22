-- Staging for stores: trim, dedupe, type, apply quality rules. Placeholders: {run_id} {dag_id} {as_of}
DELETE FROM dq_results WHERE run_id = '{run_id}' AND dag_id = '{dag_id}' AND check_name LIKE 'stores.%';

CREATE OR REPLACE TEMP TABLE flags AS
WITH src AS (
    SELECT _row_id, _source_file,
           NULLIF(TRIM(store_id), '') AS store_id, NULLIF(TRIM(store_name), '') AS store_name,
           NULLIF(TRIM(zone), '') AS zone, NULLIF(TRIM(area), '') AS area, NULLIF(TRIM(pincode), '') AS pincode,
           CAST(TRY_STRPTIME(NULLIF(TRIM(open_date), ''), '%Y-%m-%d') AS DATE) AS open_dt,
           NULLIF(TRIM(manager_name), '') AS manager_name,
           TRY_CAST(NULLIF(TRIM(capacity_orders_per_day), '') AS DOUBLE) AS capacity_orders_per_day
    FROM raw_stores WHERE _run_id = '{run_id}'
)
SELECT *,
    store_id IS NULL AS f_store_id_present,
    store_id IS NOT NULL AND ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _row_id) > 1 AS f_store_id_unique,
    zone IS NULL AS f_zone_present,
    zone IS NOT NULL AND zone NOT IN ('North', 'South', 'East', 'West') AS f_zone_allowed,
    NOT COALESCE(capacity_orders_per_day > 0, FALSE) AS f_capacity_positive,
    open_dt IS NULL OR open_dt > DATE '{as_of}' AS f_open_date_not_future
FROM src;

CREATE OR REPLACE TABLE stg_stores AS
WITH s AS (
    SELECT *, (f_store_id_present OR f_store_id_unique) AS critical,
              (f_zone_present OR f_zone_allowed OR f_capacity_positive OR f_open_date_not_future) AS warn
    FROM flags
)
SELECT store_id, store_name, COALESCE(zone, 'UNKNOWN') AS zone, area, pincode, manager_name,
       CAST(capacity_orders_per_day AS BIGINT) AS capacity_orders_per_day,
       CASE WHEN critical THEN 'FAILED' WHEN warn THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_store_id_present THEN 'store_id missing' END,
           CASE WHEN f_store_id_unique THEN 'duplicate store_id' END), '') AS failure_reason,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_zone_present THEN 'zone missing' END,
           CASE WHEN f_zone_allowed THEN 'zone not in allowed list' END,
           CASE WHEN f_capacity_positive THEN 'capacity not positive' END,
           CASE WHEN f_open_date_not_future THEN 'open_date invalid or in the future' END), '') AS warning_reason,
       _row_id, _source_file
FROM s;

INSERT INTO dq_results
WITH agg AS (
    SELECT 'stores' AS dataset, COUNT(*) AS n,
        COUNT(*) FILTER (WHERE f_store_id_present) AS store_id_present,
        COUNT(*) FILTER (WHERE f_store_id_unique) AS store_id_unique,
        COUNT(*) FILTER (WHERE f_zone_present) AS zone_present,
        COUNT(*) FILTER (WHERE f_zone_allowed) AS zone_allowed,
        COUNT(*) FILTER (WHERE f_capacity_positive) AS capacity_positive,
        COUNT(*) FILTER (WHERE f_open_date_not_future) AS open_date_not_future
    FROM flags
), u AS (UNPIVOT agg ON COLUMNS(* EXCLUDE (dataset, n)) INTO NAME check_name VALUE failed),
meta(check_name, dimension, severity) AS (VALUES
    ('store_id_present', 'Completeness', 'Critical'), ('store_id_unique', 'Uniqueness', 'Critical'),
    ('zone_present', 'Completeness', 'Warning'), ('zone_allowed', 'Validity', 'Warning'),
    ('capacity_positive', 'Accuracy', 'Warning'), ('open_date_not_future', 'Timeliness', 'Warning'))
SELECT '{run_id}', '{dag_id}', u.dataset || '.' || u.check_name, m.dimension, m.severity, u.n, u.failed,
       CASE WHEN u.failed = 0 THEN 'PASS' WHEN m.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       TIMEZONE('Asia/Kolkata', NOW())
FROM u JOIN meta m USING (check_name);
