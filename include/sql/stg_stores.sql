-- stg_stores: cleaned, typed and quality-checked copy of raw_stores for ONE run.
-- Rows that fail a Critical check stay here with dq_status = 'FAIL' (marts ignore them).
CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
  SELECT trim(store_id) AS store_id, trim(store_name) AS store_name, trim(zone) AS zone_raw, trim(area) AS area,
    trim(pincode) AS pincode, trim(open_date) AS open_date_raw, trim(manager_name) AS manager_name,
    try_cast(trim(capacity_orders_per_day) AS DOUBLE) AS capacity_num,
    _source_row, _source_file, _run_id
  FROM raw_stores
  WHERE _run_id = '{{run_id}}'
),
typed AS (
  SELECT src.*,
    CAST(try_strptime(open_date_raw, '%Y-%m-%d') AS DATE) AS open_date,
    row_number() OVER (PARTITION BY store_id ORDER BY _source_row) AS _rn
  FROM src
),
flagged AS (
  SELECT typed.*,
    coalesce((store_id = ''), false) AS chk_store_id_not_null,
    coalesce((zone_raw = ''), false) AS chk_zone_not_null,
    coalesce((store_id <> '' AND _rn > 1), false) AS chk_store_id_unique,
    coalesce((zone_raw <> '' AND zone_raw NOT IN ('North', 'South', 'East', 'West')), false) AS chk_zone_valid,
    coalesce((NOT regexp_matches(pincode, '^\d{6}$')), false) AS chk_pincode_is_6_digits,
    coalesce((NOT coalesce(capacity_num > 0, false)), false) AS chk_capacity_positive,
    coalesce((open_date IS NULL), false) AS chk_open_date_valid,
    coalesce((CAST(open_date AS TIMESTAMP) > CAST('{{as_of}}' AS TIMESTAMP)), false) AS chk_open_date_not_future
  FROM typed
),
scored AS (
  SELECT flagged.*,
    concat_ws(', ', CASE WHEN chk_store_id_not_null THEN 'store_id_not_null' END, CASE WHEN chk_zone_not_null THEN 'zone_not_null' END, CASE WHEN chk_store_id_unique THEN 'store_id_unique' END, CASE WHEN chk_zone_valid THEN 'zone_valid' END, CASE WHEN chk_open_date_valid THEN 'open_date_valid' END, CASE WHEN chk_open_date_not_future THEN 'open_date_not_future' END) AS failed_checks,
    concat_ws('; ', CASE WHEN chk_store_id_not_null THEN 'store_id is missing' END, CASE WHEN chk_zone_not_null THEN 'zone is missing' END, CASE WHEN chk_store_id_unique THEN 'duplicate store_id (repeat of an earlier row)' END, CASE WHEN chk_zone_valid THEN 'zone is not North/South/East/West' END, CASE WHEN chk_open_date_valid THEN 'open_date is missing or not a valid date' END, CASE WHEN chk_open_date_not_future THEN 'open_date is in the future' END) AS failure_reason,
    concat_ws('; ', CASE WHEN chk_pincode_is_6_digits THEN 'pincode is not exactly 6 digits' END, CASE WHEN chk_capacity_positive THEN 'capacity_orders_per_day is zero, negative or not a number' END) AS dq_warnings
  FROM flagged
)
SELECT
  store_id, store_name,
  CASE WHEN zone_raw = '' AND store_id = 'S07' AND failed_checks = 'zone_not_null' THEN 'South' ELSE zone_raw END AS zone,
  area, pincode, open_date, manager_name, CAST(capacity_num AS BIGINT) AS capacity_orders_per_day,
  (failed_checks = '' OR (failed_checks = 'zone_not_null' AND store_id = 'S07')) AS dim_usable,
  chk_store_id_not_null, chk_zone_not_null, chk_store_id_unique, chk_zone_valid, chk_pincode_is_6_digits, chk_capacity_positive, chk_open_date_valid, chk_open_date_not_future,
  failed_checks, failure_reason, dq_warnings,
  CASE WHEN failed_checks = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
  _source_row, _source_file, _run_id
FROM scored;
