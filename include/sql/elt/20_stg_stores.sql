-- Staging: stores. SQL only. dq_status = FAIL (critical rule broken) / WARN (warning rule broken) / PASS.
CREATE OR REPLACE TABLE stg_stores AS
WITH std AS (
    SELECT _row_num, _source_file, _run_id,
           trim(store_id) AS store_id, trim(store_name) AS store_name, trim(zone) AS zone, trim(area) AS area,
           trim(pincode) AS pincode, try_strptime(trim(open_date), '%Y-%m-%d') AS open_date,
           trim(manager_name) AS manager_name,
           TRY_CAST(trim(capacity_orders_per_day) AS INTEGER) AS capacity_orders_per_day
    FROM v_latest_raw_stores
), flags AS (
    SELECT *,
        coalesce(store_id, '') = ''                                         AS chk_stores_store_id_not_null,
        coalesce(zone, '') = ''                                             AS chk_stores_zone_not_null,
        row_number() OVER (PARTITION BY store_id ORDER BY _row_num) > 1     AS chk_stores_store_id_unique,
        coalesce(zone, '') <> '' AND zone NOT IN ('North', 'South', 'East', 'West') AS chk_stores_zone_valid,
        NOT regexp_full_match(coalesce(pincode, ''), '\d{6}')               AS chk_stores_pincode_6_digits,
        NOT coalesce(capacity_orders_per_day > 0, false)                    AS chk_stores_capacity_positive,
        coalesce(open_date > TIMESTAMP '{{as_of}}', false)                  AS chk_stores_open_date_not_future
    FROM std
)
SELECT *,
    concat_ws('; ',
        CASE WHEN chk_stores_store_id_not_null THEN 'stores_store_id_not_null' END,
        CASE WHEN chk_stores_zone_not_null THEN 'stores_zone_not_null' END,
        CASE WHEN chk_stores_store_id_unique THEN 'stores_store_id_unique' END,
        CASE WHEN chk_stores_zone_valid THEN 'stores_zone_valid' END,
        CASE WHEN chk_stores_open_date_not_future THEN 'stores_open_date_not_future' END) AS failure_reason,
    concat_ws('; ',
        CASE WHEN chk_stores_pincode_6_digits THEN 'stores_pincode_6_digits' END,
        CASE WHEN chk_stores_capacity_positive THEN 'stores_capacity_positive' END) AS warning_reason,
    CASE WHEN chk_stores_store_id_not_null OR chk_stores_zone_not_null OR chk_stores_store_id_unique
              OR chk_stores_zone_valid OR chk_stores_open_date_not_future THEN 'FAIL'
         WHEN chk_stores_pincode_6_digits OR chk_stores_capacity_positive THEN 'WARN' ELSE 'PASS' END AS dq_status
FROM flags;
