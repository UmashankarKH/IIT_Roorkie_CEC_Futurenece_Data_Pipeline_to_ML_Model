CREATE OR REPLACE TABLE stg_stores_std AS
SELECT
    _row,
    nm_text(store_id)       AS store_id,
    nm_text(store_name)     AS store_name,
    nm_zone(zone)           AS zone,
    nm_raw(zone)            AS zone_raw,
    nm_text(area)           AS area,
    nm_text(pincode)        AS pincode,
    nm_ts(open_date)        AS open_date,
    nm_text(manager_name)   AS manager_name,
    nm_num(capacity_orders_per_day) AS capacity_orders_per_day,
    _source_file, _run_id, _loaded_at
FROM raw_stores
WHERE _run_id = '{{ run_id }}';
