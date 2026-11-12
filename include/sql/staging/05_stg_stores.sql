-- manager_name (personal data) is not needed for reporting, so it is not carried into staging.
CREATE OR REPLACE TABLE stg_stores AS
SELECT
    store_id, store_name,
    coalesce(zone, 'UNKNOWN') AS zone,                                            -- DI-40
    area, pincode,
    CAST(open_date AS DATE) AS open_date,
    CASE WHEN capacity_orders_per_day > 0 THEN capacity_orders_per_day END AS capacity_orders_per_day,  -- DI-41
    dq_status, failure_reason, warning_reason,
    _row AS _source_row, _source_file, _run_id, _loaded_at
FROM stg_stores_flagged;
