CREATE OR REPLACE TABLE stg_products AS
SELECT
    product_id, product_name,
    coalesce(category, 'UNKNOWN') AS category,                                   -- DI-37
    sub_category, brand,
    CASE WHEN mrp > 0 THEN mrp END AS mrp,                                        -- DI-39
    CASE WHEN cost_price > 0 AND NOT (coalesce(mrp > 0, FALSE) AND cost_price > mrp)
         THEN cost_price END AS cost_price,                                       -- DI-38: excluded from margin
    unit,
    nm_bool(is_perishable) AS is_perishable,
    CAST(launch_date AS DATE) AS launch_date,
    dq_status, failure_reason, warning_reason,
    _row AS _source_row, _source_file, _run_id, _loaded_at
FROM stg_products_flagged;
