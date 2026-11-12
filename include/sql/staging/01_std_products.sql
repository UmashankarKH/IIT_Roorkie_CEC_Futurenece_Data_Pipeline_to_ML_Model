CREATE OR REPLACE TABLE stg_products_std AS
SELECT
    _row,
    nm_text(product_id)     AS product_id,
    nm_text(product_name)   AS product_name,
    nm_text(category)       AS category,
    nm_text(sub_category)   AS sub_category,
    nm_text(brand)          AS brand,
    nm_num(mrp)             AS mrp,
    nm_num(cost_price)      AS cost_price,
    nm_text(unit)           AS unit,
    nm_text(is_perishable)  AS is_perishable,
    nm_ts(launch_date)      AS launch_date,
    _source_file, _run_id, _loaded_at
FROM raw_products
WHERE _run_id = '{{ run_id }}';
