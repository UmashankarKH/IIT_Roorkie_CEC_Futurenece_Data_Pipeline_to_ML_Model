-- Staging: products. All product rules are Critical (same as the ETL pipeline).
CREATE OR REPLACE TABLE stg_products AS
WITH base AS (
    SELECT
        trim(product_id)                      AS product_id,
        trim(product_name)                    AS product_name,
        trim(category)                        AS category,
        trim(sub_category)                    AS sub_category,
        trim(brand)                           AS brand,
        TRY_CAST(trim(mrp) AS DOUBLE)         AS mrp,
        TRY_CAST(trim(cost_price) AS DOUBLE)  AS cost_price,
        trim(unit)                            AS unit,
        trim(is_perishable)                   AS is_perishable,
        TRY_CAST(trim(launch_date) AS DATE)   AS launch_date,
        _source_file, _run_id, _loaded_at,
        row_number() OVER (PARTITION BY trim(product_id) ORDER BY rowid) AS rn
    FROM raw_products
    WHERE _run_id = '{{run_id}}'
),
checked AS (
    SELECT *,
        concat_ws(';',
            CASE WHEN product_id = '' THEN 'product_id_not_null' END,
            CASE WHEN category = '' THEN 'category_not_null' END,
            CASE WHEN product_id <> '' AND rn > 1 THEN 'product_id_unique' END,
            CASE WHEN mrp IS NULL OR mrp <= 0 THEN 'mrp_positive' END,
            CASE WHEN cost_price IS NULL OR mrp IS NULL OR cost_price > mrp THEN 'cost_price_le_mrp' END,
            CASE WHEN launch_date > current_date THEN 'launch_date_not_future' END
        ) AS failure_reason
    FROM base
)
SELECT
    product_id, product_name, category, sub_category, brand, mrp, cost_price,
    unit, is_perishable, launch_date,
    CASE WHEN failure_reason <> '' THEN 'REJECTED' ELSE 'PASSED' END AS dq_status,
    failure_reason,
    '' AS warning_reason,
    _source_file, _run_id, _loaded_at
FROM checked;