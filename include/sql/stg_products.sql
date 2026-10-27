-- include/sql/stg_products.sql
CREATE OR REPLACE TABLE stg_products AS
WITH latest_raw AS (
    SELECT *
    FROM raw_products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _source_file, _row_id ORDER BY _loaded_at DESC) = 1
),
ranked AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        CASE
            WHEN TRIM(COALESCE(category, '')) = '' AND product_name LIKE 'GoldenGrain%' THEN 'Staples'
            ELSE TRIM(COALESCE(category, ''))
        END AS category,
        TRIM(sub_category) AS sub_category,
        TRIM(brand) AS brand,
        TRY_CAST(mrp AS DOUBLE) AS mrp,
        TRY_CAST(cost_price AS DOUBLE) AS cost_price,
        TRIM(unit) AS unit,
        TRY_CAST(is_perishable AS BOOLEAN) AS is_perishable,
        TRY_CAST(launch_date AS DATE) AS launch_date,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(product_id) ORDER BY CASE WHEN product_name LIKE '%(old)%' THEN 2 ELSE 1 END, _row_id) AS row_num
    FROM latest_raw
)
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    mrp,
    cost_price,
    unit,
    is_perishable,
    launch_date,
    CASE
        WHEN row_num > 1 OR mrp <= 0 THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CASE
        WHEN row_num > 1 THEN 'uniqueness:duplicate_product_id'
        WHEN mrp <= 0 THEN 'accuracy:mrp_le_zero'
        WHEN category = '' THEN 'completeness:null_category'
        WHEN cost_price > mrp THEN 'accuracy:cost_price_gt_mrp'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _run_id
FROM ranked;
