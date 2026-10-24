-- include/sql/stg_products.sql
-- Staging layer for products: deduplication, type casting, DQ checks

CREATE OR REPLACE TABLE stg_products AS
WITH deduped AS (
    SELECT
        product_id,
        product_name,
        COALESCE(category, 'Staples') AS category_clean,
        category AS category_raw,
        sub_category,
        brand,
        TRY_CAST(mrp AS DECIMAL(10, 2)) AS mrp,
        TRY_CAST(cost_price AS DECIMAL(10, 2)) AS cost_price,
        unit,
        TRY_CAST(is_perishable AS BOOLEAN) AS is_perishable,
        TRY_CAST(launch_date AS DATE) AS launch_date,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY launch_date DESC) AS rn
    FROM raw_products
),
evaluated AS (
    SELECT
        product_id,
        product_name,
        category_clean AS category,
        sub_category,
        brand,
        mrp,
        cost_price,
        unit,
        is_perishable,
        launch_date,
        _loaded_at,
        _source_file,
        _run_id,
        rn,
        CASE
            WHEN rn > 1 THEN 'DUPLICATE_PRODUCT_ID'
            WHEN mrp IS NULL OR mrp <= 0 THEN 'NEGATIVE_OR_ZERO_MRP'
            ELSE NULL
        END AS critical_failure,
        CASE
            WHEN category_raw IS NULL THEN 'MISSING_CATEGORY'
            WHEN cost_price > mrp THEN 'COST_PRICE_GREATER_THAN_MRP'
            ELSE NULL
        END AS warning_reason
    FROM deduped
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
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN critical_failure IS NOT NULL THEN 'QUARANTINE'
        WHEN warning_reason IS NOT NULL THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    COALESCE(critical_failure, warning_reason) AS failure_reason
FROM evaluated;
