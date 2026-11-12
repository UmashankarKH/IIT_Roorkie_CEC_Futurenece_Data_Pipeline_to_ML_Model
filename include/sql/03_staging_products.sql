-- include/sql/03_staging_products.sql
-- Staging Products: Cast types, deduplicate (prefer non-old variant), validate accuracy, flag DQ.

CREATE OR REPLACE TABLE stg_products AS
WITH ranked AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        COALESCE(NULLIF(TRIM(category), ''), 'Unknown') AS category,
        TRIM(sub_category) AS sub_category,
        TRIM(brand) AS brand,
        TRY_CAST(mrp AS DOUBLE) AS mrp,
        TRY_CAST(cost_price AS DOUBLE) AS cost_price,
        TRIM(unit) AS unit,
        TRY_CAST(is_perishable AS BOOLEAN) AS is_perishable,
        TRY_CAST(launch_date AS DATE) AS launch_date,
        _loaded_at,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(product_id)
            ORDER BY CASE WHEN product_name LIKE '%(old)%' THEN 2 ELSE 1 END, _loaded_at DESC
        ) AS rnk
    FROM raw_products
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
        WHEN mrp <= 0 THEN 'FAILED'
        WHEN cost_price > mrp THEN 'WARNING'
        ELSE 'PASSED'
    END AS dq_status,
    CASE
        WHEN mrp <= 0 THEN 'Accuracy: non-positive mrp'
        WHEN cost_price > mrp THEN 'Accuracy: cost_price exceeds mrp'
        ELSE ''
    END AS failure_reason,
    _loaded_at,
    _run_id
FROM ranked
WHERE rnk = 1;
