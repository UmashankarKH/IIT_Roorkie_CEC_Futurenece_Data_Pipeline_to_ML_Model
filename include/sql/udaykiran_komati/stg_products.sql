CREATE OR REPLACE TABLE stg_products AS
WITH latest AS (
    SELECT *,
           row_number() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_products
)
SELECT
    product_id,
    trim(product_name) AS product_name,
    nullif(trim(category), '') AS category,
    trim(sub_category) AS sub_category,
    trim(brand) AS brand,
    try_cast(mrp AS DOUBLE) AS mrp,
    try_cast(cost_price AS DOUBLE) AS cost_price,
    trim(unit) AS unit,
    is_perishable,
    launch_date,
    CASE
        WHEN product_id IS NULL OR trim(product_id) = '' THEN 'FAIL'
        WHEN nullif(trim(category), '') IS NULL THEN 'FAIL'
        WHEN try_cast(mrp AS DOUBLE) IS NULL OR try_cast(mrp AS DOUBLE) <= 0 THEN 'FAIL'
        WHEN try_cast(cost_price AS DOUBLE) > try_cast(mrp AS DOUBLE) THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN nullif(trim(category), '') IS NULL THEN 'missing category'
        WHEN try_cast(mrp AS DOUBLE) IS NULL OR try_cast(mrp AS DOUBLE) <= 0 THEN 'invalid mrp'
        WHEN try_cast(cost_price AS DOUBLE) > try_cast(mrp AS DOUBLE) THEN 'cost exceeds mrp'
        ELSE NULL
    END AS failure_reason
FROM latest
WHERE rn = 1;
