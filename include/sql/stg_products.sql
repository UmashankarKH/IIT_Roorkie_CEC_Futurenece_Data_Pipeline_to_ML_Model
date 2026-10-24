-- Cast product fields and reject impossible catalogue prices or missing categories.
CREATE OR REPLACE TABLE stg_products AS
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    try_cast(mrp AS DECIMAL(12, 2)) AS mrp,
    try_cast(cost_price AS DECIMAL(12, 2)) AS cost_price,
    unit,
    is_perishable,
    try_cast(launch_date AS DATE) AS launch_date,
    CASE WHEN product_id <> '' AND category <> '' AND try_cast(mrp AS DECIMAL(12, 2)) > 0
              AND try_cast(cost_price AS DECIMAL(12, 2)) <= try_cast(mrp AS DECIMAL(12, 2))
         THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    concat_ws('; ',
        CASE WHEN product_id = '' THEN 'Missing product_id' END,
        CASE WHEN category = '' THEN 'Missing category' END,
        CASE WHEN try_cast(mrp AS DECIMAL(12, 2)) <= 0 THEN 'MRP is not positive' END,
        CASE WHEN try_cast(cost_price AS DECIMAL(12, 2)) > try_cast(mrp AS DECIMAL(12, 2)) THEN 'Cost price exceeds MRP' END
    ) AS failure_reason
FROM raw_products
-- rowid preserves CSV order, so the first source row wins exactly as in the ETL.
QUALIFY row_number() OVER (PARTITION BY product_id ORDER BY _loaded_at, rowid) = 1;
