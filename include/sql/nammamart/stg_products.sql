-- include/sql/nammamart/stg_products.sql

CREATE OR REPLACE TABLE stg_products AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_products
),
base AS (
    SELECT
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        CAST(mrp        AS DOUBLE) AS mrp,
        CAST(cost_price AS DOUBLE) AS cost_price,
        unit,
        is_perishable,
        CAST(launch_date AS DATE)  AS launch_date,
        _loaded_at,
        _source_file,
        _run_id
    FROM deduped
    WHERE rn = 1
),
checked AS (
    SELECT *,
        CASE
            WHEN category IS NULL          THEN 'category is null'
            WHEN mrp IS NULL OR mrp <= 0   THEN 'mrp must be > 0'
            WHEN cost_price > mrp          THEN 'cost_price > mrp'
            ELSE NULL
        END AS failure_reason
    FROM base
)
SELECT *,
    CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status
FROM checked;
