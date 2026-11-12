
CREATE OR REPLACE TABLE stg_products AS
WITH typed AS (
    SELECT *,
        TRY_CAST(NULLIF(TRIM(mrp), '') AS DOUBLE) AS mrp_num,
        TRY_CAST(NULLIF(TRIM(cost_price), '') AS DOUBLE) AS cost_num
    FROM raw_products
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                CASE WHEN COALESCE(TRIM(product_name), '') <> '' THEN 1 ELSE 0 END DESC,
                mrp_num DESC NULLS LAST,
                product_name DESC
        ) AS rn
    FROM typed
),
deduplicated AS (
    SELECT * EXCLUDE (rn)
    FROM ranked
    WHERE rn = 1
),
checked AS (
    SELECT *,
        CASE
            WHEN COALESCE(TRIM(product_id), '') = ''
                THEN 'missing_product_id'
            WHEN COALESCE(TRIM(product_name), '') = ''
                THEN 'missing_product_name'
            WHEN mrp_num IS NULL OR mrp_num < 0
                THEN 'invalid_mrp'
            WHEN cost_num IS NULL OR cost_num < 0
                THEN 'invalid_cost_price'
            ELSE NULL
        END AS reject_reason
    FROM deduplicated
)
SELECT *,
    mrp_num AS mrp_clean,
    cost_num AS cost_price_clean,
    (reject_reason IS NULL) AS is_valid
FROM checked;
