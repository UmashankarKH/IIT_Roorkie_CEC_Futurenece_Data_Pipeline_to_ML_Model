-- Staging products. Same rules as product_checks() in include/nammamart/quality.py.

CREATE OR REPLACE TABLE stg_products AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(product_id)
                              ORDER BY _loaded_at DESC, rowid DESC) AS dup_rank
    FROM raw_products
),
typed AS (
    SELECT
        trim(product_id)                                 AS product_id,
        trim(product_name)                               AS product_name,
        title_case(nullif(trim(category), ''))           AS category,
        title_case(trim(sub_category))                   AS sub_category,
        trim(brand)                                      AS brand,
        try_cast(mrp AS DOUBLE)                          AS mrp,
        try_cast(cost_price AS DOUBLE)                   AS cost_price,
        trim(unit)                                       AS unit,
        lower(trim(is_perishable)) IN ('true', '1', 'yes', 'y') AS is_perishable,
        CAST(try_cast(launch_date AS TIMESTAMP) AS DATE)  AS launch_date,
        dup_rank, _run_id, _source_file, _loaded_at
    FROM ranked
),
flagged AS (
    SELECT *,
        list_filter([
            CASE WHEN product_id IS NULL OR product_id = ''
                 THEN 'Critical: product_id is missing' END,
            CASE WHEN dup_rank > 1
                 THEN 'Critical: duplicate product_id, later row kept' END,
            CASE WHEN mrp IS NULL OR mrp <= 0
                 THEN 'Critical: mrp must be greater than zero' END,
            CASE WHEN cost_price IS NOT NULL AND mrp IS NOT NULL AND cost_price > mrp
                 THEN 'Critical: cost_price is higher than mrp' END
        ], lambda x: x IS NOT NULL) AS critical_reasons,
        list_filter([
            CASE WHEN category IS NULL THEN 'Warning: category is missing' END
        ], lambda x: x IS NOT NULL) AS warning_reasons
    FROM typed
)
SELECT
    product_id, product_name, category, sub_category, brand,
    mrp, cost_price, unit, is_perishable, launch_date,
    CASE WHEN len(critical_reasons) > 0 THEN 'REJECTED' ELSE 'PASSED' END AS dq_status,
    array_to_string(list_concat(critical_reasons, warning_reasons), ' | ') AS failure_reason,
    array_to_string(warning_reasons, ' | ')                               AS dq_warnings,
    _run_id, _source_file, _loaded_at
FROM flagged;
