-- Staging: products received in THIS run.
-- Two product_ids appear twice (current row + an "(old)" row with last season's MRP).
-- The current row is ranked first, so the "(old)" row fails product_id_unique.

CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
    SELECT
        *,
        try_cast(trim(mrp) AS DOUBLE) AS mrp_num,
        try_cast(trim(cost_price) AS DOUBLE) AS cost_num,
        try_strptime(trim(launch_date), '%Y-%m-%d') AS launch_date_parsed,
        CAST(getvariable('base_business_date') AS DATE) + INTERVAL 1 DAY + INTERVAL 9 HOUR AS as_of,
        row_number() OVER (
            PARTITION BY product_id
            ORDER BY contains(product_name, '(old)'), _row_num
        ) AS dup_rank
    FROM raw_products
    WHERE _run_id = getvariable('run_id')
),

checked AS (
    SELECT
        *,
        list_filter([
            CASE WHEN coalesce(trim(category), '') = '' THEN 'category_not_null' END,
            CASE WHEN dup_rank > 1 THEN 'product_id_unique' END,
            CASE WHEN launch_date_parsed IS NULL THEN 'launch_date_valid' END,
            CASE WHEN mrp_num IS NULL OR mrp_num <= 0 THEN 'mrp_positive' END,
            CASE WHEN cost_num IS NULL OR mrp_num IS NULL OR cost_num > mrp_num THEN 'cost_price_not_above_mrp' END,
            CASE WHEN launch_date_parsed > as_of THEN 'launch_date_not_future' END
        ], x -> x IS NOT NULL) AS dq_failed,
        CAST([] AS VARCHAR[]) AS dq_warnings
    FROM src
)

SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    mrp_num AS mrp,
    cost_num AS cost_price,
    unit,
    is_perishable = 'True' AS is_perishable,
    CAST(launch_date_parsed AS DATE) AS launch_date,
    CASE WHEN len(dq_failed) = 0 THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    array_to_string(dq_failed, '; ') AS failure_reason,
    array_to_string(dq_warnings, '; ') AS warnings,
    dq_failed,
    dq_warnings,
    _row_num,
    _source_file,
    _run_id
FROM checked;
