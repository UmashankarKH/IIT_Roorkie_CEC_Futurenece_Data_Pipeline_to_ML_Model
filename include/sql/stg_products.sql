CREATE OR REPLACE TABLE stg_products AS
WITH batch_ranked AS (
    SELECT _source_file, _run_id, MAX(_loaded_at) AS batch_at,
           ROW_NUMBER() OVER (PARTITION BY _source_file ORDER BY MAX(_loaded_at) DESC) AS batch_rank
    FROM raw_products
    GROUP BY _source_file, _run_id
), latest_batches AS (
    SELECT _source_file, _run_id FROM batch_ranked WHERE batch_rank = 1
), ranked AS (
    SELECT r.*,
           COUNT(*) OVER (PARTITION BY r.product_id, r._source_file) AS source_key_count,
           ROW_NUMBER() OVER (PARTITION BY r.product_id ORDER BY r._loaded_at ASC) AS key_rank
    FROM raw_products r
    JOIN latest_batches b USING (_source_file, _run_id)
), typed AS (
    SELECT
        NULLIF(TRIM(product_id), '') AS product_id,
        NULLIF(TRIM(product_name), '') AS product_name,
        INITCAP(TRIM(category)) AS category,
        INITCAP(TRIM(sub_category)) AS sub_category,
        TRIM(brand) AS brand,
        TRY_CAST(NULLIF(TRIM(mrp), '') AS DOUBLE) AS mrp,
        TRY_CAST(NULLIF(TRIM(cost_price), '') AS DOUBLE) AS cost_price,
        TRIM(unit) AS unit,
        CASE WHEN LOWER(TRIM(is_perishable)) IN ('true', 't', '1', 'yes', 'y') THEN TRUE
             WHEN LOWER(TRIM(is_perishable)) IN ('false', 'f', '0', 'no', 'n') THEN FALSE
             ELSE NULL END AS is_perishable,
        TRY_CAST(NULLIF(TRIM(launch_date), '') AS DATE) AS launch_date,
        source_key_count, _source_file, _run_id
    FROM ranked
    WHERE key_rank = 1
), checked AS (
    SELECT *,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN product_id IS NULL THEN 'product_id is missing' END,
            CASE WHEN category IS NULL OR category = '' THEN 'category is missing' END,
            CASE WHEN mrp IS NULL OR mrp <= 0 THEN 'mrp must be positive' END,
            CASE WHEN cost_price IS NULL OR cost_price < 0 THEN 'cost_price must be non-negative' END,
            CASE WHEN cost_price > mrp THEN 'cost_price exceeds mrp' END,
            CASE WHEN launch_date IS NULL THEN 'launch_date is missing or invalid' END,
            CASE WHEN is_perishable IS NULL THEN 'is_perishable is not a recognised boolean' END
        ), '') AS critical_reason,
        NULLIF(CONCAT_WS('; ',
            CASE WHEN source_key_count > 1 THEN 'duplicate product_id deduplicated to first source row' END
        ), '') AS warning_reason
    FROM typed
)
SELECT product_id, product_name, category, sub_category, brand, mrp, cost_price,
       unit, is_perishable, launch_date,
       CASE WHEN critical_reason IS NOT NULL THEN 'FAIL'
            WHEN warning_reason IS NOT NULL THEN 'WARN' ELSE 'PASS' END AS dq_status,
       NULLIF(CONCAT_WS('; ', critical_reason, warning_reason), '') AS failure_reason,
       _source_file, _run_id, source_key_count AS source_row_count
FROM checked;
