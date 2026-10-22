-- Staging for products: trim, dedupe, type, apply quality rules. Placeholders: {run_id} {dag_id} {as_of}
DELETE FROM dq_results WHERE run_id = '{run_id}' AND dag_id = '{dag_id}' AND check_name LIKE 'products.%';

CREATE OR REPLACE TEMP TABLE flags AS
WITH src AS (
    SELECT _row_id, _source_file,
           NULLIF(TRIM(product_id), '') AS product_id, NULLIF(TRIM(product_name), '') AS product_name,
           NULLIF(TRIM(category), '') AS category, NULLIF(TRIM(sub_category), '') AS sub_category,
           NULLIF(TRIM(brand), '') AS brand, TRY_CAST(NULLIF(TRIM(mrp), '') AS DOUBLE) AS mrp,
           TRY_CAST(NULLIF(TRIM(cost_price), '') AS DOUBLE) AS cost_price, NULLIF(TRIM(unit), '') AS unit,
           NULLIF(TRIM(is_perishable), '') AS is_perishable,
           CAST(TRY_STRPTIME(NULLIF(TRIM(launch_date), ''), '%Y-%m-%d') AS DATE) AS launch_dt
    FROM raw_products WHERE _run_id = '{run_id}'
)
SELECT *,
    product_id IS NULL AS f_product_id_present,
    product_id IS NOT NULL AND ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _row_id) > 1 AS f_product_id_unique,
    NOT COALESCE(mrp > 0, FALSE) AS f_mrp_positive,
    NOT COALESCE(cost_price > 0, FALSE) AS f_cost_price_positive,
    COALESCE(cost_price > mrp, FALSE) AS f_cost_not_above_mrp,
    category IS NULL AS f_category_present,
    launch_dt IS NULL OR launch_dt > DATE '{as_of}' AS f_launch_date_valid
FROM src;

CREATE OR REPLACE TABLE stg_products AS
WITH s AS (
    SELECT *, (f_product_id_present OR f_product_id_unique OR f_mrp_positive OR f_cost_price_positive
               OR f_cost_not_above_mrp) AS critical,
              (f_category_present OR f_launch_date_valid) AS warn
    FROM flags
)
SELECT product_id, product_name, COALESCE(category, 'UNKNOWN') AS category, sub_category, brand, mrp, cost_price,
       unit, is_perishable,
       CASE WHEN critical THEN 'FAILED' WHEN warn THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_product_id_present THEN 'product_id missing' END,
           CASE WHEN f_product_id_unique THEN 'duplicate product_id' END,
           CASE WHEN f_mrp_positive THEN 'mrp not positive' END,
           CASE WHEN f_cost_price_positive THEN 'cost_price not positive' END,
           CASE WHEN f_cost_not_above_mrp THEN 'cost_price above mrp' END), '') AS failure_reason,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN f_category_present THEN 'category missing' END,
           CASE WHEN f_launch_date_valid THEN 'launch_date invalid or in the future' END), '') AS warning_reason,
       _row_id, _source_file
FROM s;

INSERT INTO dq_results
WITH agg AS (
    SELECT 'products' AS dataset, COUNT(*) AS n,
        COUNT(*) FILTER (WHERE f_product_id_present) AS product_id_present,
        COUNT(*) FILTER (WHERE f_product_id_unique) AS product_id_unique,
        COUNT(*) FILTER (WHERE f_mrp_positive) AS mrp_positive,
        COUNT(*) FILTER (WHERE f_cost_price_positive) AS cost_price_positive,
        COUNT(*) FILTER (WHERE f_cost_not_above_mrp) AS cost_not_above_mrp,
        COUNT(*) FILTER (WHERE f_category_present) AS category_present,
        COUNT(*) FILTER (WHERE f_launch_date_valid) AS launch_date_valid
    FROM flags
), u AS (UNPIVOT agg ON COLUMNS(* EXCLUDE (dataset, n)) INTO NAME check_name VALUE failed),
meta(check_name, dimension, severity) AS (VALUES
    ('product_id_present', 'Completeness', 'Critical'), ('product_id_unique', 'Uniqueness', 'Critical'),
    ('mrp_positive', 'Accuracy', 'Critical'), ('cost_price_positive', 'Accuracy', 'Critical'),
    ('cost_not_above_mrp', 'Accuracy', 'Critical'), ('category_present', 'Completeness', 'Warning'),
    ('launch_date_valid', 'Validity', 'Warning'))
SELECT '{run_id}', '{dag_id}', u.dataset || '.' || u.check_name, m.dimension, m.severity, u.n, u.failed,
       CASE WHEN u.failed = 0 THEN 'PASS' WHEN m.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       TIMEZONE('Asia/Kolkata', NOW())
FROM u JOIN meta m USING (check_name);
