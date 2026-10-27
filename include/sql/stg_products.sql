-- stg_products: cleaned, typed and quality-checked copy of raw_products for ONE run.
-- Rows that fail a Critical check stay here with dq_status = 'FAIL' (marts ignore them).
CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
  SELECT trim(product_id) AS product_id, trim(product_name) AS product_name, trim(category) AS category_raw,
    trim(sub_category) AS sub_category, trim(brand) AS brand, try_cast(trim(mrp) AS DOUBLE) AS mrp_num,
    try_cast(trim(cost_price) AS DOUBLE) AS cost_num, trim(unit) AS unit, trim(is_perishable) AS is_perishable_raw,
    trim(launch_date) AS launch_date_raw,
    _source_row, _source_file, _run_id
  FROM raw_products
  WHERE _run_id = '{{run_id}}'
),
sub_cat AS (
  SELECT sub_category, mode(category_raw) AS mapped_category
  FROM src WHERE category_raw <> ''
  GROUP BY sub_category
),
typed AS (
  SELECT src.*,
    CAST(try_strptime(launch_date_raw, '%Y-%m-%d') AS DATE) AS launch_date,
    row_number() OVER (PARTITION BY product_id ORDER BY CASE WHEN lower(product_name) LIKE '%(old)%' THEN 1 ELSE 0 END, _source_row) AS _rn
  FROM src
),
flagged AS (
  SELECT typed.*,
    coalesce((product_id = ''), false) AS chk_product_id_not_null,
    coalesce((category_raw = ''), false) AS chk_category_not_null,
    coalesce((product_id <> '' AND _rn > 1), false) AS chk_product_id_unique,
    coalesce((NOT coalesce(mrp_num > 0, false)), false) AS chk_mrp_positive,
    coalesce((NOT coalesce(cost_num > 0, false)), false) AS chk_cost_price_positive,
    coalesce((coalesce(cost_num > mrp_num, false)), false) AS chk_cost_price_not_above_mrp,
    coalesce((unit NOT IN ('kg', 'pack', 'unit')), false) AS chk_unit_valid,
    coalesce((is_perishable_raw NOT IN ('True', 'False')), false) AS chk_is_perishable_valid,
    coalesce((launch_date IS NULL), false) AS chk_launch_date_valid,
    coalesce((CAST(launch_date AS TIMESTAMP) > CAST('{{as_of}}' AS TIMESTAMP)), false) AS chk_launch_date_not_future
  FROM typed
),
scored AS (
  SELECT flagged.*,
    concat_ws(', ', CASE WHEN chk_product_id_not_null THEN 'product_id_not_null' END, CASE WHEN chk_category_not_null THEN 'category_not_null' END, CASE WHEN chk_product_id_unique THEN 'product_id_unique' END, CASE WHEN chk_mrp_positive THEN 'mrp_positive' END, CASE WHEN chk_cost_price_positive THEN 'cost_price_positive' END, CASE WHEN chk_cost_price_not_above_mrp THEN 'cost_price_not_above_mrp' END, CASE WHEN chk_launch_date_valid THEN 'launch_date_valid' END, CASE WHEN chk_launch_date_not_future THEN 'launch_date_not_future' END) AS failed_checks,
    concat_ws('; ', CASE WHEN chk_product_id_not_null THEN 'product_id is missing' END, CASE WHEN chk_category_not_null THEN 'category is missing' END, CASE WHEN chk_product_id_unique THEN 'duplicate product_id (superseded or repeated row)' END, CASE WHEN chk_mrp_positive THEN 'mrp is zero, negative or not a number' END, CASE WHEN chk_cost_price_positive THEN 'cost_price is zero, negative or not a number' END, CASE WHEN chk_cost_price_not_above_mrp THEN 'cost_price is higher than mrp' END, CASE WHEN chk_launch_date_valid THEN 'launch_date is missing or not a valid date' END, CASE WHEN chk_launch_date_not_future THEN 'launch_date is in the future' END) AS failure_reason,
    concat_ws('; ', CASE WHEN chk_unit_valid THEN 'unit is not kg/pack/unit' END, CASE WHEN chk_is_perishable_valid THEN 'is_perishable is not True/False' END) AS dq_warnings
  FROM flagged
)
SELECT
  product_id, product_name,
  CASE WHEN category_raw <> '' THEN category_raw
       WHEN failed_checks = 'category_not_null' THEN sub_cat.mapped_category END AS category,
  scored.sub_category AS sub_category, brand,
  CASE WHEN failed_checks = 'mrp_positive, cost_price_not_above_mrp' THEN NULL ELSE mrp_num END AS mrp,
  CASE WHEN failed_checks = 'cost_price_not_above_mrp' THEN NULL ELSE cost_num END AS cost_price,
  unit, (is_perishable_raw = 'True') AS is_perishable, launch_date,
  (failed_checks = ''
   OR (failed_checks = 'category_not_null' AND sub_cat.mapped_category IS NOT NULL)
   OR failed_checks = 'cost_price_not_above_mrp'
   OR failed_checks = 'mrp_positive, cost_price_not_above_mrp') AS dim_usable,
  chk_product_id_not_null, chk_category_not_null, chk_product_id_unique, chk_mrp_positive, chk_cost_price_positive, chk_cost_price_not_above_mrp, chk_unit_valid, chk_is_perishable_valid, chk_launch_date_valid, chk_launch_date_not_future,
  scored.failed_checks AS failed_checks, failure_reason, dq_warnings,
  CASE WHEN scored.failed_checks = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
  scored._source_row AS _source_row, scored._source_file AS _source_file, scored._run_id AS _run_id
FROM scored LEFT JOIN sub_cat ON sub_cat.sub_category = scored.sub_category;
