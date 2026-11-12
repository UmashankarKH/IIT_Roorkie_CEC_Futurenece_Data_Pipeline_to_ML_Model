-- Staging: products. Duplicate product_id: the current catalogue row wins over the "(old)" entry.
CREATE OR REPLACE TABLE stg_products AS
WITH std AS (
    SELECT _row_num, _source_file, _run_id,
           trim(product_id) AS product_id, trim(product_name) AS product_name, trim(category) AS category,
           trim(sub_category) AS sub_category, trim(brand) AS brand, TRY_CAST(trim(mrp) AS DOUBLE) AS mrp,
           TRY_CAST(trim(cost_price) AS DOUBLE) AS cost_price, trim(unit) AS unit,
           trim(is_perishable) AS is_perishable, try_strptime(trim(launch_date), '%Y-%m-%d') AS launch_date
    FROM v_latest_raw_products
), flags AS (
    SELECT *,
        coalesce(product_id, '') = ''                                      AS chk_products_product_id_not_null,
        coalesce(category, '') = ''                                        AS chk_products_category_not_null,
        row_number() OVER (PARTITION BY product_id
                           ORDER BY product_name LIKE '%(old)', _row_num) > 1 AS chk_products_product_id_unique,
        NOT coalesce(mrp > 0, false)                                       AS chk_products_mrp_positive,
        NOT coalesce(cost_price > 0, false)                                AS chk_products_cost_price_positive,
        coalesce(cost_price > mrp, false)                                  AS chk_products_cost_not_above_mrp,
        coalesce(launch_date > TIMESTAMP '{{as_of}}', false)               AS chk_products_launch_date_not_future,
        coalesce(is_perishable, '') NOT IN ('True', 'False')               AS chk_products_is_perishable_valid
    FROM std
)
SELECT *,
    concat_ws('; ',
        CASE WHEN chk_products_product_id_not_null THEN 'products_product_id_not_null' END,
        CASE WHEN chk_products_category_not_null THEN 'products_category_not_null' END,
        CASE WHEN chk_products_product_id_unique THEN 'products_product_id_unique' END,
        CASE WHEN chk_products_mrp_positive THEN 'products_mrp_positive' END,
        CASE WHEN chk_products_cost_price_positive THEN 'products_cost_price_positive' END,
        CASE WHEN chk_products_cost_not_above_mrp THEN 'products_cost_not_above_mrp' END,
        CASE WHEN chk_products_launch_date_not_future THEN 'products_launch_date_not_future' END) AS failure_reason,
    CASE WHEN chk_products_is_perishable_valid THEN 'products_is_perishable_valid' ELSE '' END AS warning_reason,
    CASE WHEN chk_products_product_id_not_null OR chk_products_category_not_null OR chk_products_product_id_unique
              OR chk_products_mrp_positive OR chk_products_cost_price_positive OR chk_products_cost_not_above_mrp
              OR chk_products_launch_date_not_future THEN 'FAIL'
         WHEN chk_products_is_perishable_valid THEN 'WARN' ELSE 'PASS' END AS dq_status
FROM flags;
