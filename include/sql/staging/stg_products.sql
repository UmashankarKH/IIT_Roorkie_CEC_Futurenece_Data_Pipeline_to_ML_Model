-- stg_products: typed product catalogue; superseded "(old)" duplicates are FAILED,
-- blank category is inferred from sub_category.
CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
    SELECT * FROM raw_products WHERE _run_id = {{run_id}}
),
category_map(sub_category, category) AS (
    VALUES ('Fresh Vegetables', 'Fruits & Vegetables'), ('Fresh Fruits', 'Fruits & Vegetables'),
           ('Herbs', 'Fruits & Vegetables'),
           ('Milk', 'Dairy & Bakery'), ('Curd & Paneer', 'Dairy & Bakery'), ('Bread', 'Dairy & Bakery'),
           ('Cheese', 'Dairy & Bakery'),
           ('Rice', 'Staples'), ('Atta', 'Staples'), ('Dal', 'Staples'), ('Oil', 'Staples'),
           ('Soft Drinks', 'Snacks & Beverages'), ('Tea & Coffee', 'Snacks & Beverages'),
           ('Biscuits', 'Snacks & Beverages'), ('Chips', 'Snacks & Beverages'),
           ('Shampoo', 'Personal Care'), ('Soap', 'Personal Care'), ('Oral Care', 'Personal Care'),
           ('Detergent', 'Household'), ('Kitchen Supplies', 'Household'), ('Cleaners', 'Household')
),
typed AS (
    SELECT
        nullif(trim(product_id), '')                    AS product_id,
        trim(product_name)                              AS product_name,
        nullif(trim(category), '')                      AS category_raw,
        trim(sub_category)                              AS sub_category,
        trim(brand)                                     AS brand,
        try_cast(trim(mrp) AS DOUBLE)                   AS mrp,
        try_cast(trim(cost_price) AS DOUBLE)            AS cost_price,
        trim(unit)                                      AS unit,
        lower(trim(is_perishable)) = 'true'             AS is_perishable,
        try_strptime(trim(launch_date), '%Y-%m-%d')::DATE AS launch_date,
        coalesce(lower(trim(product_name)) LIKE '%(old)', false) AS is_old,
        _row_num, _source_file, _run_id
    FROM src
),
flags AS (
    SELECT t.*,
        m.category                                                                  AS category_inferred,
        row_number() OVER (PARTITION BY t.product_id ORDER BY t.is_old, t._row_num) > 1 AS f_prod_id_unique,
        NOT coalesce(t.mrp > 0, false)                                              AS f_prod_mrp_positive,
        coalesce(t.launch_date >= CAST({{cutoff}} AS TIMESTAMP)::DATE, false)       AS f_prod_launch_not_future,
        t.category_raw IS NULL                                                      AS f_prod_category_present,
        coalesce(t.cost_price > t.mrp, false)                                       AS f_prod_cost_le_mrp
    FROM typed t
    LEFT JOIN category_map m ON m.sub_category = t.sub_category
)
SELECT
    product_id, product_name,
    coalesce(category_raw, category_inferred, 'Unknown') AS category,
    sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date,
    f_prod_id_unique, f_prod_mrp_positive, f_prod_launch_not_future,
    f_prod_category_present, f_prod_cost_le_mrp,
    nullif(concat_ws('; ',
        CASE WHEN f_prod_id_unique THEN 'duplicate product_id (superseded row)' END,
        CASE WHEN f_prod_mrp_positive THEN 'mrp <= 0' END,
        CASE WHEN f_prod_launch_not_future THEN 'launch_date is in the future' END), '') AS failure_reason,
    nullif(concat_ws('; ',
        CASE WHEN f_prod_category_present THEN 'category blank (inferred from sub_category)' END,
        CASE WHEN f_prod_cost_le_mrp THEN 'cost_price > mrp' END), '')                AS dq_warnings,
    CASE WHEN f_prod_id_unique OR f_prod_mrp_positive OR f_prod_launch_not_future
         THEN 'FAILED' ELSE 'PASSED' END                                              AS dq_status,
    _row_num, _source_file, _run_id
FROM flags;
