-- include/sql/staging/stg_products_view.sql
-- ELT layer 2: STAGING (assignment Part B) - products
CREATE OR REPLACE VIEW v_stg_products AS
WITH source_rows AS (
    SELECT product_id, product_name, category, sub_category, brand, mrp, cost_price, unit,
           is_perishable, launch_date,
           max(_loaded_at) AS _loaded_at, arg_max(_source_file, _loaded_at) AS _source_file,
           arg_max(_run_id, _loaded_at)      AS _run_id, min(_source_row_num) AS _source_row_num,
           count(*) AS _raw_copies
    FROM raw_products
    GROUP BY product_id, product_name, category, sub_category, brand, mrp, cost_price, unit,
             is_perishable, launch_date
),
versioned AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(product_id)
                              ORDER BY try_cast(launch_date AS DATE) DESC NULLS LAST,
                                       _loaded_at DESC, _source_row_num DESC) AS _version_rank
    FROM source_rows
),
typed AS (
    SELECT
        nullif(trim(product_id), '')                                 AS product_id,
        nullif(trim(product_name), '')                               AS product_name,
        nullif(trim(category), '')                                   AS category_raw,
        nullif(trim(sub_category), '')                               AS sub_category,
        nullif(trim(brand), '')                                      AS brand,
        try_cast(mrp AS DOUBLE)                                      AS mrp,
        try_cast(cost_price AS DOUBLE)                               AS cost_price,
        nullif(trim(unit), '')                                       AS unit,
        CASE upper(nullif(trim(is_perishable), ''))
             WHEN 'TRUE' THEN 'Y' WHEN 'YES' THEN 'Y' WHEN '1' THEN 'Y' WHEN 'Y' THEN 'Y'
             WHEN 'FALSE' THEN 'N' WHEN 'NO' THEN 'N' WHEN '0' THEN 'N' WHEN 'N' THEN 'N'
        END                                                          AS is_perishable,
        try_cast(launch_date AS DATE)                                AS launch_date,
        _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
    FROM versioned
),
checked AS (
    SELECT *,
        product_id IS NULL                                           AS chk_rule_044,
        category_raw IS NULL                                         AS chk_rule_026,
        (mrp IS NULL OR mrp <= 0)                                    AS chk_rule_027,
        (cost_price IS NOT NULL AND mrp IS NOT NULL AND cost_price > mrp)
                                                                     AS chk_rule_028,
        (launch_date IS NOT NULL AND launch_date > as_of()::DATE) AS chk_rule_047
    FROM typed
)
SELECT
    product_id, product_name,
    -- A blank category must not delete a real delivered order from revenue, so
    -- the row loads with "Unknown" and the gap is reported as a warning.
    coalesce(category_raw, 'Unknown')                                AS category,
    sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date,
    chk_rule_044, chk_rule_026, chk_rule_027, chk_rule_028, chk_rule_047,
    CASE WHEN _version_rank > 1 THEN 'SUPERSEDED'
         WHEN chk_rule_044 OR chk_rule_027 THEN 'QUARANTINED'
         ELSE 'PASS' END                                             AS dq_status,
    concat_ws(' | ',
        CASE WHEN _version_rank > 1 THEN 'RULE-025: product_id must be unique (superseded copy) [Uniqueness/Critical]' END,
        CASE WHEN chk_rule_044 THEN 'RULE-044: product_id must be present [Completeness/Critical]' END,
        CASE WHEN chk_rule_027 THEN 'RULE-027: mrp must be greater than zero [Accuracy/Critical]' END
    )                                                                AS failure_reason,
    concat_ws(' | ',
        CASE WHEN chk_rule_026 THEN 'RULE-026: category must not be blank [Completeness/Warning]' END,
        CASE WHEN chk_rule_028 THEN 'RULE-028: cost_price should not exceed mrp [Accuracy/Warning]' END,
        CASE WHEN chk_rule_047 THEN 'RULE-047: launch_date must not be future-dated [Timeliness/Warning]' END
    )                                                                AS dq_warnings,
    _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
FROM checked;
