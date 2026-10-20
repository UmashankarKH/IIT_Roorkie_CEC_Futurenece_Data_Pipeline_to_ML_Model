-- include/sql/dq/elt_rule_failures.sql
-- ELT data quality (assignment Part D) - one row per failed check, per record.
--
-- The staging views already evaluated every rule as a boolean chk_rule_* column.
-- UNPIVOT turns those columns into rows, which gives a long, queryable list of
-- "this record broke this rule".  That list is what makes the ELT side
-- auditable: finance can ask "why was ORD000153 rejected?" and get an answer
-- with one SELECT.
--
-- Uniqueness is handled separately because duplicate copies are collapsed in
-- staging; they are counted directly from the raw layer for this run.
--
-- Parameters substituted by the Airflow task: {run_id}
CREATE OR REPLACE TABLE elt_rule_failures AS
WITH order_flags AS (
    SELECT order_id AS row_key, chk_rule_001, chk_rule_003, chk_rule_004, chk_rule_005,
           chk_rule_006, chk_rule_007, chk_rule_008, chk_rule_009, chk_rule_010,
           chk_rule_011, chk_rule_012, chk_rule_013, chk_rule_014, chk_rule_015,
           chk_rule_032, chk_rule_033
    FROM stg_orders WHERE dq_status <> 'SUPERSEDED'
),
payment_flags AS (
    SELECT payment_id AS row_key, chk_rule_017, chk_rule_018, chk_rule_019, chk_rule_020,
           chk_rule_036, chk_rule_037, chk_rule_038, chk_rule_039
    FROM stg_payments WHERE dq_status <> 'SUPERSEDED'
),
customer_flags AS (
    SELECT customer_id AS row_key, chk_rule_022, chk_rule_023, chk_rule_024, chk_rule_040,
           chk_rule_041, chk_rule_042, chk_rule_043
    FROM stg_customers WHERE dq_status <> 'SUPERSEDED'
),
product_flags AS (
    SELECT product_id AS row_key, chk_rule_026, chk_rule_027, chk_rule_028, chk_rule_044,
           chk_rule_047
    FROM stg_products WHERE dq_status <> 'SUPERSEDED'
),
store_flags AS (
    SELECT store_id AS row_key, chk_rule_030, chk_rule_031, chk_rule_045, chk_rule_046,
           chk_rule_048
    FROM stg_stores WHERE dq_status <> 'SUPERSEDED'
),
unpivoted AS (
    SELECT 'orders' AS dataset, row_key, flag_name, failed FROM order_flags
        UNPIVOT (failed FOR flag_name IN (chk_rule_001, chk_rule_003, chk_rule_004,
                 chk_rule_005, chk_rule_006, chk_rule_007, chk_rule_008, chk_rule_009,
                 chk_rule_010, chk_rule_011, chk_rule_012, chk_rule_013, chk_rule_014,
                 chk_rule_015, chk_rule_032, chk_rule_033))
    UNION ALL
    SELECT 'payments', row_key, flag_name, failed FROM payment_flags
        UNPIVOT (failed FOR flag_name IN (chk_rule_017, chk_rule_018, chk_rule_019,
                 chk_rule_020, chk_rule_036, chk_rule_037, chk_rule_038, chk_rule_039))
    UNION ALL
    SELECT 'customers', row_key, flag_name, failed FROM customer_flags
        UNPIVOT (failed FOR flag_name IN (chk_rule_022, chk_rule_023, chk_rule_024,
                 chk_rule_040, chk_rule_041, chk_rule_042, chk_rule_043))
    UNION ALL
    SELECT 'products', row_key, flag_name, failed FROM product_flags
        UNPIVOT (failed FOR flag_name IN (chk_rule_026, chk_rule_027, chk_rule_028,
                 chk_rule_044, chk_rule_047))
    UNION ALL
    SELECT 'stores', row_key, flag_name, failed FROM store_flags
        UNPIVOT (failed FOR flag_name IN (chk_rule_030, chk_rule_031, chk_rule_045,
                 chk_rule_046, chk_rule_048))
),
row_rule_failures AS (
    SELECT dataset, row_key,
           upper(replace(substr(flag_name, 5), '_', '-')) AS rule_id   -- chk_rule_001 -> RULE-001
    FROM unpivoted
    WHERE failed
),
-- Duplicate keys inside ONE file loaded by this run (the copies staging collapsed).
-- Partitioning by _source_file matters: the same order_id in a later file is a
-- correction, not a duplicate, and must not be reported as a quality failure.
duplicate_failures AS (
    SELECT 'orders' AS dataset, order_id AS row_key, 'RULE-002' AS rule_id FROM (
        SELECT trim(order_id) AS order_id,
               row_number() OVER (PARTITION BY _source_file, trim(order_id)
                                  ORDER BY _source_row_num) AS rn
        FROM raw_orders WHERE _run_id = '{run_id}' AND nullif(trim(order_id), '') IS NOT NULL
    ) WHERE rn > 1
    UNION ALL
    SELECT 'payments', payment_id, 'RULE-016' FROM (
        SELECT trim(payment_id) AS payment_id,
               row_number() OVER (PARTITION BY _source_file, trim(payment_id)
                                  ORDER BY _source_row_num) AS rn
        FROM raw_payments WHERE _run_id = '{run_id}' AND nullif(trim(payment_id), '') IS NOT NULL
    ) WHERE rn > 1
    UNION ALL
    SELECT 'customers', customer_id, 'RULE-021' FROM (
        SELECT trim(customer_id) AS customer_id,
               row_number() OVER (PARTITION BY _source_file, trim(customer_id)
                                  ORDER BY _source_row_num) AS rn
        FROM raw_customers WHERE _run_id = '{run_id}' AND nullif(trim(customer_id), '') IS NOT NULL
    ) WHERE rn > 1
    UNION ALL
    SELECT 'products', product_id, 'RULE-025' FROM (
        SELECT trim(product_id) AS product_id,
               row_number() OVER (PARTITION BY _source_file, trim(product_id)
                                  ORDER BY _source_row_num) AS rn
        FROM raw_products WHERE _run_id = '{run_id}' AND nullif(trim(product_id), '') IS NOT NULL
    ) WHERE rn > 1
    UNION ALL
    SELECT 'stores', store_id, 'RULE-029' FROM (
        SELECT trim(store_id) AS store_id,
               row_number() OVER (PARTITION BY _source_file, trim(store_id)
                                  ORDER BY _source_row_num) AS rn
        FROM raw_stores WHERE _run_id = '{run_id}' AND nullif(trim(store_id), '') IS NOT NULL
    ) WHERE rn > 1
)
SELECT '{run_id}' AS run_id, dataset, row_key, rule_id, now() AS checked_at
FROM (SELECT * FROM row_rule_failures UNION ALL SELECT * FROM duplicate_failures);
