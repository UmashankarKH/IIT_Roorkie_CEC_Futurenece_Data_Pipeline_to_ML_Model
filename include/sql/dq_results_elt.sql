-- Quality scorecard for the ELT run: one row per check per file.
-- Counts how many rows in each staging table carry each check name in
-- dq_failed (critical) or dq_warnings (warning). The DAG adds run_id / dag_id
-- and writes the result to dq_results.

WITH checks(source_table, check_name, dimension, severity) AS (
    VALUES
    ('stores', 'store_id_not_null', 'Completeness', 'Critical'),
    ('stores', 'zone_present', 'Completeness', 'Warning'),
    ('stores', 'store_id_unique', 'Uniqueness', 'Critical'),
    ('stores', 'zone_valid', 'Validity', 'Critical'),
    ('stores', 'pincode_6_digits', 'Validity', 'Warning'),
    ('stores', 'capacity_positive', 'Accuracy', 'Warning'),
    ('stores', 'open_date_not_future', 'Timeliness', 'Critical'),

    ('products', 'category_not_null', 'Completeness', 'Critical'),
    ('products', 'product_id_unique', 'Uniqueness', 'Critical'),
    ('products', 'launch_date_valid', 'Validity', 'Critical'),
    ('products', 'mrp_positive', 'Accuracy', 'Critical'),
    ('products', 'cost_price_not_above_mrp', 'Accuracy', 'Critical'),
    ('products', 'launch_date_not_future', 'Timeliness', 'Critical'),

    ('customers', 'customer_id_not_null', 'Completeness', 'Critical'),
    ('customers', 'city_present', 'Completeness', 'Warning'),
    ('customers', 'customer_id_unique', 'Uniqueness', 'Critical'),
    ('customers', 'signup_date_valid', 'Validity', 'Critical'),
    ('customers', 'loyalty_tier_valid', 'Validity', 'Critical'),
    ('customers', 'email_format', 'Validity', 'Warning'),
    ('customers', 'phone_10_digits', 'Validity', 'Warning'),
    ('customers', 'pincode_6_digits', 'Validity', 'Warning'),
    ('customers', 'loyalty_tier_casing', 'Consistency', 'Warning'),
    ('customers', 'signup_date_not_future', 'Timeliness', 'Critical'),

    ('orders', 'customer_id_not_null', 'Completeness', 'Critical'),
    ('orders', 'delivered_has_delivery_minutes', 'Completeness', 'Critical'),
    ('orders', 'order_id_unique', 'Uniqueness', 'Critical'),
    ('orders', 'order_ts_valid', 'Validity', 'Critical'),
    ('orders', 'order_status_valid', 'Validity', 'Critical'),
    ('orders', 'payment_mode_valid', 'Validity', 'Critical'),
    ('orders', 'discount_pct_in_range', 'Validity', 'Critical'),
    ('orders', 'quantity_positive', 'Accuracy', 'Critical'),
    ('orders', 'order_amount_matches_formula', 'Accuracy', 'Critical'),
    ('orders', 'delivery_minutes_below_120', 'Accuracy', 'Critical'),
    ('orders', 'delivery_minutes_normal_range', 'Accuracy', 'Warning'),
    ('orders', 'customer_id_in_master', 'Consistency', 'Critical'),
    ('orders', 'product_id_in_master', 'Consistency', 'Critical'),
    ('orders', 'store_id_in_master', 'Consistency', 'Critical'),
    ('orders', 'values_need_standardising', 'Consistency', 'Warning'),
    ('orders', 'unit_price_vs_mrp', 'Consistency', 'Warning'),
    ('orders', 'order_ts_not_future', 'Timeliness', 'Critical'),

    ('payments', 'order_id_not_null', 'Completeness', 'Critical'),
    ('payments', 'gateway_ref_present', 'Completeness', 'Warning'),
    ('payments', 'payment_id_unique', 'Uniqueness', 'Critical'),
    ('payments', 'payment_ts_valid', 'Validity', 'Critical'),
    ('payments', 'payment_status_valid', 'Validity', 'Critical'),
    ('payments', 'payment_mode_valid', 'Validity', 'Critical'),
    ('payments', 'amount_paid_valid', 'Accuracy', 'Critical'),
    ('payments', 'order_id_in_orders', 'Consistency', 'Critical'),
    ('payments', 'values_need_standardising', 'Consistency', 'Warning'),
    ('payments', 'payment_ts_not_future', 'Timeliness', 'Critical')
),

flags AS (
    SELECT 'stores' AS source_table, _source_file, dq_failed, dq_warnings FROM stg_stores
    UNION ALL SELECT 'products', _source_file, dq_failed, dq_warnings FROM stg_products
    UNION ALL SELECT 'customers', _source_file, dq_failed, dq_warnings FROM stg_customers
    UNION ALL SELECT 'orders', _source_file, dq_failed, dq_warnings FROM stg_orders_validated
    UNION ALL SELECT 'payments', _source_file, dq_failed, dq_warnings FROM stg_payments
)

SELECT
    f._source_file AS source_file,
    c.check_name,
    c.dimension,
    c.severity,
    count(*) AS rows_checked,
    count(*) FILTER (WHERE list_contains(f.dq_failed, c.check_name)
                        OR list_contains(f.dq_warnings, c.check_name)) AS rows_failed
FROM checks c
JOIN flags f ON f.source_table = c.source_table
GROUP BY ALL
ORDER BY source_file, c.dimension, c.check_name;
