INSERT INTO dq_results
SELECT
    (SELECT max(_run_id) FROM raw_orders) AS run_id,
    'nammamart_elt_udaykiran_komati' AS dag_id,
    check_name,
    dimension,
    severity,
    rows_checked,
    rows_failed,
    CASE WHEN rows_failed = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    current_timestamp AS checked_at
FROM (
    SELECT 'stg_orders_pass_rate' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity,
           count(*) AS rows_checked, sum(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END) AS rows_failed
    FROM stg_orders
    UNION ALL
    SELECT 'stg_payments_pass_rate', 'Consistency', 'Critical', count(*), sum(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END)
    FROM stg_payments
    UNION ALL
    SELECT 'stg_customers_pass_rate', 'Validity', 'Warning', count(*), sum(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END)
    FROM stg_customers
    UNION ALL
    SELECT 'stg_products_pass_rate', 'Accuracy', 'Critical', count(*), sum(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END)
    FROM stg_products
    UNION ALL
    SELECT 'stg_stores_pass_rate', 'Completeness', 'Warning', count(*), sum(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END)
    FROM stg_stores
);
