-- Persist one SQL quality-scorecard row for every check on every DAG run.
-- Raw-layer checks read the current snapshot of each source file (the rows
-- written by the most recent load), so re-loading the same file on every run
-- never inflates the row counts or the failure counts.

CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
    severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT,
    status VARCHAR, checked_at TIMESTAMP
);

WITH latest_order_load AS (
    SELECT _source_file, MAX(_loaded_at) AS latest_loaded_at
    FROM raw_orders GROUP BY _source_file
), current_orders AS (
    SELECT r.* FROM raw_orders r
    JOIN latest_order_load l
      ON r._source_file = l._source_file AND r._loaded_at = l.latest_loaded_at
), latest_customer_load AS (
    SELECT MAX(_loaded_at) AS latest_loaded_at FROM raw_customers
), current_customers AS (
    SELECT r.* FROM raw_customers r
    JOIN latest_customer_load l ON r._loaded_at = l.latest_loaded_at
)
INSERT INTO dq_results
SELECT '{{ run_id }}', '{{ dag_id }}', check_name, dimension, severity,
       rows_checked, rows_failed,
       CASE WHEN rows_failed = 0 THEN 'PASS'
            WHEN severity = 'Warning' THEN 'WARN'
            ELSE 'FAIL' END,
       CURRENT_TIMESTAMP
FROM (
    SELECT 'stg_orders_quality' AS check_name, 'Validity' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE dq_status = 'FAIL') AS rows_failed FROM stg_orders
    UNION ALL
    SELECT 'stg_payments_quality', 'Consistency', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_payments
    UNION ALL
    SELECT 'stg_customers_quality', 'Completeness', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_customers
    UNION ALL
    SELECT 'stg_products_quality', 'Accuracy', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_products
    UNION ALL
    SELECT 'stg_stores_quality', 'Completeness', 'Critical', COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM stg_stores
    UNION ALL
    SELECT 'raw_orders_duplicate_keys', 'Uniqueness', 'Critical',
           SUM(rows_checked), SUM(rows_failed)
    FROM (
        SELECT _source_file, COUNT(*) AS rows_checked,
               COUNT(*) - COUNT(DISTINCT order_id) AS rows_failed
        FROM current_orders GROUP BY _source_file
    ) duplicate_keys
    UNION ALL
    SELECT 'orders_future_dates', 'Timeliness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(order_ts AS TIMESTAMP) > TIMESTAMP '2026-10-09 23:59:59')
    FROM current_orders
    UNION ALL
    SELECT 'customers_future_dates', 'Timeliness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(signup_date AS DATE) > DATE '2026-10-09')
    FROM current_customers
    UNION ALL
    SELECT 'customer_pii_format', 'Validity', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE NOT regexp_matches(TRIM(email), '^[^@\s]+@[^@\s]+\.[^@\s]+$') OR NOT regexp_matches(TRIM(phone), '^[0-9]{10}$'))
    FROM current_customers
    UNION ALL
    SELECT 'delivery_normal_range', 'Accuracy', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(delivery_minutes AS INTEGER) > 35 AND TRY_CAST(delivery_minutes AS INTEGER) < 120)
    FROM current_orders
    UNION ALL
    SELECT 'order_volume_drop', 'Volume', 'Warning', COUNT(*),
           CASE WHEN COUNT(*) < 255 THEN 1 ELSE 0 END
    FROM raw_orders
    WHERE _run_id = '{{ run_id }}'
    UNION ALL
    SELECT 'price_distribution_drift', 'Distribution', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE o.unit_price > 2 * p.mrp)
    FROM stg_orders o
    JOIN stg_products p ON o.product_id = p.product_id
    UNION ALL
    SELECT 'stale_snapshot', 'Timeliness', 'Warning', COUNT(*),
           CASE WHEN MAX(order_ts) < TIMESTAMP '{{ as_of_date }}' - INTERVAL '1 day' THEN 1 ELSE 0 END
    FROM stg_orders
) checks;
