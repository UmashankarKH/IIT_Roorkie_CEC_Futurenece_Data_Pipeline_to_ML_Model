-- SQL quality scorecard for this ELT run. Repeated raw replays are collapsed
-- by source file and source row number before the checks are counted.
INSERT INTO dq_results
WITH source_volume AS (
    SELECT COUNT(*)::BIGINT AS rows_checked
    FROM raw_orders r
    JOIN _pipeline_context p ON r._run_id = p.run_id
), previous_volume AS (
    SELECT COALESCE((
        SELECT d.rows_checked
        FROM dq_results d
        JOIN _pipeline_context p ON d.dag_id = p.dag_id
        WHERE d.check_name = 'elt_orders_source_volume_not_down_90_percent'
          AND d.run_id <> p.run_id
        ORDER BY d.checked_at DESC
        LIMIT 1
    ), 0)::BIGINT AS rows_checked
), metrics AS (
    SELECT 'elt_orders_customer_id_required' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE customer_id IS NULL) AS rows_failed
    FROM stg_orders
    UNION ALL
    SELECT 'elt_orders_order_id_unique', 'Uniqueness', 'Critical',
           (SELECT COUNT(*) FROM (
               SELECT _source_file, _source_row_number,
                      ROW_NUMBER() OVER (PARTITION BY _source_file, _source_row_number ORDER BY _loaded_at DESC, _run_id DESC) AS copy_rank
               FROM raw_orders
           ) source_rows WHERE copy_rank = 1),
           COALESCE(SUM(duplicate_rows), 0)
    FROM (
        SELECT _source_file, order_id, COUNT(*) AS duplicate_rows
        FROM (
            SELECT _source_file, _source_row_number, NULLIF(TRIM(order_id), '') AS order_id,
                   ROW_NUMBER() OVER (PARTITION BY _source_file, _source_row_number ORDER BY _loaded_at DESC, _run_id DESC) AS copy_rank
            FROM raw_orders
        ) snapshots
        WHERE copy_rank = 1
        GROUP BY _source_file, order_id
        HAVING COUNT(*) > 1
    ) duplicated
    UNION ALL
    SELECT 'elt_orders_status_and_payment_mode_valid', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%order_status is invalid%' OR failure_reason LIKE '%payment_mode is invalid%')
    FROM stg_orders
    UNION ALL
    SELECT 'elt_orders_timestamp_parses', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%order_ts is missing or invalid%')
    FROM stg_orders
    UNION ALL
    SELECT 'elt_orders_amount_and_delivery_accurate', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%quantity must be greater than zero%'
                              OR failure_reason LIKE '%discount_pct must be between%'
                              OR failure_reason LIKE '%order_amount does not match%'
                              OR failure_reason LIKE '%delivery_minutes must be%'
                              OR failure_reason LIKE '%DELIVERED order needs delivery_minutes%')
    FROM stg_orders
    UNION ALL
    SELECT 'elt_orders_foreign_keys_and_payment_reconcile', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%does not exist in a valid%'
                              OR failure_reason LIKE '%successful payment amount or mode does not match%')
    FROM stg_orders
    UNION ALL
    SELECT 'elt_orders_payment_present', 'Consistency', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%no successful payment has arrived%')
    FROM stg_orders
    UNION ALL
    SELECT 'elt_orders_no_future_timestamp', 'Timeliness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%order_ts is in the future%')
    FROM stg_orders
    UNION ALL
    SELECT 'elt_orders_latest_within_24h', 'Timeliness', 'Warning', COUNT(*),
           CASE WHEN MAX(order_ts) FILTER (WHERE order_ts <= CURRENT_TIMESTAMP) IS NULL
                     OR MAX(order_ts) FILTER (WHERE order_ts <= CURRENT_TIMESTAMP) < CURRENT_TIMESTAMP - INTERVAL '24 hours'
                THEN 1 ELSE 0 END
    FROM stg_orders
    UNION ALL
    SELECT 'elt_orders_source_volume_not_down_90_percent', 'Volume', 'Warning', source_volume.rows_checked,
           CASE WHEN previous_volume.rows_checked > 0
                     AND source_volume.rows_checked < previous_volume.rows_checked * 0.10
                THEN 1 ELSE 0 END
    FROM source_volume CROSS JOIN previous_volume
    UNION ALL
    SELECT 'elt_customers_required_fields', 'Completeness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE customer_id IS NULL)
    FROM stg_customers
    UNION ALL
    SELECT 'elt_customers_unique_key', 'Uniqueness', 'Critical', COALESCE(SUM(_duplicate_count), 0),
           COALESCE(SUM(_duplicate_count - 1), 0)
    FROM stg_customers
    UNION ALL
    SELECT 'elt_customers_pincode_and_signup_valid', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%pincode must contain six digits%'
                              OR failure_reason LIKE '%signup_date is missing or invalid%')
    FROM stg_customers
    UNION ALL
    SELECT 'elt_customers_contact_format', 'Validity', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%email format should be reviewed%'
                              OR failure_reason LIKE '%phone should contain 10 digits%')
    FROM stg_customers
    UNION ALL
    SELECT 'elt_customers_loyalty_tier_allowed', 'Validity', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%loyalty_tier should be reviewed%')
    FROM stg_customers
    UNION ALL
    SELECT 'elt_customers_no_future_signup', 'Timeliness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%signup_date is in the future%')
    FROM stg_customers
    UNION ALL
    SELECT 'elt_products_category_present', 'Completeness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE category IS NULL)
    FROM stg_products
    UNION ALL
    SELECT 'elt_products_unique_key', 'Uniqueness', 'Critical', COALESCE(SUM(_duplicate_count), 0),
           COALESCE(SUM(_duplicate_count - 1), 0)
    FROM stg_products
    UNION ALL
    SELECT 'elt_products_cost_and_mrp_valid', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%mrp must be greater than zero%'
                              OR failure_reason LIKE '%cost_price must not exceed mrp%')
    FROM stg_products
    UNION ALL
    SELECT 'elt_stores_zone_and_pincode_valid', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%zone is invalid%'
                              OR failure_reason LIKE '%pincode must contain six digits%')
    FROM stg_stores
    UNION ALL
    SELECT 'elt_stores_capacity_positive', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%capacity_orders_per_day must be positive%')
    FROM stg_stores
    UNION ALL
    SELECT 'elt_payments_required_keys', 'Completeness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE payment_id IS NULL OR order_id IS NULL)
    FROM stg_payments
    UNION ALL
    SELECT 'elt_payments_unique_key', 'Uniqueness', 'Critical', COALESCE(SUM(_duplicate_count), 0),
           COALESCE(SUM(_duplicate_count - 1), 0)
    FROM stg_payments
    UNION ALL
    SELECT 'elt_payments_status_mode_timestamp_valid', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%payment_status is invalid%'
                              OR failure_reason LIKE '%payment_mode is invalid%'
                              OR failure_reason LIKE '%payment_ts is missing or invalid%')
    FROM stg_payments
    UNION ALL
    SELECT 'elt_payments_amount_nonnegative', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%amount_paid must be nonnegative%')
    FROM stg_payments
    UNION ALL
    SELECT 'elt_payments_order_and_amount_consistent', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%unknown order_id%'
                              OR failure_reason LIKE '%amount does not match order_amount%'
                              OR failure_reason LIKE '%payment exists for a cancelled order%')
    FROM stg_payments
    UNION ALL
    SELECT 'elt_payments_no_future_timestamp', 'Timeliness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%payment_ts is in the future%')
    FROM stg_payments
    UNION ALL
    SELECT 'elt_payments_gateway_reference_present', 'Completeness', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE '%gateway_ref is missing%')
    FROM stg_payments
)
SELECT context.run_id, context.dag_id, metrics.check_name, metrics.dimension, metrics.severity,
       metrics.rows_checked, metrics.rows_failed,
       CASE WHEN metrics.rows_failed = 0 THEN 'PASS'
            WHEN metrics.severity = 'Warning' THEN 'WARNING'
            ELSE 'FAIL' END,
       CURRENT_TIMESTAMP
FROM metrics CROSS JOIN _pipeline_context AS context;
