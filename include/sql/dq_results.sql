DELETE FROM dq_results
WHERE run_id = {{RUN_ID}} AND dag_id = {{DAG_ID}};

WITH checks AS (
    SELECT 'orders.required_fields' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked,
           COUNT(*) FILTER (WHERE order_id IS NULL OR order_ts IS NULL OR customer_id IS NULL
               OR store_id IS NULL OR product_id IS NULL OR quantity IS NULL OR unit_price IS NULL
               OR discount_pct IS NULL OR order_amount IS NULL OR payment_mode IS NULL OR order_status IS NULL) AS rows_failed
    FROM stg_orders
    UNION ALL
    SELECT 'orders.delivered_delivery_minutes', 'Completeness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE order_status = 'DELIVERED' AND delivery_minutes IS NULL)
    FROM stg_orders
    UNION ALL
    SELECT 'products.category_required', 'Completeness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE category IS NULL OR category = '') FROM stg_products
    UNION ALL
    SELECT 'orders.order_id_unique', 'Uniqueness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE key_count > 1)
    FROM (SELECT order_id, _source_file, COUNT(*) AS key_count FROM raw_orders
          WHERE _run_id = {{RUN_ID}} GROUP BY order_id, _source_file)
    UNION ALL
    SELECT 'payments.payment_id_unique', 'Uniqueness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE key_count > 1)
    FROM (SELECT payment_id, COUNT(*) AS key_count FROM raw_payments
          WHERE _run_id = {{RUN_ID}} GROUP BY payment_id)
    UNION ALL
    SELECT 'customers.customer_id_unique', 'Uniqueness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE key_count > 1)
    FROM (SELECT customer_id, COUNT(*) AS key_count FROM raw_customers
          WHERE _run_id = {{RUN_ID}} GROUP BY customer_id)
    UNION ALL
    SELECT 'products.product_id_unique', 'Uniqueness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE key_count > 1)
    FROM (SELECT product_id, COUNT(*) AS key_count FROM raw_products
          WHERE _run_id = {{RUN_ID}} GROUP BY product_id)
    UNION ALL
    SELECT 'stores.store_id_unique', 'Uniqueness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE key_count > 1)
    FROM (SELECT store_id, COUNT(*) AS key_count FROM raw_stores
          WHERE _run_id = {{RUN_ID}} GROUP BY store_id)
    UNION ALL
    SELECT 'orders.allowed_status', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') OR order_status IS NULL)
    FROM stg_orders
    UNION ALL
    SELECT 'orders.allowed_payment_mode', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') OR payment_mode IS NULL)
    FROM stg_orders
    UNION ALL
    SELECT 'payments.allowed_status', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') OR payment_status IS NULL)
    FROM stg_payments
    UNION ALL
    SELECT 'stores.allowed_zone', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE zone NOT IN ('NORTH', 'SOUTH', 'EAST', 'WEST') OR zone IS NULL)
    FROM stg_stores
    UNION ALL
    SELECT 'customers.allowed_loyalty_tier', 'Validity', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') OR loyalty_tier IS NULL)
    FROM stg_customers
    UNION ALL
    SELECT 'customers.email_format', 'Validity', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE COALESCE(email, '') <> '' AND NOT REGEXP_FULL_MATCH(TRIM(email), '[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+'))
    FROM raw_customers WHERE _run_id = {{RUN_ID}}
    UNION ALL
    SELECT 'customers.phone_10_digits', 'Validity', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE COALESCE(phone, '') <> '' AND LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) <> 10)
    FROM raw_customers WHERE _run_id = {{RUN_ID}}
    UNION ALL
    SELECT 'orders.quantity_positive', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE quantity IS NULL OR quantity <= 0)
    FROM stg_orders
    UNION ALL
    SELECT 'orders.amount_formula', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE order_amount IS NULL OR quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL
               OR ROUND(order_amount, 2) <> ROUND(quantity * unit_price * (1 - discount_pct / 100), 2))
    FROM stg_orders
    UNION ALL
    SELECT 'products.price_relationship', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE mrp IS NULL OR mrp <= 0 OR cost_price IS NULL OR cost_price < 0 OR cost_price > mrp)
    FROM stg_products
    UNION ALL
    SELECT 'orders.delivery_plausibility', 'Accuracy', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE delivery_minutes < 0 OR delivery_minutes > 120)
    FROM stg_orders
    UNION ALL
    SELECT 'orders.customer_foreign_key', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE customer_id IS NOT NULL AND NOT EXISTS
               (SELECT 1 FROM stg_customers c WHERE c.customer_id = o.customer_id AND c.dq_status <> 'FAIL'))
    FROM stg_orders o
    UNION ALL
    SELECT 'orders.product_foreign_key', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE product_id IS NOT NULL AND NOT EXISTS
               (SELECT 1 FROM stg_products p WHERE p.product_id = o.product_id AND p.dq_status <> 'FAIL'))
    FROM stg_orders o
    UNION ALL
    SELECT 'orders.store_foreign_key', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE store_id IS NOT NULL AND NOT EXISTS
               (SELECT 1 FROM stg_stores s WHERE s.store_id = o.store_id AND s.dq_status <> 'FAIL'))
    FROM stg_orders o
    UNION ALL
    SELECT 'payments.success_amount_matches_order', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE p.payment_status = 'SUCCESS' AND o.order_id IS NOT NULL
               AND ABS(p.amount_paid - o.order_amount) > 0.005)
    FROM stg_payments p LEFT JOIN stg_orders o USING (order_id)
    UNION ALL
    SELECT 'payments.order_foreign_key', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE p.order_id IS NOT NULL AND o.order_id IS NULL)
    FROM stg_payments p LEFT JOIN stg_orders o USING (order_id)
    UNION ALL
    SELECT 'payments.mode_matches_order', 'Consistency', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE o.order_id IS NOT NULL AND p.payment_mode <> o.payment_mode)
    FROM stg_payments p LEFT JOIN stg_orders o USING (order_id)
    UNION ALL
    SELECT 'orders.not_future_dated', 'Timeliness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE CAST(order_ts AS DATE) > CAST(TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP) AS DATE))
    FROM stg_orders
    UNION ALL
    SELECT 'customers.not_future_signup', 'Timeliness', 'Critical', COUNT(*),
           COUNT(*) FILTER (WHERE signup_date > CAST(TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP) AS DATE))
    FROM stg_customers
    UNION ALL
    SELECT 'orders.not_stale_over_one_day', 'Timeliness', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE CAST(order_ts AS DATE) < CAST(TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP) AS DATE) - INTERVAL '1 day')
    FROM stg_orders
    UNION ALL
    SELECT 'customers.loyalty_tier_standardized', 'Consistency', 'Warning', COUNT(*),
           COUNT(*) FILTER (WHERE TRIM(loyalty_tier) <> CASE UPPER(TRIM(loyalty_tier))
               WHEN 'BRONZE' THEN 'Bronze' WHEN 'SILVER' THEN 'Silver'
               WHEN 'GOLD' THEN 'Gold' WHEN 'PLATINUM' THEN 'Platinum'
               ELSE TRIM(loyalty_tier) END)
    FROM raw_customers WHERE _run_id = {{RUN_ID}}
    UNION ALL
    SELECT 'orders.source_volume_baseline', 'Volume', 'Warning', 1,
           CASE WHEN COUNT(*) < 242 THEN 1 ELSE 0 END
    FROM raw_orders
    WHERE _run_id = {{RUN_ID}} AND _source_file = 'orders.csv'
), scored AS (
    SELECT {{RUN_ID}} AS run_id, {{DAG_ID}} AS dag_id, check_name, dimension, severity,
           rows_checked, rows_failed,
           CASE WHEN rows_failed = 0 THEN 'PASS'
                WHEN severity = 'Warning' THEN 'WARN' ELSE 'FAIL' END AS status,
           CURRENT_TIMESTAMP AS checked_at
    FROM checks
)
INSERT INTO dq_results
SELECT run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at
FROM scored;
