-- include/sql/13_mart_rejected_orders_summary.sql
-- Mart: Summary of rejected rows by file and reason.
-- Answers Business Question 6.

CREATE OR REPLACE TABLE mart_rejected_orders_summary AS
WITH order_rejections AS (
    SELECT
        'orders.csv' AS source_file,
        UNNEST(STR_SPLIT(failure_reason, '; ')) AS failure_reason,
        COUNT(*) AS rejected_count
    FROM stg_orders
    WHERE dq_status = 'FAILED'
    GROUP BY source_file, failure_reason
),
customer_rejections AS (
    SELECT
        'customers.csv' AS source_file,
        failure_reason,
        COUNT(*) AS rejected_count
    FROM stg_customers
    WHERE dq_status = 'FAILED'
    GROUP BY source_file, failure_reason
),
product_rejections AS (
    SELECT
        'products.csv' AS source_file,
        failure_reason,
        COUNT(*) AS rejected_count
    FROM stg_products
    WHERE dq_status = 'FAILED'
    GROUP BY source_file, failure_reason
),
payment_rejections AS (
    SELECT
        'payments.csv' AS source_file,
        UNNEST(STR_SPLIT(failure_reason, '; ')) AS failure_reason,
        COUNT(*) AS rejected_count
    FROM stg_payments
    WHERE dq_status = 'FAILED'
    GROUP BY source_file, failure_reason
)
SELECT * FROM order_rejections
UNION ALL
SELECT * FROM customer_rejections
UNION ALL
SELECT * FROM product_rejections
UNION ALL
SELECT * FROM payment_rejections
ORDER BY source_file, rejected_count DESC;
