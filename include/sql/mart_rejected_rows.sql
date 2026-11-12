-- Mart: every row rejected in this run, one row per staged record. Placeholders: {run_id}
-- Split failure_reason on '; ' to count by reason (business question 6).
CREATE OR REPLACE TABLE mart_rejected_rows AS
SELECT '{run_id}' AS run_id, 'orders' AS source_table, _source_file AS source_file, order_id AS row_key, failure_reason
FROM stg_orders_incoming WHERE dq_status = 'FAILED'
UNION ALL
SELECT '{run_id}', 'payments', _source_file, payment_id, failure_reason FROM stg_payments WHERE dq_status = 'FAILED'
UNION ALL
SELECT '{run_id}', 'customers', _source_file, customer_id, failure_reason FROM stg_customers WHERE dq_status = 'FAILED'
UNION ALL
SELECT '{run_id}', 'products', _source_file, product_id, failure_reason FROM stg_products WHERE dq_status = 'FAILED'
UNION ALL
SELECT '{run_id}', 'stores', _source_file, store_id, failure_reason FROM stg_stores WHERE dq_status = 'FAILED';
