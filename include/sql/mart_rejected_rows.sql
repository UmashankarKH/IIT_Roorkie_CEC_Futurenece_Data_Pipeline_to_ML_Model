-- MART: every row that failed a Critical check, with the file, the row number and the reason.
-- (ELT keeps these rows in staging and raw instead of moving them to a quarantine folder.)
CREATE OR REPLACE TABLE mart_rejected_rows AS
SELECT 'customers.csv' AS source_file, customer_id AS row_key, _source_row AS source_row, failure_reason, failed_checks, _run_id AS run_id
FROM stg_customers WHERE dq_status = 'FAIL'
UNION ALL
SELECT 'products.csv', product_id, _source_row, failure_reason, failed_checks, _run_id FROM stg_products WHERE dq_status = 'FAIL'
UNION ALL
SELECT 'stores.csv', store_id, _source_row, failure_reason, failed_checks, _run_id FROM stg_stores WHERE dq_status = 'FAIL'
UNION ALL
SELECT _source_file, order_id, _source_row, failure_reason, failed_checks, _run_id FROM stg_orders WHERE dq_status = 'FAIL'
UNION ALL
SELECT 'payments.csv', payment_id, _source_row, failure_reason, failed_checks, _run_id FROM stg_payments WHERE dq_status = 'FAIL';
