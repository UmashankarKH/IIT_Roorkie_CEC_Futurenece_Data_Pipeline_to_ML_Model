CREATE OR REPLACE TABLE mart_dq_rejections AS
WITH failed_rows AS (
    SELECT 'orders' AS file_name, _source_file, _run_id, failure_reason, source_row_count
    FROM stg_orders WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'payments', _source_file, _run_id, failure_reason, source_row_count
    FROM stg_payments WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'customers', _source_file, _run_id, failure_reason, source_row_count
    FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'products', _source_file, _run_id, failure_reason, source_row_count
    FROM stg_products WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'stores', _source_file, _run_id, failure_reason, source_row_count
    FROM stg_stores WHERE dq_status = 'FAIL'
), reasons AS (
    SELECT file_name, _source_file, _run_id, source_row_count,
           TRIM(reason) AS failure_reason
    FROM failed_rows
    CROSS JOIN UNNEST(STRING_SPLIT(COALESCE(failure_reason, 'unspecified failure'), ';')) AS u(reason)
)
SELECT file_name, _source_file, _run_id, failure_reason,
       SUM(source_row_count) AS rejected_rows
FROM reasons
GROUP BY file_name, _source_file, _run_id, failure_reason
ORDER BY file_name, failure_reason;
