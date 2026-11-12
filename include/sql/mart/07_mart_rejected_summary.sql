-- Business question 6: rows rejected in THIS run, by file and by reason.
-- A row that broke two rules is counted once under each rule (rows_rejected per file is exact).
CREATE OR REPLACE TABLE mart_rejected_summary AS
WITH rejected AS (
    SELECT _source_file, failure_reason FROM stg_orders    WHERE dq_status = 'FAIL' AND _run_id = '{{ run_id }}'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_payments  WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_products  WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_stores    WHERE dq_status = 'FAIL'
)
SELECT _source_file AS source_file, reason AS failed_check, count(*) AS rows_rejected
FROM (SELECT _source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM rejected)
GROUP BY ALL
ORDER BY source_file, rows_rejected DESC, failed_check;
