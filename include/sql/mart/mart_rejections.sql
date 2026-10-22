-- Q6: rows rejected in this run, by file and by reason (the ELT "quarantine").
-- FAILED rows are never updated by the MERGE, so orders.csv rejections are read
-- from stg_orders and increment rejections from stg_orders_increment.
CREATE OR REPLACE TABLE mart_rejections AS
WITH rejected AS (
    SELECT _source_file AS source_file, failure_reason FROM stg_orders
    WHERE dq_status = 'FAILED' AND _source_file = 'orders.csv'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_orders_increment WHERE dq_status = 'FAILED'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_payments WHERE dq_status = 'FAILED'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_customers WHERE dq_status = 'FAILED'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_products WHERE dq_status = 'FAILED'
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_stores WHERE dq_status = 'FAILED'
)
SELECT {{run_id}} AS run_id, source_file, failure_reason, count(*) AS rows_rejected
FROM rejected
GROUP BY ALL
ORDER BY source_file, rows_rejected DESC;
