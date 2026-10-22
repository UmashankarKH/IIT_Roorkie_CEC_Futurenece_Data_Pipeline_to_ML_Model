-- Incremental load: validate the next-day feed with the same rules, keep it in
-- stg_orders_increment for audit, then MERGE (upsert) on order_id:
--   * a PASSED increment row whose order already exists as PASSED -> UPDATE
--     (late status changes such as DELIVERED -> RETURNED)
--   * any other increment row -> INSERT (new orders; FAILED rows kept for audit)
CREATE OR REPLACE TABLE stg_orders_increment AS
SELECT * FROM validate_orders({{run_id}}, 'orders_%_increment.csv', {{cutoff}});

MERGE INTO stg_orders AS t
USING stg_orders_increment AS s
   ON t.order_id = s.order_id
  AND t.dq_status = 'PASSED'
  AND s.dq_status = 'PASSED'
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;
