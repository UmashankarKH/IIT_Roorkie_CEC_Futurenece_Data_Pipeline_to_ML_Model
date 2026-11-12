-- stg_orders: today's full order snapshot (orders.csv), rebuilt every run so a
-- re-run is idempotent. Bad rows stay here with dq_status = 'FAILED' and a
-- failure_reason; marts read only PASSED rows.
CREATE OR REPLACE TABLE stg_orders AS
SELECT * FROM validate_orders({{run_id}}, 'orders.csv', {{cutoff}});
