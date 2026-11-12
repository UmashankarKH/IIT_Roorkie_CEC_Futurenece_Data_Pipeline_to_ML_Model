-- Staging for orders, step 2b: MERGE (upsert) on order_id. A later batch overwrites an earlier one, never the reverse,
-- and a rejected row never replaces an accepted one. Re-running is safe.
CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * FROM stg_orders_incoming WHERE FALSE;

MERGE INTO stg_orders AS t
USING (
    SELECT * FROM stg_orders_incoming WHERE order_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id
                               ORDER BY (dq_status <> 'FAILED') DESC, source_batch DESC, _row_id) = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.source_batch >= t.source_batch AND (s.dq_status <> 'FAILED' OR t.dq_status = 'FAILED') THEN UPDATE
WHEN NOT MATCHED THEN INSERT;
