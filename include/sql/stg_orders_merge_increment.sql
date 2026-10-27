-- Incremental load: MERGE the next-day files into stg_orders on order_id.
--   order_id already there (e.g. DELIVERED -> RETURNED) -> UPDATE the row
--   new order_id                                        -> INSERT it
-- Rules:
--   - Only the first copy of an order in stg_orders is matched (duplicates stay FAILED).
--   - A correction only overwrites if it PASSED its checks and is from a newer file,
--     so a bad correction can never knock out a good order.
--   - If two increment files carry the same order, the newest file wins.

MERGE INTO stg_orders AS t
USING (
    SELECT * FROM stg_orders_validated
    WHERE _source_file <> 'orders.csv' AND dup_rank = 1
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _batch_date DESC) = 1
) AS s
ON t.order_id = s.order_id AND t.dup_rank = 1
WHEN MATCHED AND s.dq_status = 'PASSED' AND s._batch_date >= t._batch_date THEN UPDATE
WHEN NOT MATCHED THEN INSERT;
