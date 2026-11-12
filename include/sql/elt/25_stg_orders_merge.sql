-- Incremental load: MERGE (upsert) the increment batch into stg_orders on order_id.
--   * new order_id        -> inserted
--   * existing order_id   -> updated in place (e.g. DELIVERED -> RETURNED), only if the incoming row passed DQ
--   * duplicate/failed rows in the increment are inserted flagged FAIL so they show up in the rejected-rows mart
CREATE TABLE IF NOT EXISTS stg_order_status_history (
    order_id VARCHAR, old_status VARCHAR, new_status VARCHAR, old_net_revenue DOUBLE, new_net_revenue DOUBLE,
    changed_in_run VARCHAR, changed_at TIMESTAMP
);

-- keep the superseded version: finance can still ask "what did we report before the correction?"
INSERT INTO stg_order_status_history
SELECT t.order_id, t.order_status, n.order_status,
       CASE WHEN t.order_status = 'DELIVERED' THEN t.order_amount ELSE 0 END,
       CASE WHEN n.order_status = 'DELIVERED' THEN n.order_amount ELSE 0 END,
       '{{run_id}}', now()
FROM stg_orders_batch n JOIN stg_orders t ON t.order_id = n.order_id AND t.dup_seq = 1
WHERE n.dup_seq = 1 AND n.dq_status <> 'FAIL' AND t.dq_status <> 'FAIL' AND t.order_status <> n.order_status
  AND NOT EXISTS (SELECT 1 FROM stg_order_status_history h
                  WHERE h.order_id = t.order_id AND h.old_status = t.order_status AND h.new_status = n.order_status);

-- rows of this increment that were inserted by an earlier attempt of the same file are removed first
DELETE FROM stg_orders WHERE _source_file = '{{source_file}}' AND dup_seq > 1;

MERGE INTO stg_orders AS t
USING (SELECT * FROM stg_orders_batch WHERE dup_seq = 1) AS n
   ON t.order_id = n.order_id AND t.dup_seq = 1
WHEN MATCHED AND n.dq_status <> 'FAIL' THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

INSERT INTO stg_orders SELECT * FROM stg_orders_batch WHERE dup_seq > 1;
