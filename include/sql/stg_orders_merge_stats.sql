-- Staging for orders, step 2a: count what the merge is about to do (inserted / updated / status changed / kept newer).
-- Placeholders: {run_id}
WITH src AS (
    SELECT * FROM stg_orders_incoming WHERE order_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id
                               ORDER BY (dq_status <> 'FAILED') DESC, source_batch DESC, _row_id) = 1
)
SELECT COUNT(*) FILTER (WHERE t.order_id IS NULL) AS inserted,
       COUNT(*) FILTER (WHERE t.order_id IS NOT NULL AND s.source_batch >= t.source_batch
                          AND (s.dq_status <> 'FAILED' OR t.dq_status = 'FAILED')) AS updated,
       COUNT(*) FILTER (WHERE t.order_id IS NOT NULL AND s.source_batch >= t.source_batch
                          AND (s.dq_status <> 'FAILED' OR t.dq_status = 'FAILED')
                          AND s.order_status IS DISTINCT FROM t.order_status) AS status_changed,
       COUNT(*) FILTER (WHERE t.order_id IS NOT NULL AND NOT (s.source_batch >= t.source_batch
                          AND (s.dq_status <> 'FAILED' OR t.dq_status = 'FAILED'))) AS kept_existing
FROM src s LEFT JOIN stg_orders t ON t.order_id = s.order_id;
