-- Part B3, staging half: MERGE the increment rows into stg_orders on order_id.
--
-- The 1 Oct file holds 150 brand new orders and 10 corrections to September orders
-- whose status became RETURNED. An INSERT would leave two rows per corrected order
-- and the September revenue would stay wrong. So the 10 matched rows are updated in
-- place and the 150 unmatched rows are inserted.
--
-- The increment path deliberately does NOT rebuild stg_orders from scratch. Touching
-- only the 160 affected rows is the point of an incremental load, and it is also the
-- honest test of whether the MERGE key is right.
--
-- Placeholders: {run_id}

-- The ON condition includes dq_status = 'PASSED', and it has to.
--
-- stg_orders is not unique on order_id by design: a duplicated order_id arrives as
-- two rows, the later one PASSED and the earlier one REJECTED with "duplicate
-- order_id, later row kept". Matching on order_id alone made one increment row
-- update BOTH of them, which flipped the rejected copy to PASSED and put the same
-- order in the fact table twice. The next task then failed with "a target row
-- matched more than one source row", which is the kind of error I would much rather
-- have than a silently doubled order.
--
-- A clean build has at most one PASSED row per order_id, so this makes the match
-- unique. If the only row for an order_id is REJECTED, the correction is inserted
-- instead, which is right - a correction can fix an order that failed yesterday.

MERGE INTO stg_orders AS target
USING (SELECT * FROM v_stg_orders WHERE _run_id = '{run_id}') AS source
   ON target.order_id = source.order_id
  AND target.dq_status = 'PASSED'
WHEN MATCHED THEN UPDATE SET
    order_ts            = source.order_ts,
    order_date          = source.order_date,
    customer_id         = source.customer_id,
    store_id            = source.store_id,
    product_id          = source.product_id,
    quantity            = source.quantity,
    unit_price          = source.unit_price,
    discount_pct        = source.discount_pct,
    order_amount_source = source.order_amount_source,
    net_revenue         = source.net_revenue,
    payment_mode        = source.payment_mode,
    payment_mode_source = source.payment_mode_source,
    order_status        = source.order_status,
    channel             = source.channel,
    delivery_minutes    = source.delivery_minutes,
    within_15_min       = source.within_15_min,
    dq_status           = source.dq_status,
    failure_reason      = source.failure_reason,
    dq_warnings         = source.dq_warnings,
    _run_id             = source._run_id,
    _source_file        = source._source_file,
    _loaded_at          = source._loaded_at
WHEN NOT MATCHED THEN INSERT BY NAME
RETURNING merge_action;
