-- Part B3: the incremental load.
--
-- The 1 Oct increment file holds 150 brand new orders and 10 corrections where an
-- order that was DELIVERED is now RETURNED. An INSERT would duplicate the 10 and
-- double-count their revenue; a full reload would work here but is wasteful once the
-- fact table is large. So: MERGE on order_id, update what already exists, insert
-- what does not.
--
-- This is also what makes the pipeline safe to re-run. Running it twice updates the
-- same 10 rows to the same values and inserts nothing the second time, so the totals
-- do not move.
--
-- Placeholders: {run_id}

MERGE INTO mart_fact_orders AS target
USING (SELECT * FROM v_fact_orders WHERE _run_id = '{run_id}') AS source
   ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    order_ts         = source.order_ts,
    order_date       = source.order_date,
    customer_id      = source.customer_id,
    store_id         = source.store_id,
    store_name       = source.store_name,
    zone             = source.zone,
    product_id       = source.product_id,
    category         = source.category,
    sub_category     = source.sub_category,
    loyalty_tier     = source.loyalty_tier,
    quantity         = source.quantity,
    unit_price       = source.unit_price,
    discount_pct     = source.discount_pct,
    net_revenue      = source.net_revenue,
    cost_price       = source.cost_price,
    gross_margin     = source.gross_margin,
    is_revenue       = source.is_revenue,
    revenue_amount   = source.revenue_amount,
    margin_amount    = source.margin_amount,
    payment_mode     = source.payment_mode,
    order_status     = source.order_status,
    channel          = source.channel,
    delivery_minutes = source.delivery_minutes,
    within_15_min    = source.within_15_min,
    dq_warnings      = source.dq_warnings,
    _run_id          = source._run_id,
    _source_file     = source._source_file,
    _loaded_at       = source._loaded_at
WHEN NOT MATCHED THEN INSERT BY NAME
RETURNING merge_action;
