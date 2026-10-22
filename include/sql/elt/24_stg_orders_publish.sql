-- Full load: the September batch becomes stg_orders (rebuilt from raw, so re-running never double counts).
CREATE OR REPLACE TABLE stg_orders AS SELECT * FROM stg_orders_batch;

CREATE TABLE IF NOT EXISTS stg_order_status_history (
    order_id VARCHAR, old_status VARCHAR, new_status VARCHAR, old_net_revenue DOUBLE, new_net_revenue DOUBLE,
    changed_in_run VARCHAR, changed_at TIMESTAMP
);
