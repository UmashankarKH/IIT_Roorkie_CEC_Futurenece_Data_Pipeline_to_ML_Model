-- Quarantine for the ELT side. The rejected rows are already sitting in the staging
-- tables with dq_status = 'REJECTED'; this collects them into one place with the
-- whole original row kept as JSON, so nothing has to be fished back out of the CSVs.
-- include/nammamart/elt.py copies this table out to
-- include/quarantine/dinesh/elt_quarantine_<run_id>.csv as well.
--
-- Placeholders: {run_id}

CREATE TABLE IF NOT EXISTS elt_quarantine (
    run_id          VARCHAR,
    source_table    VARCHAR,
    record_key      VARCHAR,
    failure_reason  VARCHAR,
    quarantined_at  TIMESTAMP,
    row_json        VARCHAR
);

INSERT INTO elt_quarantine
SELECT '{run_id}', 'stg_orders', t.order_id, t.failure_reason, now(), CAST(to_json(t) AS VARCHAR)
FROM stg_orders t WHERE t.dq_status = 'REJECTED'
UNION ALL
SELECT '{run_id}', 'stg_payments', t.payment_id, t.failure_reason, now(), CAST(to_json(t) AS VARCHAR)
FROM stg_payments t WHERE t.dq_status = 'REJECTED'
UNION ALL
SELECT '{run_id}', 'stg_customers', t.customer_id, t.failure_reason, now(), CAST(to_json(t) AS VARCHAR)
FROM stg_customers t WHERE t.dq_status = 'REJECTED'
UNION ALL
SELECT '{run_id}', 'stg_products', t.product_id, t.failure_reason, now(), CAST(to_json(t) AS VARCHAR)
FROM stg_products t WHERE t.dq_status = 'REJECTED'
UNION ALL
SELECT '{run_id}', 'stg_stores', t.store_id, t.failure_reason, now(), CAST(to_json(t) AS VARCHAR)
FROM stg_stores t WHERE t.dq_status = 'REJECTED';
