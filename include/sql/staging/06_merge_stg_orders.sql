-- Upsert one flagged orders batch into stg_orders (the MERGE step).
--
-- Key: order_id for rows that passed; '<file>#<row>' for rejected rows (duplicates, missing ids),
-- so bad rows stay visible in staging without colliding with the real order.
-- Rule: an existing row is only overwritten by a file that is the same age or NEWER, so a
-- 1 October correction (DELIVERED -> RETURNED) is never undone by re-running September.
-- DuckDB's INSERT ... ON CONFLICT DO UPDATE is its upsert (MERGE) statement.

CREATE TABLE IF NOT EXISTS stg_orders (
    stg_key VARCHAR PRIMARY KEY,
    order_id VARCHAR, order_ts TIMESTAMP, order_date DATE,
    customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity DOUBLE, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes DOUBLE,
    dq_status VARCHAR, failure_reason VARCHAR, warning_reason VARCHAR,
    _source_file VARCHAR, _source_row INTEGER, _source_date DATE, _run_id VARCHAR, _loaded_at TIMESTAMP
);

CREATE OR REPLACE TEMP TABLE merge_batch AS
SELECT
    CASE WHEN dq_status <> 'FAIL' THEN order_id ELSE _source_file || '#' || _row END AS stg_key,
    order_id, order_ts, CAST(order_ts AS DATE) AS order_date,
    customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel, delivery_minutes,
    dq_status, failure_reason, warning_reason,
    _source_file, _row AS _source_row,
    -- business date of the file: the date in its name; else its latest valid order date;
    -- else the day before the run date (same rule as source_date() in the ETL)
    coalesce(TRY_CAST(nullif(regexp_extract(_source_file, '[0-9]{4}-[0-9]{2}-[0-9]{2}'), '') AS DATE),
             (SELECT CAST(max(order_ts) AS DATE) FROM {{ batch_table }} WHERE dq_status <> 'FAIL'),
             CAST(DATE '{{ as_of }}' - INTERVAL 1 DAY AS DATE)) AS _source_date,
    _run_id, _loaded_at
FROM {{ batch_table }};

-- What the upsert will do, measured before it runs (printed in the task log)
CREATE OR REPLACE TEMP TABLE merge_stats AS
SELECT
    '{{ source_file }}' AS source_file,
    count(*) AS rows_in_batch,
    count(*) FILTER (WHERE b.dq_status = 'FAIL') AS rejected_rows,
    count(*) FILTER (WHERE t.stg_key IS NULL) AS inserted_new,
    count(*) FILTER (WHERE t._source_file = b._source_file) AS rewritten_same_file,
    count(*) FILTER (WHERE t._source_file <> b._source_file AND b._source_date >= t._source_date) AS updated_by_newer_file,
    count(*) FILTER (WHERE t._source_file <> b._source_file AND b._source_date < t._source_date) AS kept_newer_version
FROM merge_batch b LEFT JOIN stg_orders t ON t.stg_key = b.stg_key;

-- This file's previous rows are replaced (so re-running a file never duplicates it) ...
DELETE FROM stg_orders WHERE _source_file = '{{ source_file }}';

-- ... then upserted: new orders inserted, corrections from a newer file applied
INSERT INTO stg_orders
SELECT * FROM merge_batch
ON CONFLICT (stg_key) DO UPDATE SET
    order_id = EXCLUDED.order_id, order_ts = EXCLUDED.order_ts, order_date = EXCLUDED.order_date,
    customer_id = EXCLUDED.customer_id, store_id = EXCLUDED.store_id, product_id = EXCLUDED.product_id,
    quantity = EXCLUDED.quantity, unit_price = EXCLUDED.unit_price, discount_pct = EXCLUDED.discount_pct,
    order_amount = EXCLUDED.order_amount, payment_mode = EXCLUDED.payment_mode,
    order_status = EXCLUDED.order_status, channel = EXCLUDED.channel,
    delivery_minutes = EXCLUDED.delivery_minutes, dq_status = EXCLUDED.dq_status,
    failure_reason = EXCLUDED.failure_reason, warning_reason = EXCLUDED.warning_reason,
    _source_file = EXCLUDED._source_file, _source_row = EXCLUDED._source_row,
    _source_date = EXCLUDED._source_date, _run_id = EXCLUDED._run_id, _loaded_at = EXCLUDED._loaded_at
WHERE EXCLUDED._source_date >= stg_orders._source_date;

SELECT * FROM merge_stats;
