-- include/sql/raw/raw_orders.sql
-- ELT layer 1: RAW (assignment Part B)
--
-- Rule for this layer: an exact copy of the CSV, every column as text, nothing
-- cleaned and nothing deleted.  Three audit columns are added so any row can be
-- traced back to the file and the run that loaded it.
--
-- Listing the columns explicitly is deliberate: if the source system renames a
-- column the load fails immediately instead of loading wrong data silently
-- (Part F experiment 6).
--
-- Parameters substituted by the Airflow task: {file_path}, {source_file}, {run_id}

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id           VARCHAR,
    order_ts           VARCHAR,
    customer_id        VARCHAR,
    store_id           VARCHAR,
    product_id         VARCHAR,
    quantity           VARCHAR,
    unit_price         VARCHAR,
    discount_pct       VARCHAR,
    order_amount       VARCHAR,
    payment_mode       VARCHAR,
    order_status       VARCHAR,
    channel            VARCHAR,
    delivery_minutes   VARCHAR,
    _loaded_at       TIMESTAMP,
    _source_file     VARCHAR,
    _run_id          VARCHAR,
    _source_row_num  BIGINT
);

INSERT INTO raw_orders
SELECT
    order_id,
    order_ts,
    customer_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    order_amount,
    payment_mode,
    order_status,
    channel,
    delivery_minutes,
    now()                        AS _loaded_at,
    '{source_file}'            AS _source_file,
    '{run_id}'                 AS _run_id,
    row_number() OVER ()         AS _source_row_num
FROM read_csv('{file_path}', all_varchar = true, header = true);
