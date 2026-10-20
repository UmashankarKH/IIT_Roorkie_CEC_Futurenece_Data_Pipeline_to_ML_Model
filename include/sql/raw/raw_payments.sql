-- include/sql/raw/raw_payments.sql
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

CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id         VARCHAR,
    order_id           VARCHAR,
    payment_ts         VARCHAR,
    payment_mode       VARCHAR,
    amount_paid        VARCHAR,
    payment_status     VARCHAR,
    gateway_ref        VARCHAR,
    _loaded_at       TIMESTAMP,
    _source_file     VARCHAR,
    _run_id          VARCHAR,
    _source_row_num  BIGINT
);

INSERT INTO raw_payments
SELECT
    payment_id,
    order_id,
    payment_ts,
    payment_mode,
    amount_paid,
    payment_status,
    gateway_ref,
    now()                        AS _loaded_at,
    '{source_file}'            AS _source_file,
    '{run_id}'                 AS _run_id,
    row_number() OVER ()         AS _source_row_num
FROM read_csv('{file_path}', all_varchar = true, header = true);
