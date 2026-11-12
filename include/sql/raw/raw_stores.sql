-- include/sql/raw/raw_stores.sql
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

CREATE TABLE IF NOT EXISTS raw_stores (
    store_id           VARCHAR,
    store_name         VARCHAR,
    zone               VARCHAR,
    area               VARCHAR,
    pincode            VARCHAR,
    open_date          VARCHAR,
    manager_name       VARCHAR,
    capacity_orders_per_day VARCHAR,
    _loaded_at       TIMESTAMP,
    _source_file     VARCHAR,
    _run_id          VARCHAR,
    _source_row_num  BIGINT
);

INSERT INTO raw_stores
SELECT
    store_id,
    store_name,
    zone,
    area,
    pincode,
    open_date,
    manager_name,
    capacity_orders_per_day,
    now()                        AS _loaded_at,
    '{source_file}'            AS _source_file,
    '{run_id}'                 AS _run_id,
    row_number() OVER ()         AS _source_row_num
FROM read_csv('{file_path}', all_varchar = true, header = true);
