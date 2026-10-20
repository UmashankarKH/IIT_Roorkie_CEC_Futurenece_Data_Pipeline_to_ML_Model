-- include/sql/staging/stg_orders_merge.sql
-- ELT incremental load (assignment Part B.3)
--
-- The next-day file (orders_2026-10-01_increment.csv) contains two kinds of row:
--   * about 150 brand new orders            -> must be INSERTed
--   * about 10 corrections to older orders  -> must UPDATE the existing row
--     (for example a September order whose status became RETURNED)
--
-- A MERGE (also called an upsert) does both in one statement, matched on the
-- business key order_id, so a re-run can never create a duplicate order.
--
-- Java comparison (learning aid): this is the SQL equivalent of
-- `repository.save(entity)` - insert when new, update when the id already exists.
--
-- Parameters substituted by the Airflow task: {run_id}, {source_file}
--
-- Filtering on _source_file lets the daily file and the increment file be
-- merged as two separate, observable steps.
MERGE INTO stg_orders AS target
USING (
    -- only the rows loaded by this run, and only the current version of each order
    SELECT * FROM v_stg_orders
    WHERE _run_id = '{run_id}'
      AND _source_file = '{source_file}'
      AND _version_rank = 1
) AS source
ON target.order_id = source.order_id
-- overwrite every column of the old row (the late status correction)
WHEN MATCHED THEN UPDATE
-- add the brand new orders
WHEN NOT MATCHED THEN INSERT;
