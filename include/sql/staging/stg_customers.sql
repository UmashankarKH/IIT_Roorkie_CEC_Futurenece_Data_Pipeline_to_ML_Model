-- include/sql/staging/stg_customers.sql
-- ELT layer 2: materialise the staging table for "customers".
--
-- All of the logic lives in v_stg_customers (see stg_customers_view.sql) so that the full
-- build and the incremental MERGE always apply exactly the same rules.
-- Rebuilding from the raw layer is idempotent: running the DAG twice produces
-- the same table, because identical re-loads were collapsed in the view.
CREATE OR REPLACE TABLE stg_customers AS SELECT * FROM v_stg_customers;
