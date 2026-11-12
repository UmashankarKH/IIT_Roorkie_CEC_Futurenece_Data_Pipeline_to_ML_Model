-- include/sql/staging/stg_products.sql
-- ELT layer 2: materialise the staging table for "products".
--
-- All of the logic lives in v_stg_products (see stg_products_view.sql) so that the full
-- build and the incremental MERGE always apply exactly the same rules.
-- Rebuilding from the raw layer is idempotent: running the DAG twice produces
-- the same table, because identical re-loads were collapsed in the view.
CREATE OR REPLACE TABLE stg_products AS SELECT * FROM v_stg_products;
