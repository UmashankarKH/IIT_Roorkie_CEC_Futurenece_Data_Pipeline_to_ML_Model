-- include/sql/staging/stg_orders.sql
-- ELT layer 2: FULL REFRESH of the orders staging table.
--
-- Normal daily runs do NOT use this file: they create the table with
-- stg_orders_init.sql and then upsert through stg_orders_merge.sql.
-- This file rebuilds stg_orders from the whole raw layer, which is useful
-- after a rule change ("replay history"), and is the ELT answer to
-- "we changed the definition of correct, recompute everything".
CREATE OR REPLACE TABLE stg_orders AS SELECT * FROM v_stg_orders;
