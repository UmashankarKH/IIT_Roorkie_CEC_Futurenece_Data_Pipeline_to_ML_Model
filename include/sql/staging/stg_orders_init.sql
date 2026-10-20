-- include/sql/staging/stg_orders_init.sql
-- ELT layer 2: create the orders staging table if it does not exist yet.
--
-- The table is created empty, with the exact column list of v_stg_orders.
-- Every row then arrives through stg_orders_merge.sql, so there is only one
-- write path into staging and a re-run can never duplicate an order.
CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * FROM v_stg_orders WHERE 1 = 0;
