-- Full rebuild of the orders staging table. The projection is in v_stg_orders.sql.
-- CREATE OR REPLACE is what makes a re-run idempotent: same raw rows in, same
-- staging table out, no growth.

CREATE OR REPLACE TABLE stg_orders AS
SELECT * FROM v_stg_orders;
