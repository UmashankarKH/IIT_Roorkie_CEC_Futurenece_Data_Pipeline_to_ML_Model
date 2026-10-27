-- The grain of this table is one clean order line. It is the ELT twin of
-- etl_fact_orders, and the row count and revenue totals of the two have to match
-- (the check is in include/nammamart/elt.py, task compare_with_etl).
--
-- CREATE OR REPLACE makes the full load idempotent: running the DAG twice on the
-- same day gives the same table, not double the rows. The projection itself is in
-- v_fact_orders.sql, shared with the incremental MERGE.

CREATE OR REPLACE TABLE mart_fact_orders AS
SELECT * FROM v_fact_orders;
