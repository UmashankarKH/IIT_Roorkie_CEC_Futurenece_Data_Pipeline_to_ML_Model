CREATE TABLE IF NOT EXISTS etl_fact_orders AS
SELECT * FROM incoming_orders WHERE 1 = 0;
