CREATE OR REPLACE TEMP TABLE merged_fact AS
SELECT * FROM etl_fact_orders
WHERE CAST(order_id AS VARCHAR) NOT IN (
  SELECT CAST(order_id AS VARCHAR) FROM incoming_orders
)
UNION ALL BY NAME
SELECT * FROM incoming_orders;
