-- Upsert on order_id: corrections overwrite the old row, new orders are inserted. Safe to run repeatedly.
MERGE INTO stg_orders AS t
USING stg_orders_inc AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
