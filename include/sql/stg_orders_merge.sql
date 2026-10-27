-- Upsert on order_id: rows from the increment file that passed every Critical check
-- replace the older row with the same order_id (status corrections) or are added (new orders).
-- Increment rows that FAILED stay in the table flagged FAIL and do not overwrite anything.
DELETE FROM stg_orders
WHERE order_id IN (SELECT order_id FROM stg_orders_increment WHERE dq_status = 'PASS');

INSERT INTO stg_orders SELECT * FROM stg_orders_increment;
