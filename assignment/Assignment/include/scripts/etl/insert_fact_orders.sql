INSERT INTO etl_fact_orders
SELECT incoming_orders.*
FROM incoming_orders
WHERE NOT EXISTS (
    SELECT 1
    FROM etl_fact_orders existing
    WHERE existing.order_id = incoming_orders.order_id
);