-- Delivery performance by store, including the 15-minute service-level metric.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    store_id,
    avg(delivery_minutes) AS average_delivery_minutes,
    100.0 * avg(CASE WHEN delivery_minutes <= 15 THEN 1 ELSE 0 END) AS pct_within_15_min,
    count(*) AS delivered_orders
FROM stg_orders
WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
GROUP BY store_id;
