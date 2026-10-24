-- include/sql/mart_delivery_sla.sql
-- Mart Layer: Average delivery time per store and % delivered within 15 minutes
-- Answers Business Question 3: Average delivery time per store, and the % of delivered orders within 15 minutes

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS total_delivered_orders,
    ROUND(AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes END), 1) AS avg_delivery_minutes,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15 THEN 1 END) AS orders_within_15_min,
    ROUND(COUNT(CASE WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15 THEN 1 END) * 100.0 / 
          NULLIF(COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END), 0), 2) AS pct_within_15_min
FROM stg_orders o
INNER JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'VALID'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY pct_within_15_min DESC, avg_delivery_minutes ASC;
