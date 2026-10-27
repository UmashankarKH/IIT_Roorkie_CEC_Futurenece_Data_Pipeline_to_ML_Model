-- include/sql/mart_delivery_sla.sql
-- Answers Business Question 3: Average delivery time per store, and % of delivered orders within 15 minutes
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    COUNT(o.order_id) AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes,
    SUM(CASE WHEN o.delivery_minutes <= 15 THEN 1 ELSE 0 END) AS orders_within_15_min,
    ROUND(SUM(CASE WHEN o.delivery_minutes <= 15 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(o.order_id), 0), 2) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY s.zone, s.store_name;
