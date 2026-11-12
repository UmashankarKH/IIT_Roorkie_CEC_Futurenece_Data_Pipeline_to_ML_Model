-- include/sql/10_mart_delivery_sla.sql
-- Mart: Average delivery time per store and % of orders delivered within 15 minutes.
-- Answers Business Question 3.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    COUNT(o.order_id) AS total_delivered_orders,
    ROUND(AVG(o.delivery_minutes), 1) AS avg_delivery_minutes,
    ROUND(MIN(o.delivery_minutes), 1) AS min_delivery_minutes,
    ROUND(MAX(o.delivery_minutes), 1) AS max_delivery_minutes,
    SUM(CASE WHEN o.within_15_min THEN 1 ELSE 0 END) AS orders_within_15_min,
    ROUND(SUM(CASE WHEN o.within_15_min THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id), 1) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY pct_within_15_min DESC, avg_delivery_minutes ASC;
