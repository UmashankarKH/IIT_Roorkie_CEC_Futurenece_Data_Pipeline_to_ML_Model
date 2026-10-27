-- MART: delivery speed per store (DELIVERED orders only).
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT o.store_id, s.store_name, s.zone,
       count(*) AS delivered_orders,
       round(avg(o.delivery_minutes), 1) AS avg_delivery_minutes,
       round(100.0 * sum(CASE WHEN o.delivery_minutes <= 15 THEN 1 ELSE 0 END) / count(*), 1) AS pct_within_15_min
FROM stg_orders o
LEFT JOIN stg_stores s ON s.store_id = o.store_id AND s.dim_usable
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY o.store_id, s.store_name, s.zone;
