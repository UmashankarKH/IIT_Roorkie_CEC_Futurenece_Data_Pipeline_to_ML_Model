CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT s.store_id, s.store_name, AVG(o.delivery_minutes_clean) avg_delivery_minutes,
 100.0*SUM(CASE WHEN o.delivery_minutes_clean<=15 THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0) delivered_within_15_min_pct,
 COUNT(*) delivered_orders
FROM stg_orders o LEFT JOIN stg_stores s ON o.store_id=s.store_id
WHERE o.is_valid AND o.order_status_clean='DELIVERED' AND o.delivery_minutes_clean IS NOT NULL
GROUP BY s.store_id,s.store_name;
