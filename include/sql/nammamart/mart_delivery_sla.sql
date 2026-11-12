-- include/sql/nammamart/mart_delivery_sla.sql
-- BQ3: Average delivery time per store, % within 15 min

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    s.store_name,
    s.zone,
    COUNT(*)                                                           AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 2)                                  AS avg_delivery_min,
    MIN(o.delivery_minutes)                                            AS min_delivery_min,
    MAX(o.delivery_minutes)                                            AS max_delivery_min,
    ROUND(100.0 * SUM(o.within_15_min) / COUNT(*), 2)                 AS pct_within_15_min
FROM stg_orders o
LEFT JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status    = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY o.store_id, s.store_name, s.zone
ORDER BY avg_delivery_min;
