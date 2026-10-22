CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.order_date,
    s.store_id,
    s.store_name,
    s.zone,
    COUNT(*) AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 2) AS average_delivery_minutes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) / NULLIF(COUNT(*), 0), 2) AS within_15_min_pct
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.dq_status <> 'FAIL'
  AND s.dq_status <> 'FAIL'
  AND o.order_status = 'DELIVERED'
  AND o.delivery_minutes IS NOT NULL
GROUP BY o.order_date, s.store_id, s.store_name, s.zone
ORDER BY o.order_date, s.zone, s.store_id;
