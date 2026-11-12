-- Delivery SLA by store.
-- SLA denominator includes delivered orders with valid delivery times.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    s.store_name,
    s.zone,
    COUNT(*) AS delivered_orders_with_delivery_time,
    ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes,
    COUNT(*) FILTER (
        WHERE o.delivery_minutes <= 15
    ) AS delivered_within_15_min,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE o.delivery_minutes <= 15
        ) / NULLIF(COUNT(*), 0), 2
    ) AS delivered_within_15_min_pct
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.order_status = 'DELIVERED'
  AND o.delivery_minutes IS NOT NULL
GROUP BY o.store_id, s.store_name, s.zone;
