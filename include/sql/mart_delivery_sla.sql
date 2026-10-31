-- Mart: delivery SLA per store (delivered orders only). The "10-minute promise" is tracked at 15 minutes.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    coalesce(s.store_name, 'UNKNOWN')                  AS store_name,
    coalesce(nullif(s.zone, ''), 'UNKNOWN')            AS zone,
    count(*)                                           AS delivered_orders,
    round(avg(o.delivery_minutes), 1)                  AS avg_delivery_minutes,
    count(*) FILTER (WHERE o.delivery_minutes <= 15)   AS orders_within_15_min,
    round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 15) / count(*), 1) AS pct_within_15_min
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_stores WHERE dq_status <> 'REJECTED') s ON s.store_id = o.store_id
WHERE o.dq_status <> 'REJECTED'
  AND o.order_status = 'DELIVERED'
GROUP BY o.store_id, coalesce(s.store_name, 'UNKNOWN'), coalesce(nullif(s.zone, ''), 'UNKNOWN');