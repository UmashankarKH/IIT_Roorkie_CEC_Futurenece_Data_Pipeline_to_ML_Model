-- include/sql/mart/mart_delivery_sla.sql
-- ELT layer 3: MART - answers business question 3
--
-- "Average delivery time per store, and the % of delivered orders within 15 minutes."
--
-- Reads  : stg_orders (PASS only), stg_stores (PASS only)
-- Note   : only DELIVERED orders have a delivery time, so cancelled and
--          returned orders are excluded before averaging.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    coalesce(s.store_name, 'UNKNOWN (master row quarantined)')         AS store_name,
    coalesce(s.zone, 'Unknown')                                        AS zone,
    count(*)                                                           AS delivered_orders,
    round(avg(o.delivery_minutes), 2)                                  AS avg_delivery_minutes,
    min(o.delivery_minutes)                                            AS fastest_delivery_minutes,
    max(o.delivery_minutes)                                            AS slowest_delivery_minutes,
    sum(CASE WHEN o.within_15_min THEN 1 ELSE 0 END)                   AS orders_within_15_min,
    round(100.0 * sum(CASE WHEN o.within_15_min THEN 1 ELSE 0 END) / count(*), 2)
                                                                       AS pct_within_15_min
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_stores WHERE dq_status = 'PASS') s ON s.store_id = o.store_id
WHERE o.dq_status = 'PASS'
  AND o.is_revenue
  AND o.delivery_minutes IS NOT NULL
GROUP BY o.store_id, coalesce(s.store_name, 'UNKNOWN (master row quarantined)'),
         coalesce(s.zone, 'Unknown')
ORDER BY pct_within_15_min DESC;
