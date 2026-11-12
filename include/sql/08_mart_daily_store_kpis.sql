-- include/sql/08_mart_daily_store_kpis.sql
-- Mart: Daily net revenue, volume, and average delivery SLA by store and zone (Delivered orders only).
-- Answers Business Question 1 and part of Business Question 3.

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    s.zone,
    s.store_id,
    s.store_name,
    o.order_date,
    COUNT(o.order_id) AS delivered_orders,
    ROUND(SUM(o.net_revenue), 2) AS daily_net_revenue,
    ROUND(AVG(o.delivery_minutes), 1) AS avg_delivery_minutes,
    ROUND(SUM(CASE WHEN o.within_15_min THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id), 1) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY s.zone, s.store_id, s.store_name, o.order_date
ORDER BY o.order_date, s.zone, s.store_id;
