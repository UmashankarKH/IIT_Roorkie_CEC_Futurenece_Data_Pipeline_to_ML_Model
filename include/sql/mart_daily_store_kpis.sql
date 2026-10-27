-- include/sql/mart_daily_store_kpis.sql
-- Answers Business Question 1: Daily net revenue by zone and by store (delivered orders only, after discount)
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    s.store_id,
    s.store_name,
    COUNT(o.order_id) AS total_orders,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0.0 END), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED'
GROUP BY o.order_date, s.zone, s.store_id, s.store_name
ORDER BY o.order_date, s.zone, s.store_name;
