-- include/sql/mart_daily_store_kpis.sql
-- Mart Layer: Daily net revenue by zone and by store (Delivered orders only)
-- Answers Business Question 1: Daily net revenue by zone and by store

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    s.store_id,
    s.store_name,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
    COUNT(*) AS total_orders,
    ROUND(SUM(o.net_revenue), 2) AS net_revenue,
    ROUND(AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount END), 2) AS avg_order_value
FROM stg_orders o
INNER JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'VALID'
GROUP BY o.order_date, s.zone, s.store_id, s.store_name
ORDER BY o.order_date DESC, s.zone, net_revenue DESC;
