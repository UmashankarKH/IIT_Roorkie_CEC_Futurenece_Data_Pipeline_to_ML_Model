CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    s.store_id,
    s.store_name,
    COUNT(*) AS delivered_orders,
    ROUND(SUM(o.order_amount), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.dq_status <> 'FAIL'
  AND s.dq_status <> 'FAIL'
  AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone, s.store_id, s.store_name
ORDER BY o.order_date, s.zone, s.store_id;
