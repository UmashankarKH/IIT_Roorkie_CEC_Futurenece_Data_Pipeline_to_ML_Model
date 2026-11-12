CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT CAST(o.order_ts_clean AS DATE) order_date, s.zone, s.store_id, s.store_name,
 COUNT(*) delivered_orders, SUM(o.order_amount_clean) net_revenue
FROM stg_orders o LEFT JOIN stg_stores s ON o.store_id=s.store_id
WHERE o.is_valid AND o.order_status_clean='DELIVERED'
GROUP BY 1,2,3,4;
