CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
SELECT order_date, zone, store_id,
       SUM(net_revenue) AS net_revenue,
       COUNT(*) AS order_count
FROM etl_fact_orders
WHERE UPPER(order_status) = 'DELIVERED'
GROUP BY order_date, zone, store_id;
