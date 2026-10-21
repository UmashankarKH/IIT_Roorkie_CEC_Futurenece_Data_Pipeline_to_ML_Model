CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT order_date, zone, store_id, store_name,
       count(DISTINCT order_id) FILTER (WHERE order_status='DELIVERED') AS delivered_orders,
       round(sum(CASE WHEN order_status='DELIVERED' THEN quantity*unit_price*(1-discount_pct/100.0) ELSE 0 END),2) AS net_revenue
FROM stg_orders o LEFT JOIN stg_stores s USING(store_id)
WHERE dq_status='PASS'
GROUP BY order_date, zone, store_id, store_name;
