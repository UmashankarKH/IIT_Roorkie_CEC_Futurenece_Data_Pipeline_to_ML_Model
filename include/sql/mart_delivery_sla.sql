CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT store_id, store_name,
       round(avg(delivery_minutes) FILTER (WHERE order_status='DELIVERED' AND delivery_minutes IS NOT NULL),2) AS avg_delivery_minutes,
       round(100.0*count(*) FILTER (WHERE order_status='DELIVERED' AND delivery_minutes<=15)
         / nullif(count(*) FILTER (WHERE order_status='DELIVERED' AND delivery_minutes IS NOT NULL),0),2) AS delivered_within_15_min_pct,
       count(*) FILTER (WHERE order_status='DELIVERED' AND delivery_minutes IS NULL) AS delivered_missing_time
FROM stg_orders o LEFT JOIN stg_stores s USING(store_id)
WHERE dq_status='PASS'
GROUP BY store_id, store_name;
