-- Business question 3 (ELT): average delivery time per store and 15-minute SLA share.
SELECT store_id, store_name, zone, delivered_orders, avg_delivery_minutes,
       orders_within_15_min, pct_within_15_min
FROM mart_delivery_sla
ORDER BY pct_within_15_min DESC;
