-- Business question 3 (ETL): average delivery time per store and the share of
-- delivered orders inside the 15-minute promise.
SELECT store_id,
       store_name,
       zone,
       count(*)                                                     AS delivered_orders,
       round(avg(delivery_minutes), 2)                              AS avg_delivery_minutes,
       sum(CASE WHEN within_15_min THEN 1 ELSE 0 END)               AS orders_within_15_min,
       round(100.0 * sum(CASE WHEN within_15_min THEN 1 ELSE 0 END) / count(*), 2)
                                                                    AS pct_within_15_min
FROM etl_fact_orders
WHERE is_revenue AND delivery_minutes IS NOT NULL
GROUP BY store_id, store_name, zone
ORDER BY pct_within_15_min DESC;
