-- Business question 3: average delivery time per store and % of delivered orders within 15 minutes
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    zone, store_id, store_name,
    count(*)                                                        AS delivered_orders,
    round(avg(delivery_minutes), 1)                                 AS avg_delivery_minutes,
    round(median(delivery_minutes), 1)                              AS median_delivery_minutes,
    max(delivery_minutes)                                           AS slowest_delivery_minutes,
    count(*) FILTER (WHERE within_15_min)                           AS within_15_min_orders,
    round(100.0 * count(*) FILTER (WHERE within_15_min) / count(*), 1) AS pct_within_15_min
FROM int_orders_enriched
WHERE order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY pct_within_15_min DESC, store_id;
