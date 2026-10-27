-- Business question 3: average delivery time per store and % of delivered orders within 15 minutes.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.zone,
    o.store_id,
    s.store_name,
    count(*) AS delivered_orders,
    round(avg(o.delivery_minutes), 1) AS avg_delivery_minutes,
    count(*) FILTER (WHERE o.within_15_min) AS orders_within_15_min,
    round(100.0 * count(*) FILTER (WHERE o.within_15_min) / count(*), 1) AS pct_within_15_min,
    count(*) FILTER (WHERE o.delivery_minutes > 35) AS orders_over_35_min
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY pct_within_15_min DESC;
