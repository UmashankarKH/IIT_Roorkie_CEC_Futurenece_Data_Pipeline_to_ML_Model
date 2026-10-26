CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    round(avg(o.delivery_minutes), 2) AS avg_delivery_minutes,
    round(
        100.0 * sum(CASE WHEN o.delivery_minutes <= 15 THEN 1 ELSE 0 END)
        / nullif(count(*), 0),
        2
    ) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2
ORDER BY avg_delivery_minutes;
