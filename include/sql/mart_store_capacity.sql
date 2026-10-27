-- Bonus: daily orders per store vs capacity_orders_per_day from the store master.
-- peak_utilisation_pct is the busiest day as a % of capacity (NULL when capacity is 0).

CREATE OR REPLACE TABLE mart_store_capacity AS
WITH daily AS (
    SELECT store_id, order_date, count(*) AS orders
    FROM stg_orders
    WHERE dq_status = 'PASSED'
    GROUP BY ALL
)

SELECT
    s.store_id,
    s.store_name,
    s.zone,
    s.capacity_orders_per_day,
    count(*) AS days_with_orders,
    round(avg(d.orders), 1) AS avg_daily_orders,
    max(d.orders) AS max_daily_orders,
    count(*) FILTER (WHERE d.orders > s.capacity_orders_per_day) AS days_over_capacity,
    round(100.0 * max(d.orders) / nullif(s.capacity_orders_per_day, 0), 1) AS peak_utilisation_pct
FROM daily d
JOIN stg_stores s ON s.store_id = d.store_id AND s.dq_status = 'PASSED'
GROUP BY 1, 2, 3, 4
ORDER BY days_over_capacity DESC, peak_utilisation_pct DESC;
