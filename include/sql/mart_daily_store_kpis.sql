-- Business question 1: daily net revenue by zone and by store (delivered orders only, after discount).
-- Also carries delivery KPIs per store per day for the 9:30 stand-up.

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    o.store_id,
    s.store_name,
    count(*) AS total_orders,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
    count(*) FILTER (WHERE o.order_status = 'CANCELLED') AS cancelled_orders,
    count(*) FILTER (WHERE o.order_status = 'RETURNED') AS returned_orders,
    round(sum(o.net_revenue), 2) AS net_revenue,
    round(sum(o.gross_margin), 2) AS gross_margin,
    round(avg(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED'), 1) AS avg_delivery_minutes,
    round(100.0 * count(*) FILTER (WHERE o.within_15_min)
          / nullif(count(*) FILTER (WHERE o.order_status = 'DELIVERED'), 0), 1) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED'
GROUP BY ALL
ORDER BY o.order_date, s.zone, o.store_id;
