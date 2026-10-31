-- Mart: daily net revenue and delivery KPIs by zone and store (delivered orders only, after discount).
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    coalesce(nullif(s.zone, ''), 'UNKNOWN')    AS zone,
    o.store_id,
    coalesce(s.store_name, 'UNKNOWN')          AS store_name,
    count(*)                                   AS delivered_orders,
    round(sum(o.order_amount), 2)              AS net_revenue,
    round(avg(o.delivery_minutes), 1)          AS avg_delivery_minutes,
    count(*) FILTER (WHERE o.delivery_minutes <= 15) AS orders_within_15_min,
    round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 15) / count(*), 1) AS pct_within_15_min
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_stores WHERE dq_status <> 'REJECTED') s ON s.store_id = o.store_id
WHERE o.dq_status <> 'REJECTED'
  AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, coalesce(nullif(s.zone, ''), 'UNKNOWN'), o.store_id, coalesce(s.store_name, 'UNKNOWN');