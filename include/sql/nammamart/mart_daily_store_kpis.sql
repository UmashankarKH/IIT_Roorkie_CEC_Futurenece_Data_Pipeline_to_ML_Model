-- include/sql/nammamart/mart_daily_store_kpis.sql
-- BQ1: Daily net revenue by zone and by store (DELIVERED orders, after discount)

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    o.store_id,
    s.store_name,
    s.capacity_orders_per_day,
    COUNT(*)                                                           AS order_count,
    ROUND(SUM(o.net_revenue), 2)                                       AS net_revenue,
    ROUND(SUM(o.net_revenue - o.quantity * p.cost_price), 2)          AS gross_margin,
    ROUND(AVG(o.delivery_minutes), 2)                                  AS avg_delivery_min,
    ROUND(100.0 * SUM(o.within_15_min) / COUNT(*), 2)                 AS pct_within_15_min,
    SUM(CASE WHEN o.order_count_day > s.capacity_orders_per_day
             THEN 1 ELSE 0 END)                                        AS over_capacity_days
FROM (
    SELECT o.*,
           COUNT(*) OVER (PARTITION BY o.store_id, o.order_date) AS order_count_day
    FROM stg_orders o
    WHERE o.dq_status = 'PASS'
      AND o.order_status = 'DELIVERED'
) o
LEFT JOIN stg_stores  s ON o.store_id  = s.store_id
LEFT JOIN stg_products p ON o.product_id = p.product_id
GROUP BY o.order_date, s.zone, o.store_id, s.store_name, s.capacity_orders_per_day
ORDER BY o.order_date, s.zone, o.store_id;
