-- include/sql/mart/mart_daily_store_kpis.sql
-- ELT layer 3: MART (assignment Part B) - answers business question 1
--
-- "Daily net revenue by zone and by store (delivered orders only, after discount)."
--
-- Reads  : stg_orders (only rows with dq_status = 'PASS'), stg_stores, stg_products
-- Filters: delivered orders only - CANCELLED and RETURNED are not revenue
-- Returns: one row per order_date + store, with revenue, margin and SLA columns
-- Why    : this is the table the 9:00 AM operations review reads first.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    coalesce(s.zone, 'Unknown')                                        AS zone,
    o.store_id,
    coalesce(s.store_name, 'UNKNOWN (master row quarantined)')         AS store_name,
    count(*)                                                           AS delivered_orders,
    round(sum(o.net_revenue), 2)                                       AS net_revenue,
    round(sum(coalesce(o.quantity, 0) * coalesce(p.cost_price, 0)), 2) AS cost_of_goods,
    round(sum(o.net_revenue)
          - sum(coalesce(o.quantity, 0) * coalesce(p.cost_price, 0)), 2) AS gross_margin,
    round(avg(o.delivery_minutes), 2)                                  AS avg_delivery_minutes,
    sum(CASE WHEN o.within_15_min THEN 1 ELSE 0 END)                   AS orders_within_15_min,
    round(100.0 * sum(CASE WHEN o.within_15_min THEN 1 ELSE 0 END) / count(*), 2)
                                                                       AS pct_within_15_min,
    max(s.capacity_orders_per_day)                                     AS capacity_orders_per_day,
    -- bonus check: did the store take more orders than it can handle?
    -- A capacity of zero or null means the store master is wrong (RULE-031),
    -- not that the store is overloaded, so those rows are not flagged here.
    (max(s.capacity_orders_per_day) > 0
     AND count(*) > max(s.capacity_orders_per_day))                    AS over_capacity
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_stores WHERE dq_status = 'PASS') s ON s.store_id = o.store_id
LEFT JOIN (SELECT * FROM stg_products WHERE dq_status = 'PASS') p ON p.product_id = o.product_id
WHERE o.dq_status = 'PASS'
  AND o.is_revenue
GROUP BY o.order_date, coalesce(s.zone, 'Unknown'), o.store_id,
         coalesce(s.store_name, 'UNKNOWN (master row quarantined)')
ORDER BY o.order_date, zone, o.store_id;
