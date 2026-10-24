-- Daily finance-safe revenue by zone and store, using only passing delivered orders.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    o.store_id,
    sum(o.net_revenue) AS daily_net_revenue,
    count(*) AS delivered_orders
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id AND s.dq_status = 'PASS'
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2, 3;
