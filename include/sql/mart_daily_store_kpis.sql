-- MART: daily KPIs per store. Only orders that passed every Critical check.
-- net_revenue counts DELIVERED orders only (after discount). Margin = revenue - quantity x cost_price.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT CAST(o.order_ts AS DATE) AS order_date, s.zone, o.store_id, s.store_name,
       count(*) AS orders_total,
       count(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
       round(sum(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS net_revenue,
       round(sum(CASE WHEN o.order_status = 'DELIVERED' THEN round(o.order_amount - o.quantity * p.cost_price, 2) END), 2) AS gross_margin
FROM stg_orders o
LEFT JOIN stg_stores s ON s.store_id = o.store_id AND s.dim_usable
LEFT JOIN stg_products p ON p.product_id = o.product_id AND p.dim_usable
WHERE o.dq_status = 'PASS'
GROUP BY CAST(o.order_ts AS DATE), s.zone, o.store_id, s.store_name;
