-- Business question 1 (ELT): daily net revenue by zone and by store.
-- Source: mart_daily_store_kpis, which is built in SQL from stg_orders (PASS rows only).
SELECT order_date,
       zone,
       store_id,
       store_name,
       delivered_orders,
       net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date DESC, net_revenue DESC
LIMIT 10;
