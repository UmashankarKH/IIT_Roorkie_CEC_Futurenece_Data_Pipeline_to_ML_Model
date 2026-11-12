-- Business question 1 (ETL): daily net revenue by zone and by store.
-- Delivered orders only, after discount. Source: etl_fact_orders (ETL star schema).
SELECT order_date,
       zone,
       store_id,
       store_name,
       count(*)                    AS delivered_orders,
       round(sum(net_revenue), 2)  AS net_revenue
FROM etl_fact_orders
WHERE is_revenue
GROUP BY order_date, zone, store_id, store_name
ORDER BY order_date DESC, net_revenue DESC
LIMIT 10;
