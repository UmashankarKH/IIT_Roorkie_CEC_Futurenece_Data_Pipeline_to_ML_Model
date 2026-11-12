-- Business question 1: daily net revenue by zone and by store (delivered orders, after discount)
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    order_date, zone, store_id, store_name,
    count(*)                                              AS total_orders,
    count(*) FILTER (WHERE order_status = 'DELIVERED')    AS delivered_orders,
    count(*) FILTER (WHERE order_status = 'CANCELLED')    AS cancelled_orders,
    count(*) FILTER (WHERE order_status = 'RETURNED')     AS returned_orders,
    round(sum(net_revenue), 2)                            AS net_revenue,
    round(sum(gross_margin), 2)                           AS gross_margin,
    round(avg(delivery_minutes) FILTER (WHERE order_status = 'DELIVERED'), 1) AS avg_delivery_minutes
FROM int_orders_enriched
GROUP BY ALL
ORDER BY order_date, zone, store_id;
