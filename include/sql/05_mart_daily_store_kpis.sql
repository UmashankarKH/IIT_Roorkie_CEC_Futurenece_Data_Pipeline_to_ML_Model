-- Daily revenue and operational KPIs by store and zone.
-- Only DELIVERED orders contribute to revenue.

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    o.store_id,
    s.store_name,
    s.zone,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (
        WHERE o.order_status = 'DELIVERED'
    ) AS delivered_orders,
    COUNT(*) FILTER (
        WHERE o.order_status = 'CANCELLED'
    ) AS cancelled_orders,
    COUNT(*) FILTER (
        WHERE o.order_status = 'RETURNED'
    ) AS returned_orders,
    ROUND(
        COALESCE(
            SUM(o.order_amount) FILTER (
                WHERE o.order_status = 'DELIVERED'
            ), 0
        ), 2
    ) AS net_revenue
FROM stg_orders o
JOIN stg_stores s USING (store_id)
GROUP BY CAST(o.order_ts AS DATE), o.store_id, s.store_name, s.zone;
