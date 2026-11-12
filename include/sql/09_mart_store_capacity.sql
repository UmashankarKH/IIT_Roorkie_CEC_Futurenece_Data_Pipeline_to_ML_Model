-- Daily store-capacity monitoring.
-- All curated orders count toward operational capacity.
-- A capacity breach is a warning, not a data-quality rejection.

CREATE OR REPLACE TABLE mart_store_capacity AS
WITH daily AS (
    SELECT
        CAST(o.order_ts AS DATE) AS order_date,
        o.store_id,
        s.store_name,
        s.zone,
        TRY_CAST(s.capacity_orders_per_day AS DOUBLE)
            AS capacity_orders_per_day,
        COUNT(*) AS daily_orders
    FROM stg_orders o
    JOIN stg_stores s USING (store_id)
    GROUP BY
        CAST(o.order_ts AS DATE),
        o.store_id,
        s.store_name,
        s.zone,
        s.capacity_orders_per_day
)
SELECT
    order_date,
    store_id,
    store_name,
    zone,
    daily_orders,
    capacity_orders_per_day,
    ROUND(
        100.0 * daily_orders
        / NULLIF(capacity_orders_per_day, 0),
        2
    ) AS capacity_utilization_pct,
    daily_orders > capacity_orders_per_day AS capacity_breached
FROM daily;
