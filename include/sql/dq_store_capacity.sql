-- Bonus quality check: clean orders per store per day must not exceed the store's capacity.
-- One row checked per store-day. Warning only: an over-capacity day is an ops signal
-- (or a wrong capacity in the store master), not a reason to drop orders.

WITH daily AS (
    SELECT o.store_id, o.order_date, count(*) AS orders, any_value(s.capacity_orders_per_day) AS capacity
    FROM stg_orders o   -- after the MERGE: one current row per order
    JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'PASSED'
    WHERE o.dq_status = 'PASSED'
    GROUP BY ALL
)

SELECT
    'orders.csv' AS source_file,
    'store_daily_orders_within_capacity' AS check_name,
    'Consistency' AS dimension,
    'Warning' AS severity,
    count(*) AS rows_checked,
    count(*) FILTER (WHERE orders > capacity) AS rows_failed
FROM daily;
