CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    cast(o.order_ts AS DATE) AS order_date,
    s.zone,
    s.store_id,
    s.store_name,
    round(sum(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3;
