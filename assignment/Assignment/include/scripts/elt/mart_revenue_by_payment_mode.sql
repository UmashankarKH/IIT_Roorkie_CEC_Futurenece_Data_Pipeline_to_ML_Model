CREATE OR REPLACE TABLE mart_revenue_by_payment_mode AS
SELECT payment_mode_clean payment_mode,COUNT(*) delivered_orders,SUM(order_amount_clean) net_revenue
FROM stg_orders WHERE is_valid AND order_status_clean='DELIVERED' GROUP BY payment_mode_clean;
