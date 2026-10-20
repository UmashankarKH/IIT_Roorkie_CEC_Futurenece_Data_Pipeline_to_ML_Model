CREATE OR REPLACE TABLE increment_typed AS
SELECT *, TRY_CAST(NULLIF(TRIM(order_ts),'') AS TIMESTAMP) AS order_ts_typed,
 TRY_CAST(NULLIF(TRIM(quantity),'') AS INTEGER) AS quantity_typed,
 TRY_CAST(NULLIF(TRIM(unit_price),'') AS DOUBLE) AS unit_price_typed,
 TRY_CAST(NULLIF(TRIM(discount_pct),'') AS DOUBLE) AS discount_pct_typed,
 TRY_CAST(NULLIF(TRIM(order_amount),'') AS DOUBLE) AS order_amount_typed,
 TRY_CAST(NULLIF(TRIM(delivery_minutes),'') AS DOUBLE) AS delivery_minutes_typed
FROM raw_orders_increment;
