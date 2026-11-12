-- Standardise one orders file from the raw layer (used for orders.csv and for the increment).
-- Reads only the rows THIS run loaded; raw keeps every earlier load untouched.
-- Column names and meaning match standardise("orders") in include/dq_rules.py.
CREATE OR REPLACE TABLE {{ std_table }} AS
SELECT
    _row,
    nm_text(order_id)            AS order_id,
    nm_ts(order_ts)              AS order_ts,
    nm_raw(order_ts)             AS order_ts_raw,
    nm_text(customer_id)         AS customer_id,
    nm_text(store_id)            AS store_id,
    nm_text(product_id)          AS product_id,
    nm_num(quantity)             AS quantity,
    nm_num(unit_price)           AS unit_price,
    nm_num(discount_pct)         AS discount_pct,
    nm_num(order_amount)         AS order_amount,
    nm_payment_mode(payment_mode) AS payment_mode,
    nm_raw(payment_mode)         AS payment_mode_raw,
    nm_order_status(order_status) AS order_status,
    nm_raw(order_status)         AS order_status_raw,
    nm_text(channel)             AS channel,
    nm_num(delivery_minutes)     AS delivery_minutes,
    _source_file, _run_id, _loaded_at
FROM raw_orders
WHERE _run_id = '{{ run_id }}' AND _source_file = '{{ source_file }}';
