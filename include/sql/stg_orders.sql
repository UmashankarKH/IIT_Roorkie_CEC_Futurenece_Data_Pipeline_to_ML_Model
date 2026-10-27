-- {{target}}: cleaned, typed and quality-checked copy of raw_orders for ONE run.
-- Rows that fail a Critical check stay here with dq_status = 'FAIL' (marts ignore them).
CREATE OR REPLACE TABLE {{target}} AS
WITH src AS (
  SELECT trim(order_id) AS order_id, trim(order_ts) AS order_ts_raw, trim(customer_id) AS customer_id, trim(store_id) AS store_id,
    trim(product_id) AS product_id, try_cast(trim(quantity) AS DOUBLE) AS qty_num, try_cast(trim(unit_price) AS DOUBLE) AS price_num,
    try_cast(trim(discount_pct) AS DOUBLE) AS disc_num, try_cast(trim(order_amount) AS DOUBLE) AS amount_num,
    trim(payment_mode) AS pm_raw, trim(order_status) AS status_raw, trim(channel) AS channel,
    trim(delivery_minutes) AS dm_raw, try_cast(trim(delivery_minutes) AS DOUBLE) AS dm_num,
    _source_row, _source_file, _run_id
  FROM raw_orders
  WHERE _run_id = '{{run_id}}' AND {{file_filter}}
),
typed AS (
  SELECT src.*,
    try_strptime(order_ts_raw, '%Y-%m-%d %H:%M:%S') AS order_ts,
    CASE WHEN upper(status_raw) IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN upper(status_raw) END AS order_status,
    CASE lower(pm_raw) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cash on delivery' THEN 'Cash on Delivery'
         WHEN 'cod' THEN 'Cash on Delivery' WHEN 'cashh' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' END AS payment_mode,
    row_number() OVER (PARTITION BY order_id ORDER BY _source_row) AS _rn
  FROM src
),
flagged AS (
  SELECT typed.*,
    coalesce((order_id = ''), false) AS chk_order_id_not_null,
    coalesce((customer_id = ''), false) AS chk_customer_id_not_null,
    coalesce((order_status = 'DELIVERED' AND dm_raw = ''), false) AS chk_delivered_has_delivery_minutes,
    coalesce((order_id <> '' AND _rn > 1), false) AS chk_order_id_unique,
    coalesce((order_status IS NULL), false) AS chk_order_status_valid,
    coalesce((order_status IS NOT NULL AND status_raw <> order_status), false) AS chk_order_status_needs_standardising,
    coalesce((payment_mode IS NULL), false) AS chk_payment_mode_valid,
    coalesce((payment_mode IS NOT NULL AND pm_raw <> payment_mode), false) AS chk_payment_mode_needs_standardising,
    coalesce((order_ts IS NULL), false) AS chk_order_ts_valid,
    coalesce((order_ts > CAST('{{as_of}}' AS TIMESTAMP)), false) AS chk_order_ts_not_future,
    coalesce((NOT coalesce(qty_num > 0, false)), false) AS chk_quantity_positive,
    coalesce((NOT coalesce(price_num > 0, false)), false) AS chk_unit_price_positive,
    coalesce((NOT coalesce(disc_num BETWEEN 0 AND 100, false)), false) AS chk_discount_pct_in_range,
    coalesce((NOT coalesce(abs(round(qty_num * price_num * (1 - disc_num / 100.0), 2) - amount_num) <= 0.01, false)), false) AS chk_order_amount_matches_formula,
    coalesce((coalesce(dm_num >= 120, false)), false) AS chk_delivery_minutes_below_120,
    coalesce((coalesce(dm_num < 8 OR (dm_num > 35 AND dm_num < 120), false)), false) AS chk_delivery_minutes_normal_range,
    coalesce((channel NOT IN ('App', 'Web')), false) AS chk_channel_valid,
    coalesce((customer_id <> '' AND customer_id NOT IN (SELECT trim(customer_id) FROM raw_customers WHERE _run_id = '{{run_id}}')), false) AS chk_customer_id_in_master,
    coalesce((product_id NOT IN (SELECT trim(product_id) FROM raw_products WHERE _run_id = '{{run_id}}')), false) AS chk_product_id_in_master,
    coalesce((store_id NOT IN (SELECT trim(store_id) FROM raw_stores WHERE _run_id = '{{run_id}}')), false) AS chk_store_id_in_master
  FROM typed
),
scored AS (
  SELECT flagged.*,
    concat_ws(', ', CASE WHEN chk_order_id_not_null THEN 'order_id_not_null' END, CASE WHEN chk_customer_id_not_null THEN 'customer_id_not_null' END, CASE WHEN chk_delivered_has_delivery_minutes THEN 'delivered_has_delivery_minutes' END, CASE WHEN chk_order_id_unique THEN 'order_id_unique' END, CASE WHEN chk_order_status_valid THEN 'order_status_valid' END, CASE WHEN chk_payment_mode_valid THEN 'payment_mode_valid' END, CASE WHEN chk_order_ts_valid THEN 'order_ts_valid' END, CASE WHEN chk_order_ts_not_future THEN 'order_ts_not_future' END, CASE WHEN chk_quantity_positive THEN 'quantity_positive' END, CASE WHEN chk_unit_price_positive THEN 'unit_price_positive' END, CASE WHEN chk_discount_pct_in_range THEN 'discount_pct_in_range' END, CASE WHEN chk_order_amount_matches_formula THEN 'order_amount_matches_formula' END, CASE WHEN chk_delivery_minutes_below_120 THEN 'delivery_minutes_below_120' END, CASE WHEN chk_customer_id_in_master THEN 'customer_id_in_master' END, CASE WHEN chk_product_id_in_master THEN 'product_id_in_master' END, CASE WHEN chk_store_id_in_master THEN 'store_id_in_master' END) AS failed_checks,
    concat_ws('; ', CASE WHEN chk_order_id_not_null THEN 'order_id is missing' END, CASE WHEN chk_customer_id_not_null THEN 'customer_id is missing' END, CASE WHEN chk_delivered_has_delivery_minutes THEN 'DELIVERED order has no delivery_minutes' END, CASE WHEN chk_order_id_unique THEN 'duplicate order_id (repeat of an earlier row)' END, CASE WHEN chk_order_status_valid THEN 'order_status is not DELIVERED/CANCELLED/RETURNED' END, CASE WHEN chk_payment_mode_valid THEN 'payment_mode is not UPI/Card/Cash on Delivery/Wallet' END, CASE WHEN chk_order_ts_valid THEN 'order_ts is missing or not a valid timestamp' END, CASE WHEN chk_order_ts_not_future THEN 'order_ts is in the future' END, CASE WHEN chk_quantity_positive THEN 'quantity is zero, negative or not a number' END, CASE WHEN chk_unit_price_positive THEN 'unit_price is zero, negative or not a number' END, CASE WHEN chk_discount_pct_in_range THEN 'discount_pct is outside 0-100' END, CASE WHEN chk_order_amount_matches_formula THEN 'order_amount != quantity x unit_price x (1 - discount_pct/100)' END, CASE WHEN chk_delivery_minutes_below_120 THEN 'delivery_minutes is 120 or more (impossible for a 10-minute service)' END, CASE WHEN chk_customer_id_in_master THEN 'customer_id not found in customers' END, CASE WHEN chk_product_id_in_master THEN 'product_id not found in products' END, CASE WHEN chk_store_id_in_master THEN 'store_id not found in stores' END) AS failure_reason,
    concat_ws('; ', CASE WHEN chk_order_status_needs_standardising THEN 'order_status casing/whitespace will be standardised' END, CASE WHEN chk_payment_mode_needs_standardising THEN 'payment_mode spelling/casing will be standardised' END, CASE WHEN chk_delivery_minutes_normal_range THEN 'delivery_minutes outside the normal 8-35 range' END, CASE WHEN chk_channel_valid THEN 'channel is not App/Web' END) AS dq_warnings
  FROM flagged
)
SELECT
  order_id, order_ts, customer_id, store_id, product_id, CAST(qty_num AS BIGINT) AS quantity,
  price_num AS unit_price, disc_num AS discount_pct, round(amount_num, 2) AS order_amount,
  payment_mode, order_status, channel, dm_num AS delivery_minutes,
  chk_order_id_not_null, chk_customer_id_not_null, chk_delivered_has_delivery_minutes, chk_order_id_unique, chk_order_status_valid, chk_order_status_needs_standardising, chk_payment_mode_valid, chk_payment_mode_needs_standardising, chk_order_ts_valid, chk_order_ts_not_future, chk_quantity_positive, chk_unit_price_positive, chk_discount_pct_in_range, chk_order_amount_matches_formula, chk_delivery_minutes_below_120, chk_delivery_minutes_normal_range, chk_channel_valid, chk_customer_id_in_master, chk_product_id_in_master, chk_store_id_in_master,
  failed_checks, failure_reason, dq_warnings,
  CASE WHEN failed_checks = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
  _source_row, _source_file, _run_id
FROM scored;
