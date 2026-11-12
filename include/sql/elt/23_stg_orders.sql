-- Staging: orders batch. Runs for one source file ({{source_file}}) and writes {{target}}.
--   full load : target = stg_orders_batch, then 24_stg_orders_publish.sql replaces stg_orders
--   increment : same batch logic, then 25_stg_orders_merge.sql MERGEs it into stg_orders
-- Every raw row stays in the batch; bad rows are flagged (dq_status = FAIL + failure_reason), never dropped.
CREATE OR REPLACE TABLE {{target}} AS
WITH std AS (
    SELECT r._row_num, r._source_file, r._run_id,
           trim(order_id) AS order_id, trim(order_ts) AS order_ts_text,
           try_strptime(trim(order_ts), '%Y-%m-%d %H:%M:%S') AS order_ts,
           trim(customer_id) AS customer_id, trim(store_id) AS store_id, trim(product_id) AS product_id,
           TRY_CAST(trim(quantity) AS DOUBLE) AS quantity, TRY_CAST(trim(unit_price) AS DOUBLE) AS unit_price,
           TRY_CAST(trim(discount_pct) AS DOUBLE) AS discount_pct, TRY_CAST(trim(order_amount) AS DOUBLE) AS order_amount,
           CASE lower(trim(payment_mode))
                WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
                WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
                ELSE trim(payment_mode) END AS payment_mode,
           upper(trim(order_status)) AS order_status, trim(channel) AS channel,
           TRY_CAST(trim(delivery_minutes) AS DOUBLE) AS delivery_minutes, pm.catalogue_mrp
    FROM v_latest_raw_orders r
    LEFT JOIN (SELECT product_id AS pid, mrp AS catalogue_mrp FROM stg_products WHERE dq_status <> 'FAIL') pm
           ON pm.pid = trim(r.product_id)
    WHERE r._source_file = '{{source_file}}'
), flags AS (
    SELECT *,
        row_number() OVER (PARTITION BY order_id ORDER BY _row_num)          AS dup_seq,
        coalesce(order_id, '') = ''                                          AS chk_orders_order_id_not_null,
        coalesce(customer_id, '') = ''                                       AS chk_orders_customer_id_not_null,
        coalesce(order_ts_text, '') = ''                                     AS chk_orders_order_ts_not_null,
        order_status = 'DELIVERED' AND delivery_minutes IS NULL              AS chk_orders_delivered_has_delivery_minutes,
        row_number() OVER (PARTITION BY order_id ORDER BY _row_num) > 1      AS chk_orders_order_id_unique,
        order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')           AS chk_orders_status_valid,
        payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')    AS chk_orders_payment_mode_valid,
        order_ts IS NULL AND coalesce(order_ts_text, '') <> ''               AS chk_orders_order_ts_parses,
        NOT coalesce(discount_pct BETWEEN 0 AND 100, false)                  AS chk_orders_discount_pct_0_100,
        coalesce(channel, '') NOT IN ('App', 'Web')                          AS chk_orders_channel_valid,
        NOT coalesce(quantity > 0, false)                                    AS chk_orders_quantity_positive,
        NOT coalesce(unit_price > 0, false)                                  AS chk_orders_unit_price_positive,
        NOT coalesce(abs(round(quantity * unit_price * (1 - discount_pct / 100), 2) - order_amount) <= 0.01, false)
                                                                             AS chk_orders_amount_matches_formula,
        coalesce(delivery_minutes >= 120, false)                             AS chk_orders_delivery_minutes_below_120,
        order_status = 'DELIVERED' AND delivery_minutes IS NOT NULL
            AND (delivery_minutes < 8 OR delivery_minutes > 35)              AS chk_orders_delivery_minutes_normal_8_35,
        coalesce(unit_price > 1.1 * catalogue_mrp, false)                    AS chk_orders_unit_price_within_mrp,
        coalesce(customer_id, '') <> ''
            AND customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE dq_status <> 'FAIL')
                                                                             AS chk_orders_customer_in_clean_customers,
        product_id NOT IN (SELECT product_id FROM stg_products WHERE dq_status <> 'FAIL')
                                                                             AS chk_orders_product_in_clean_products,
        store_id NOT IN (SELECT store_id FROM stg_stores WHERE dq_status <> 'FAIL')
                                                                             AS chk_orders_store_in_clean_stores,
        coalesce(order_ts > TIMESTAMP '{{as_of}}', false)                    AS chk_orders_order_ts_not_future
    FROM std
)
SELECT *,
    concat_ws('; ',
        CASE WHEN chk_orders_order_id_not_null THEN 'orders_order_id_not_null' END,
        CASE WHEN chk_orders_customer_id_not_null THEN 'orders_customer_id_not_null' END,
        CASE WHEN chk_orders_order_ts_not_null THEN 'orders_order_ts_not_null' END,
        CASE WHEN chk_orders_delivered_has_delivery_minutes THEN 'orders_delivered_has_delivery_minutes' END,
        CASE WHEN chk_orders_order_id_unique THEN 'orders_order_id_unique' END,
        CASE WHEN chk_orders_status_valid THEN 'orders_status_valid' END,
        CASE WHEN chk_orders_payment_mode_valid THEN 'orders_payment_mode_valid' END,
        CASE WHEN chk_orders_order_ts_parses THEN 'orders_order_ts_parses' END,
        CASE WHEN chk_orders_discount_pct_0_100 THEN 'orders_discount_pct_0_100' END,
        CASE WHEN chk_orders_quantity_positive THEN 'orders_quantity_positive' END,
        CASE WHEN chk_orders_unit_price_positive THEN 'orders_unit_price_positive' END,
        CASE WHEN chk_orders_amount_matches_formula THEN 'orders_amount_matches_formula' END,
        CASE WHEN chk_orders_delivery_minutes_below_120 THEN 'orders_delivery_minutes_below_120' END,
        CASE WHEN chk_orders_customer_in_clean_customers THEN 'orders_customer_in_clean_customers' END,
        CASE WHEN chk_orders_product_in_clean_products THEN 'orders_product_in_clean_products' END,
        CASE WHEN chk_orders_store_in_clean_stores THEN 'orders_store_in_clean_stores' END,
        CASE WHEN chk_orders_order_ts_not_future THEN 'orders_order_ts_not_future' END) AS failure_reason,
    concat_ws('; ',
        CASE WHEN chk_orders_channel_valid THEN 'orders_channel_valid' END,
        CASE WHEN chk_orders_delivery_minutes_normal_8_35 THEN 'orders_delivery_minutes_normal_8_35' END,
        CASE WHEN chk_orders_unit_price_within_mrp THEN 'orders_unit_price_within_mrp' END) AS warning_reason
FROM flags;

-- dq_status is derived from the reasons so the rule list lives in exactly one place
ALTER TABLE {{target}} ADD COLUMN dq_status VARCHAR;
UPDATE {{target}} SET dq_status = CASE WHEN failure_reason <> '' THEN 'FAIL'
                                       WHEN warning_reason <> '' THEN 'WARN' ELSE 'PASS' END;
