-- Table macro: type, standardise and DQ-flag order rows from raw_orders for one run
-- and one file pattern. Used for both the daily snapshot (orders.csv) and the
-- next-day increment (orders_%_increment.csv) so both obey identical rules.
-- Requires stg_stores, stg_products and stg_customers to exist.
CREATE OR REPLACE MACRO validate_orders(p_run_id, p_file_pattern, p_cutoff) AS TABLE
WITH src AS (
    SELECT * FROM raw_orders WHERE _run_id = p_run_id AND _source_file LIKE p_file_pattern
),
typed AS (
    SELECT
        trim(order_id)                                            AS order_id,
        try_strptime(trim(order_ts), '%Y-%m-%d %H:%M:%S')         AS order_ts,
        nullif(trim(customer_id), '')                             AS customer_id,
        nullif(trim(store_id), '')                                AS store_id,
        nullif(trim(product_id), '')                              AS product_id,
        try_cast(trim(quantity) AS DOUBLE)                        AS quantity,
        try_cast(trim(unit_price) AS DOUBLE)                      AS unit_price,
        try_cast(trim(discount_pct) AS DOUBLE)                    AS discount_pct,
        try_cast(trim(order_amount) AS DOUBLE)                    AS order_amount,
        std_payment_mode(payment_mode)                            AS payment_mode,
        upper(trim(order_status))                                 AS order_status,
        trim(channel)                                             AS channel,
        try_cast(trim(delivery_minutes) AS DOUBLE)                AS delivery_minutes,
        trim(order_status)                                        AS raw_order_status,
        trim(payment_mode)                                        AS raw_payment_mode,
        _row_num, _source_file, _run_id
    FROM src
),
clean_products AS (SELECT product_id, mrp FROM stg_products WHERE dq_status = 'PASSED'),
clean_stores AS (SELECT store_id FROM stg_stores WHERE dq_status = 'PASSED'),
customer_ids AS (SELECT DISTINCT customer_id FROM stg_customers WHERE customer_id IS NOT NULL),
flags AS (
    SELECT t.*,
        t.customer_id IS NULL                                                        AS f_ord_customer_id_not_null,
        coalesce(t.order_status = 'DELIVERED' AND t.delivery_minutes IS NULL, false) AS f_ord_delivered_has_minutes,
        row_number() OVER (PARTITION BY t.order_id ORDER BY t._source_file, t._row_num) > 1 AS f_ord_order_id_unique,
        NOT coalesce(t.order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED'), false) AS f_ord_status_valid,
        NOT coalesce(t.payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), false) AS f_ord_payment_mode_valid,
        t.order_ts IS NULL                                                           AS f_ord_ts_parseable,
        NOT coalesce(t.discount_pct BETWEEN 0 AND 100, false)                        AS f_ord_discount_in_range,
        NOT coalesce(t.quantity > 0, false)                                          AS f_ord_quantity_positive,
        NOT coalesce(abs(t.quantity * t.unit_price * (1 - t.discount_pct / 100) - t.order_amount) <= 0.01, false)
                                                                                     AS f_ord_amount_matches_formula,
        coalesce(t.delivery_minutes >= 120, false)                                   AS f_ord_delivery_under_limit,
        t.store_id IS NULL OR t.store_id NOT IN (SELECT store_id FROM clean_stores)  AS f_ord_store_exists,
        p.product_id IS NULL                                                         AS f_ord_product_exists,
        t.customer_id IS NOT NULL
            AND t.customer_id NOT IN (SELECT customer_id FROM customer_ids)          AS f_ord_customer_exists,
        coalesce(t.order_ts >= CAST(p_cutoff AS TIMESTAMP), false)                   AS f_ord_ts_not_future,
        coalesce(t.order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED')
                 AND t.raw_order_status <> t.order_status, false)
          OR coalesce(t.payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
                 AND t.raw_payment_mode <> t.payment_mode, false)                    AS f_ord_value_casing,
        coalesce(t.unit_price > 3.0 * p.mrp, false)                                  AS f_ord_price_drift
    FROM typed t
    LEFT JOIN clean_products p ON p.product_id = t.product_id
)
SELECT
    order_id, order_ts, CAST(order_ts AS DATE) AS order_date, customer_id, store_id, product_id,
    CAST(quantity AS INTEGER) AS quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel, delivery_minutes,
    f_ord_customer_id_not_null, f_ord_delivered_has_minutes, f_ord_order_id_unique,
    f_ord_status_valid, f_ord_payment_mode_valid, f_ord_ts_parseable, f_ord_discount_in_range,
    f_ord_quantity_positive, f_ord_amount_matches_formula, f_ord_delivery_under_limit,
    f_ord_store_exists, f_ord_product_exists, f_ord_customer_exists, f_ord_ts_not_future,
    f_ord_value_casing, f_ord_price_drift,
    nullif(concat_ws('; ',
        CASE WHEN f_ord_customer_id_not_null THEN 'customer_id is null' END,
        CASE WHEN f_ord_delivered_has_minutes THEN 'DELIVERED order has no delivery_minutes' END,
        CASE WHEN f_ord_order_id_unique THEN 'duplicate order_id' END,
        CASE WHEN f_ord_status_valid THEN 'order_status not in allowed list' END,
        CASE WHEN f_ord_payment_mode_valid THEN 'payment_mode not in allowed list' END,
        CASE WHEN f_ord_ts_parseable THEN 'order_ts is not a valid timestamp' END,
        CASE WHEN f_ord_discount_in_range THEN 'discount_pct outside 0-100' END,
        CASE WHEN f_ord_quantity_positive THEN 'quantity <= 0' END,
        CASE WHEN f_ord_amount_matches_formula THEN 'order_amount != quantity x unit_price x (1 - discount)' END,
        CASE WHEN f_ord_delivery_under_limit THEN 'delivery_minutes >= 120' END,
        CASE WHEN f_ord_store_exists THEN 'store_id not in store master' END,
        CASE WHEN f_ord_product_exists THEN 'product_id not in clean product master' END,
        CASE WHEN f_ord_customer_exists THEN 'customer_id not in customer master' END,
        CASE WHEN f_ord_ts_not_future THEN 'order_ts is in the future' END), '') AS failure_reason,
    nullif(concat_ws('; ',
        CASE WHEN f_ord_value_casing THEN 'order_status/payment_mode needed standardising' END,
        CASE WHEN f_ord_price_drift THEN 'unit_price more than 3x catalogue MRP' END), '') AS dq_warnings,
    CASE WHEN f_ord_customer_id_not_null OR f_ord_delivered_has_minutes OR f_ord_order_id_unique
           OR f_ord_status_valid OR f_ord_payment_mode_valid OR f_ord_ts_parseable
           OR f_ord_discount_in_range OR f_ord_quantity_positive OR f_ord_amount_matches_formula
           OR f_ord_delivery_under_limit OR f_ord_store_exists OR f_ord_product_exists
           OR f_ord_customer_exists OR f_ord_ts_not_future
         THEN 'FAILED' ELSE 'PASSED' END AS dq_status,
    _row_num, _source_file, _run_id
FROM flags;
