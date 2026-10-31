-- Staging: orders. One row per order_id. Bad rows are kept and flagged (dq_status), never deleted.
CREATE OR REPLACE TABLE stg_orders AS
WITH base AS (
    SELECT
        trim(order_id)                          AS order_id,
        TRY_CAST(trim(order_ts) AS TIMESTAMP)   AS order_ts,
        trim(customer_id)                       AS customer_id,
        trim(store_id)                          AS store_id,
        trim(product_id)                        AS product_id,
        TRY_CAST(trim(quantity) AS DOUBLE)      AS quantity,
        TRY_CAST(trim(unit_price) AS DOUBLE)    AS unit_price,
        TRY_CAST(trim(discount_pct) AS DOUBLE)  AS discount_pct,
        TRY_CAST(trim(order_amount) AS DOUBLE)  AS order_amount,
        trim(payment_mode)                      AS payment_mode_raw,
        CASE lower(trim(payment_mode))
            WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet'
        END                                     AS payment_mode,
        upper(trim(order_status))               AS order_status,
        trim(channel)                           AS channel,
        TRY_CAST(trim(delivery_minutes) AS DOUBLE) AS delivery_minutes,
        _source_file, _run_id, _loaded_at,
        row_number() OVER (PARTITION BY trim(order_id) ORDER BY rowid) AS rn
    FROM raw_orders
    WHERE _run_id = '{{run_id}}' AND _source_file = 'orders.csv'
),
deduped AS (
    SELECT * FROM base WHERE rn = 1 OR order_id = ''     -- later copies of an order_id stay in raw only
),
checked AS (
    SELECT *,
        concat_ws(';',
            -- Completeness
            CASE WHEN order_id = '' THEN 'order_id_not_null' END,
            CASE WHEN customer_id = '' THEN 'customer_id_not_null' END,
            CASE WHEN store_id = '' THEN 'store_id_not_null' END,
            CASE WHEN product_id = '' THEN 'product_id_not_null' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL
                 THEN 'delivered_has_delivery_minutes' END,
            -- Validity
            CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'order_status_allowed' END,
            CASE WHEN payment_mode IS NULL THEN 'payment_mode_allowed' END,
            CASE WHEN NOT coalesce(discount_pct BETWEEN 0 AND 100, false) THEN 'discount_pct_0_100' END,
            CASE WHEN order_ts IS NULL THEN 'order_ts_parses' END,
            -- Accuracy
            CASE WHEN NOT coalesce(quantity > 0, false) THEN 'quantity_positive' END,
            CASE WHEN NOT coalesce(unit_price > 0, false) THEN 'unit_price_positive' END,
            CASE WHEN NOT coalesce(abs(round(quantity * unit_price * (1 - discount_pct / 100), 2)
                                       - order_amount) <= 0.01, false)
                 THEN 'order_amount_matches_formula' END,
            CASE WHEN delivery_minutes IS NOT NULL AND (delivery_minutes >= 120 OR delivery_minutes < 0)
                 THEN 'delivery_minutes_plausible' END,
            -- Consistency (checked against ALL master ids, rejected ones included)
            CASE WHEN customer_id <> '' AND customer_id NOT IN (SELECT customer_id FROM stg_customers)
                 THEN 'customer_id_exists' END,
            CASE WHEN product_id <> '' AND product_id NOT IN (SELECT product_id FROM stg_products)
                 THEN 'product_id_exists' END,
            CASE WHEN store_id <> '' AND store_id NOT IN (SELECT store_id FROM stg_stores)
                 THEN 'store_id_exists' END,
            -- Timeliness
            CASE WHEN order_ts > CAST(current_timestamp AS TIMESTAMP) THEN 'order_ts_not_future' END
        ) AS failure_reason,
        concat_ws(';',
            CASE WHEN payment_mode IS NOT NULL
                  AND payment_mode_raw NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
                 THEN 'payment_mode_needs_standardising' END,
            CASE WHEN delivery_minutes IS NOT NULL AND NOT (delivery_minutes BETWEEN 8 AND 35)
                  AND delivery_minutes < 120 AND delivery_minutes >= 0
                 THEN 'delivery_minutes_in_8_35' END
        ) AS warning_reason
    FROM deduped
)
SELECT
    order_id, order_ts, CAST(order_ts AS DATE) AS order_date,
    customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel, delivery_minutes,
    CASE WHEN failure_reason <> '' THEN 'REJECTED'
         WHEN warning_reason <> '' THEN 'WARNING'
         ELSE 'PASSED' END AS dq_status,
    failure_reason, warning_reason,
    _source_file, _run_id, _loaded_at
FROM checked;