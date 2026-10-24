-- Cast orders, standardise categorical values and calculate SQL quality flags.
CREATE OR REPLACE TABLE stg_orders AS
WITH latest_raw AS (
    -- Raw is append-only. This latest-row rule is the SQL equivalent of a
    -- MERGE on order_id: corrections overwrite the prior order version.
    -- Duplicates are counted within one load (before QUALIFY removes them),
    -- so a repeated order_id inside a single file still fails uniqueness.
    SELECT *, count(*) OVER (PARTITION BY order_id, _loaded_at) AS order_id_count
    FROM raw_orders
    QUALIFY row_number() OVER (
        PARTITION BY order_id
        ORDER BY try_cast(_loaded_at AS TIMESTAMP) DESC
    ) = 1
), typed AS (
    SELECT
        order_id,
        try_cast(order_ts AS TIMESTAMP) AS order_ts,
        customer_id,
        store_id,
        product_id,
        try_cast(quantity AS INTEGER) AS quantity,
        try_cast(unit_price AS DECIMAL(12, 2)) AS unit_price,
        try_cast(discount_pct AS DECIMAL(6, 2)) AS discount_pct,
        try_cast(order_amount AS DECIMAL(12, 2)) AS order_amount,
        CASE
            WHEN lower(trim(payment_mode)) = 'upi' THEN 'UPI'
            WHEN lower(trim(payment_mode)) IN ('card') THEN 'Card'
            WHEN lower(trim(payment_mode)) IN ('cod', 'cashh', 'cash on delivery') THEN 'Cash on Delivery'
            WHEN lower(trim(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE trim(payment_mode)
        END AS payment_mode,
        upper(trim(order_status)) AS order_status,
        channel,
        try_cast(delivery_minutes AS INTEGER) AS delivery_minutes,
        order_id_count
    FROM latest_raw
), checked AS (
    SELECT
        *,
        round(quantity * unit_price * (1 - discount_pct / 100), 2) AS expected_amount,
        -- An order is only finance-safe when its customer, product and store
        -- passed their own staging checks, matching the ETL inner joins.
        EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id = typed.customer_id AND c.dq_status = 'PASS') AS customer_ok,
        EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id = typed.product_id AND p.dq_status = 'PASS') AS product_ok,
        EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id = typed.store_id AND s.dq_status = 'PASS') AS store_ok
    FROM typed
)
SELECT
    *,
    CAST(order_ts AS DATE) AS order_date,
    CASE WHEN order_status = 'DELIVERED' THEN order_amount ELSE 0 END AS net_revenue,
    CASE WHEN order_id <> '' AND order_id_count = 1 AND customer_id <> '' AND store_id <> '' AND product_id <> ''
              AND quantity > 0 AND discount_pct BETWEEN 0 AND 100
              AND payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
              AND order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED')
              AND abs(order_amount - expected_amount) <= 0.01
              AND (order_status <> 'DELIVERED' OR delivery_minutes IS NOT NULL)
              AND coalesce(delivery_minutes, 0) <= 120
              AND order_ts <= least(CAST(current_timestamp AS TIMESTAMP), TIMESTAMP '2026-10-02 00:00:00')
              AND customer_ok AND product_ok AND store_ok
         THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    concat_ws('; ',
        CASE WHEN order_id = '' THEN 'Missing order_id' END,
        CASE WHEN order_id_count > 1 THEN 'Duplicate order_id' END,
        CASE WHEN customer_id = '' THEN 'Missing customer_id' END,
        CASE WHEN NOT customer_ok THEN 'Unknown or failed customer_id' END,
        CASE WHEN NOT product_ok THEN 'Unknown or failed product_id' END,
        CASE WHEN NOT store_ok THEN 'Unknown or failed store_id' END,
        CASE WHEN quantity <= 0 THEN 'Quantity is not positive' END,
        CASE WHEN discount_pct NOT BETWEEN 0 AND 100 THEN 'Discount outside 0-100' END,
        CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'Invalid payment mode' END,
        CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'Invalid order status' END,
        CASE WHEN abs(order_amount - expected_amount) > 0.01 THEN 'Amount does not match pricing formula' END,
        CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'Delivered order has no delivery time' END,
        CASE WHEN delivery_minutes > 120 THEN 'Delivery time exceeds 120 minutes' END,
        CASE WHEN order_ts IS NULL THEN 'order_ts is not a valid timestamp' END,
        -- 2026-10-02 is the end of the latest export day (data_quality.DATA_AS_OF).
        CASE WHEN order_ts > least(CAST(current_timestamp AS TIMESTAMP), TIMESTAMP '2026-10-02 00:00:00') THEN 'Future-dated order' END
    ) AS failure_reason
FROM checked;
