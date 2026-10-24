-- include/sql/incremental_merge.sql
-- Incremental load: Stage new and updated records from orders_2026-10-01_increment.csv
-- and MERGE / upsert into stg_orders on order_id.

CREATE OR REPLACE TEMPORARY TABLE tmp_stg_increment AS
WITH raw_typed AS (
    SELECT
        order_id,
        order_ts AS order_ts_raw,
        TRY_CAST(order_ts AS TIMESTAMP) AS order_ts_parsed,
        customer_id,
        store_id,
        product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
        TRY_CAST(discount_pct AS DECIMAL(10, 2)) AS discount_pct,
        TRY_CAST(order_amount AS DECIMAL(10, 2)) AS order_amount,
        CASE 
            WHEN UPPER(TRIM(payment_mode)) IN ('UPI', 'UPI ') THEN 'UPI'
            WHEN UPPER(TRIM(payment_mode)) IN ('CARD', 'CREDIT CARD', 'DEBIT CARD') THEN 'Card'
            WHEN UPPER(TRIM(payment_mode)) IN ('CASH ON DELIVERY', 'COD') THEN 'Cash on Delivery'
            WHEN UPPER(TRIM(payment_mode)) = 'WALLET' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS clean_payment_mode,
        UPPER(TRIM(order_status)) AS clean_order_status,
        channel,
        TRY_CAST(delivery_minutes AS DECIMAL(10, 2)) AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_orders
    WHERE _source_file = 'orders_2026-10-01_increment.csv'
),
evaluated AS (
    SELECT
        r.order_id,
        r.order_ts_parsed AS order_ts,
        CAST(r.order_ts_parsed AS DATE) AS order_date,
        r.customer_id,
        r.store_id,
        r.product_id,
        r.quantity,
        r.unit_price,
        r.discount_pct,
        r.order_amount,
        r.clean_payment_mode AS payment_mode,
        r.clean_order_status AS order_status,
        r.channel,
        r.delivery_minutes,
        r._loaded_at,
        r._source_file,
        r._run_id,
        r.rn,
        CASE
            WHEN r.rn > 1 THEN 'DUPLICATE_ORDER_ID'
            WHEN r.clean_order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'INVALID_ORDER_STATUS'
            WHEN r.quantity <= 0 THEN 'NON_POSITIVE_QUANTITY'
            ELSE NULL
        END AS failure_reason,
        p.cost_price
    FROM raw_typed r
    LEFT JOIN (SELECT DISTINCT product_id, cost_price FROM stg_products WHERE dq_status != 'QUARANTINE') p 
        ON r.product_id = p.product_id
)
SELECT
    order_id,
    order_ts,
    order_date,
    customer_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    order_amount,
    payment_mode,
    order_status,
    channel,
    delivery_minutes,
    CASE 
        WHEN failure_reason IS NULL AND order_status = 'DELIVERED' THEN order_amount 
        ELSE 0.00 
    END AS net_revenue,
    CASE 
        WHEN failure_reason IS NULL AND order_status = 'DELIVERED' THEN order_amount - (quantity * COALESCE(cost_price, 0)) 
        ELSE 0.00 
    END AS gross_margin,
    CASE 
        WHEN failure_reason IS NULL AND order_status = 'DELIVERED' AND delivery_minutes <= 15 THEN TRUE 
        ELSE FALSE 
    END AS within_15_min,
    _loaded_at,
    _source_file,
    _run_id,
    CASE 
        WHEN failure_reason IS NOT NULL THEN 'QUARANTINE' 
        ELSE 'VALID' 
    END AS dq_status,
    failure_reason
FROM evaluated;

MERGE INTO stg_orders AS target
USING tmp_stg_increment AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    order_ts = source.order_ts,
    order_date = source.order_date,
    customer_id = source.customer_id,
    store_id = source.store_id,
    product_id = source.product_id,
    quantity = source.quantity,
    unit_price = source.unit_price,
    discount_pct = source.discount_pct,
    order_amount = source.order_amount,
    payment_mode = source.payment_mode,
    order_status = source.order_status,
    channel = source.channel,
    delivery_minutes = source.delivery_minutes,
    net_revenue = source.net_revenue,
    gross_margin = source.gross_margin,
    within_15_min = source.within_15_min,
    _loaded_at = source._loaded_at,
    _source_file = source._source_file,
    _run_id = source._run_id,
    dq_status = source.dq_status,
    failure_reason = source.failure_reason
WHEN NOT MATCHED THEN INSERT VALUES (
    source.order_id,
    source.order_ts,
    source.order_date,
    source.customer_id,
    source.store_id,
    source.product_id,
    source.quantity,
    source.unit_price,
    source.discount_pct,
    source.order_amount,
    source.payment_mode,
    source.order_status,
    source.channel,
    source.delivery_minutes,
    source.net_revenue,
    source.gross_margin,
    source.within_15_min,
    source._loaded_at,
    source._source_file,
    source._run_id,
    source.dq_status,
    source.failure_reason
);
