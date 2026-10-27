-- Staging: orders.
-- Step 1: stg_orders_validated = every order row we hold, typed and checked:
--           - orders.csv rows received in THIS run (the September snapshot)
--           - every increment file ever received (latest load of each file)
--         Orders may only point at stores / products / customers that PASSED.
-- Step 2: stg_orders = the September snapshot only. Increment rows are added
--         by stg_orders_merge_increment.sql (MERGE on order_id).

CREATE OR REPLACE TABLE stg_orders_validated AS
WITH received AS (
    SELECT * FROM raw_orders
    WHERE _source_file = 'orders.csv' AND _run_id = getvariable('run_id')

    UNION ALL

    SELECT * FROM raw_orders
    WHERE _source_file <> 'orders.csv'
    QUALIFY _loaded_at = max(_loaded_at) OVER (PARTITION BY _source_file)
),

typed AS (
    SELECT
        *,
        try_strptime(trim(order_ts), '%Y-%m-%d %H:%M:%S') AS order_ts_parsed,
        try_cast(trim(quantity) AS DOUBLE) AS qty,
        try_cast(trim(unit_price) AS DOUBLE) AS price,
        try_cast(trim(discount_pct) AS DOUBLE) AS discount,
        try_cast(trim(order_amount) AS DOUBLE) AS amount,
        try_cast(trim(delivery_minutes) AS DOUBLE) AS minutes,
        upper(trim(order_status)) AS status_std,
        CASE lower(trim(payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'wallet' THEN 'Wallet'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'cashh' THEN 'Cash on Delivery'
            ELSE trim(payment_mode)
        END AS mode_std,
        CAST(CASE
            WHEN _source_file = 'orders.csv' THEN getvariable('base_business_date')
            ELSE regexp_extract(_source_file, 'orders_(\d{4}-\d{2}-\d{2})_increment', 1)
        END AS DATE) AS batch_date,
        row_number() OVER (PARTITION BY _source_file, order_id ORDER BY _row_num) AS dup_rank
    FROM received
),

with_refs AS (
    SELECT
        t.*,
        batch_date + INTERVAL 1 DAY + INTERVAL 9 HOUR AS as_of,
        round(qty * price * (1 - discount / 100), 2) AS expected_amount,
        p.mrp AS product_mrp,
        p.cost_price AS product_cost
    FROM typed t
    LEFT JOIN stg_products p ON p.product_id = t.product_id AND p.dq_status = 'PASSED'
),

checked AS (
    SELECT
        *,
        list_filter([
            CASE WHEN coalesce(trim(customer_id), '') = '' THEN 'customer_id_not_null' END,
            CASE WHEN status_std = 'DELIVERED' AND minutes IS NULL THEN 'delivered_has_delivery_minutes' END,
            CASE WHEN dup_rank > 1 THEN 'order_id_unique' END,
            CASE WHEN order_ts_parsed IS NULL THEN 'order_ts_valid' END,
            CASE WHEN status_std NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'order_status_valid' END,
            CASE WHEN mode_std NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode_valid' END,
            CASE WHEN discount IS NULL OR discount < 0 OR discount > 100 THEN 'discount_pct_in_range' END,
            CASE WHEN qty IS NULL OR qty <= 0 THEN 'quantity_positive' END,
            CASE WHEN amount IS NULL OR expected_amount IS NULL OR abs(amount - expected_amount) > 0.01
                 THEN 'order_amount_matches_formula' END,
            CASE WHEN minutes > 120 THEN 'delivery_minutes_below_120' END,
            CASE WHEN coalesce(trim(customer_id), '') <> ''
                  AND customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE dq_status = 'PASSED')
                 THEN 'customer_id_in_master' END,
            CASE WHEN product_id NOT IN (SELECT product_id FROM stg_products WHERE dq_status = 'PASSED')
                 THEN 'product_id_in_master' END,
            CASE WHEN store_id NOT IN (SELECT store_id FROM stg_stores WHERE dq_status = 'PASSED')
                 THEN 'store_id_in_master' END,
            CASE WHEN order_ts_parsed > as_of THEN 'order_ts_not_future' END
        ], x -> x IS NOT NULL) AS dq_failed,
        list_filter([
            CASE WHEN minutes > 35 AND minutes <= 120 THEN 'delivery_minutes_normal_range' END,
            CASE WHEN order_status <> status_std OR payment_mode <> mode_std THEN 'values_need_standardising' END,
            CASE WHEN price > 1.5 * product_mrp THEN 'unit_price_vs_mrp' END
        ], x -> x IS NOT NULL) AS dq_warnings
    FROM with_refs
)

SELECT
    order_id,
    order_ts_parsed AS order_ts,
    CAST(order_ts_parsed AS DATE) AS order_date,
    customer_id,
    store_id,
    product_id,
    CAST(qty AS INTEGER) AS quantity,
    price AS unit_price,
    discount AS discount_pct,
    amount AS order_amount,
    mode_std AS payment_mode,
    status_std AS order_status,
    channel,
    minutes AS delivery_minutes,
    CASE WHEN status_std = 'DELIVERED' THEN amount ELSE 0 END AS net_revenue,
    CASE WHEN status_std = 'DELIVERED' THEN round(qty * product_cost, 2) ELSE 0 END AS cost_amount,
    CASE WHEN status_std = 'DELIVERED' THEN round(amount - qty * product_cost, 2) ELSE 0 END AS gross_margin,
    CASE WHEN status_std = 'DELIVERED' THEN minutes <= 15 END AS within_15_min,
    CASE WHEN len(dq_failed) = 0 THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    array_to_string(dq_failed, '; ') AS failure_reason,
    array_to_string(dq_warnings, '; ') AS warnings,
    dq_failed,
    dq_warnings,
    dup_rank,
    batch_date AS _batch_date,
    _row_num,
    _source_file,
    _run_id
FROM checked;


CREATE OR REPLACE TABLE stg_orders AS
SELECT * FROM stg_orders_validated
WHERE _source_file = 'orders.csv';
