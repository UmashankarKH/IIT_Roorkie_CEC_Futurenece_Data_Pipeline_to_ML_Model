-- include/sql/05_staging_orders.sql
-- Staging Orders: Cast types, deduplicate, standardize, validate DQ, derive net_revenue, flag dq_status.

CREATE OR REPLACE TABLE stg_orders AS
WITH typed AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRY_CAST(order_ts AS TIMESTAMP) AS parsed_ts,
        order_ts AS raw_ts,
        TRIM(customer_id) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(product_id) AS product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS INTEGER) AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        CASE
            WHEN LOWER(TRIM(payment_mode)) IN ('upi', 'upi ') THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) IN ('card') THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) IN ('cash on delivery', 'cod', 'cashh') THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status,
        TRIM(channel) AS channel,
        TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY _loaded_at DESC) AS rnk
    FROM raw_orders
    WHERE _source_file = 'orders.csv'
),
deduped AS (
    SELECT * FROM typed WHERE rnk = 1
),
evaluated AS (
    SELECT
        d.order_id,
        d.parsed_ts,
        d.raw_ts,
        CAST(d.parsed_ts AS DATE) AS order_date,
        d.customer_id,
        d.store_id,
        d.product_id,
        d.quantity,
        d.unit_price,
        d.discount_pct,
        d.order_amount,
        ROUND(d.quantity * d.unit_price * (1.0 - d.discount_pct / 100.0), 2) AS expected_amount,
        d.payment_mode,
        d.order_status,
        d.channel,
        d.delivery_minutes,
        CASE
            WHEN d.order_status = 'DELIVERED' THEN d.order_amount
            ELSE 0.0
        END AS net_revenue,
        CASE
            WHEN d.order_status = 'DELIVERED' AND d.delivery_minutes <= 15.0 THEN TRUE
            ELSE FALSE
        END AS within_15_min,
        p.cost_price,
        -- Failure conditions
        CASE WHEN d.customer_id IS NULL OR d.customer_id = '' OR LOWER(d.customer_id) = 'nan' THEN 1 ELSE 0 END AS fail_null_cust,
        CASE WHEN d.parsed_ts IS NULL THEN 1 ELSE 0 END AS fail_bad_ts,
        CASE WHEN d.order_status = 'DELIVERED' AND d.delivery_minutes IS NULL THEN 1 ELSE 0 END AS fail_null_min,
        CASE WHEN d.order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 1 ELSE 0 END AS fail_bad_status,
        CASE WHEN d.discount_pct < 0 OR d.discount_pct > 100 THEN 1 ELSE 0 END AS fail_bad_disc,
        CASE WHEN d.quantity <= 0 THEN 1 ELSE 0 END AS fail_bad_qty,
        CASE WHEN d.order_amount <= 0 THEN 1 ELSE 0 END AS fail_bad_amount,
        CASE WHEN ABS(d.order_amount - ROUND(d.quantity * d.unit_price * (1.0 - d.discount_pct / 100.0), 2)) > 0.02
                  AND d.quantity > 0 AND d.discount_pct BETWEEN 0 AND 100 THEN 1 ELSE 0 END AS fail_formula,
        CASE WHEN d.order_status = 'DELIVERED' AND d.delivery_minutes > 120.0 THEN 1 ELSE 0 END AS fail_deliv_sla,
        CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END AS fail_unknown_cust,
        CASE WHEN p.product_id IS NULL THEN 1 ELSE 0 END AS fail_unknown_prod,
        CASE WHEN s.store_id IS NULL THEN 1 ELSE 0 END AS fail_unknown_store,
        CASE WHEN d.parsed_ts > TIMESTAMP '2026-10-02 23:59:59' THEN 1 ELSE 0 END AS fail_future_ts,
        d._loaded_at,
        d._source_file,
        d._run_id
    FROM deduped d
    LEFT JOIN stg_customers c ON d.customer_id = c.customer_id
    LEFT JOIN stg_products p ON d.product_id = p.product_id
    LEFT JOIN stg_stores s ON d.store_id = s.store_id
)
SELECT
    order_id,
    parsed_ts AS order_ts,
    order_date,
    customer_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    order_amount,
    net_revenue,
    CASE
        WHEN order_status = 'DELIVERED' THEN ROUND(net_revenue - (quantity * COALESCE(cost_price, 0.0)), 2)
        ELSE 0.0
    END AS gross_margin,
    payment_mode,
    order_status,
    channel,
    delivery_minutes,
    within_15_min,
    CASE
        WHEN (fail_null_cust + fail_bad_ts + fail_null_min + fail_bad_status + fail_bad_disc +
              fail_bad_qty + fail_bad_amount + fail_formula + fail_deliv_sla + fail_unknown_cust +
              fail_unknown_prod + fail_unknown_store + fail_future_ts) > 0 THEN 'FAILED'
        ELSE 'PASSED'
    END AS dq_status,
    CONCAT_WS('; ',
        CASE WHEN fail_null_cust = 1 THEN 'Completeness: null/empty customer_id' END,
        CASE WHEN fail_bad_ts = 1 THEN 'Validity: unparseable order_ts' END,
        CASE WHEN fail_null_min = 1 THEN 'Completeness: DELIVERED missing delivery_minutes' END,
        CASE WHEN fail_bad_status = 1 THEN 'Validity: invalid order_status' END,
        CASE WHEN fail_bad_disc = 1 THEN 'Validity: discount_pct out of range' END,
        CASE WHEN fail_bad_qty = 1 THEN 'Accuracy: non-positive quantity' END,
        CASE WHEN fail_bad_amount = 1 THEN 'Accuracy: non-positive order_amount' END,
        CASE WHEN fail_formula = 1 THEN 'Accuracy: formula mismatch' END,
        CASE WHEN fail_deliv_sla = 1 THEN 'Accuracy: delivery_minutes exceeds 120' END,
        CASE WHEN fail_unknown_cust = 1 THEN 'Consistency: customer_id not in customers' END,
        CASE WHEN fail_unknown_prod = 1 THEN 'Consistency: product_id not in products' END,
        CASE WHEN fail_unknown_store = 1 THEN 'Consistency: store_id not in stores' END,
        CASE WHEN fail_future_ts = 1 THEN 'Timeliness: future order_ts' END
    ) AS failure_reason,
    _loaded_at,
    _source_file,
    _run_id
FROM evaluated;
