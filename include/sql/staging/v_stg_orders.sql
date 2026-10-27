-- The orders staging projection lives in a view so the full rebuild and the
-- incremental MERGE share one definition and cannot drift apart.
--
-- Cast, standardise, dedupe, and apply the same quality rules the ETL pipeline
-- applies in Python (include/nammamart/quality.py).
--
-- Nothing is dropped. Every raw row surfaces here carrying a dq_status and a
-- failure_reason, so a rejected order can still be traced back. Only rows with
-- dq_status = 'PASSED' are allowed into the mart.

CREATE OR REPLACE VIEW v_stg_orders AS
WITH ranked AS (
    -- Last row wins on a duplicated order_id, same rule as the ETL dedupe
    -- (drop_duplicates keep="last"). The earlier copies are kept but rejected.
    SELECT *,
           row_number() OVER (PARTITION BY trim(order_id)
                              ORDER BY _loaded_at DESC, rowid DESC) AS dup_rank
    FROM raw_orders
),
typed AS (
    SELECT
        trim(order_id)                                AS order_id,
        try_cast(order_ts AS TIMESTAMP)               AS order_ts,
        CAST(try_cast(order_ts AS TIMESTAMP) AS DATE)  AS order_date,
        nullif(trim(customer_id), '')                 AS customer_id,
        nullif(trim(store_id), '')                    AS store_id,
        nullif(trim(product_id), '')                  AS product_id,
        try_cast(quantity AS INTEGER)                 AS quantity,
        try_cast(unit_price AS DOUBLE)                AS unit_price,
        try_cast(discount_pct AS DOUBLE)              AS discount_pct,
        try_cast(order_amount AS DOUBLE)              AS order_amount_source,
        upper(trim(order_status))                     AS order_status,
        title_case(trim(channel))                     AS channel,
        try_cast(delivery_minutes AS DOUBLE)          AS delivery_minutes,
        trim(payment_mode)                            AS payment_mode_source,
        standard_payment_mode(payment_mode)           AS payment_mode,
        dup_rank, _run_id, _source_file, _loaded_at
    FROM ranked
),
derived AS (
    SELECT *,
        -- Recomputed, not trusted: the profile found 18 rows where the export disagrees.
        round(quantity * unit_price * (1 - discount_pct / 100.0), 2) AS net_revenue,
        CASE WHEN delivery_minutes IS NULL THEN NULL
             ELSE delivery_minutes <= 15 END                         AS within_15_min
    FROM typed
),
-- Consistency across files. The key sets come from the FULL raw masters, not
-- just the master rows that passed quality, so quarantining one bad product
-- does not silently delete the revenue of every order that points at it.
keys AS (
    SELECT d.*,
           cu.customer_id IS NOT NULL AS customer_exists,
           pr.product_id  IS NOT NULL AS product_exists,
           st.store_id    IS NOT NULL AS store_exists
    FROM derived d
    LEFT JOIN (SELECT DISTINCT trim(customer_id) AS customer_id FROM raw_customers) cu
           ON cu.customer_id = d.customer_id
    LEFT JOIN (SELECT DISTINCT trim(product_id) AS product_id FROM raw_products) pr
           ON pr.product_id = d.product_id
    LEFT JOIN (SELECT DISTINCT trim(store_id) AS store_id FROM raw_stores) st
           ON st.store_id = d.store_id
),
flagged AS (
    SELECT *,
        -- Critical breaches: the row is rejected and never reaches the mart.
        list_filter([
            CASE WHEN order_id IS NULL OR order_id = ''
                 THEN 'Critical: order_id is missing' END,
            CASE WHEN customer_id IS NULL
                 THEN 'Critical: customer_id is missing' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL
                 THEN 'Critical: DELIVERED order has no delivery_minutes' END,
            CASE WHEN dup_rank > 1
                 THEN 'Critical: duplicate order_id, later row kept' END,
            CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')
                 THEN 'Critical: order_status not in [DELIVERED, CANCELLED, RETURNED]' END,
            CASE WHEN order_ts IS NULL
                 THEN 'Critical: order_ts does not parse' END,
            CASE WHEN discount_pct IS NOT NULL AND (discount_pct < 0 OR discount_pct > 100)
                 THEN 'Critical: discount_pct outside 0-100' END,
            CASE WHEN payment_mode IS NULL
                 THEN 'Critical: payment_mode cannot be mapped' END,
            CASE WHEN quantity IS NULL OR quantity <= 0
                 THEN 'Critical: quantity must be greater than zero' END,
            CASE WHEN order_amount_source IS NOT NULL AND net_revenue IS NOT NULL
                  AND abs(order_amount_source - net_revenue) > 0.01
                 THEN 'Critical: order_amount does not match quantity x unit_price x (1 - discount)' END,
            CASE WHEN delivery_minutes > 120
                 THEN 'Critical: delivery_minutes above 120' END,
            CASE WHEN order_ts > TIMESTAMP '2026-10-02 00:00:00'
                 THEN 'Critical: order_ts is in the future' END,
            CASE WHEN customer_id IS NOT NULL AND NOT customer_exists
                 THEN 'Critical: customer_id does not exist in customers' END,
            CASE WHEN product_id IS NOT NULL AND NOT product_exists
                 THEN 'Critical: product_id does not exist in products' END,
            CASE WHEN store_id IS NOT NULL AND NOT store_exists
                 THEN 'Critical: store_id does not exist in stores' END
        ], lambda x: x IS NOT NULL) AS critical_reasons,
        -- Warnings: the row still loads, the breach is recorded.
        list_filter([
            CASE WHEN payment_mode IS NOT NULL AND payment_mode <> payment_mode_source
                 THEN 'Warning: payment_mode needed standardising' END,
            CASE WHEN delivery_minutes > 35 AND delivery_minutes <= 120
                 THEN 'Warning: delivery took longer than the normal 8-35 minute band' END
        ], lambda x: x IS NOT NULL) AS warning_reasons
    FROM keys
)
SELECT
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount_source, net_revenue,
    payment_mode, payment_mode_source, order_status, channel,
    delivery_minutes, within_15_min,
    CASE WHEN len(critical_reasons) > 0 THEN 'REJECTED' ELSE 'PASSED' END AS dq_status,
    array_to_string(list_concat(critical_reasons, warning_reasons), ' | ')  AS failure_reason,
    array_to_string(warning_reasons, ' | ')                                AS dq_warnings,
    _run_id, _source_file, _loaded_at
FROM flagged;
