-- Normalize the newest source row per order. The increment file has priority
-- over the September snapshot, so returned-status corrections take effect.
CREATE OR REPLACE TEMP TABLE _orders_ranked AS
WITH latest_copy AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY _source_file, _source_row_number
        ORDER BY _loaded_at DESC, _run_id DESC
    ) AS _copy_rank
    FROM raw_orders
), stable AS (
    SELECT * EXCLUDE (_copy_rank) FROM latest_copy WHERE _copy_rank = 1
), source_counts AS (
    SELECT _source_file, NULLIF(TRIM(order_id), '') AS order_id, COUNT(*) AS source_key_count
    FROM stable
    GROUP BY _source_file, NULLIF(TRIM(order_id), '')
), candidates AS (
    SELECT s.*, COALESCE(c.source_key_count, 0) AS source_key_count,
           ROW_NUMBER() OVER (
               PARTITION BY NULLIF(TRIM(s.order_id), '')
               ORDER BY CASE WHEN s._source_file LIKE '%increment%' THEN 1 ELSE 0 END DESC,
                        s._loaded_at DESC, s._source_row_number DESC
           ) AS _order_rank
    FROM stable s
    LEFT JOIN source_counts c
      ON c._source_file = s._source_file
     AND c.order_id IS NOT DISTINCT FROM NULLIF(TRIM(s.order_id), '')
)
SELECT * FROM candidates;

CREATE OR REPLACE TEMP TABLE _orders_source AS
WITH chosen AS (
    SELECT * FROM _orders_ranked WHERE _order_rank = 1
), typed AS (
    SELECT
        NULLIF(TRIM(r.order_id), '') AS order_id,
        TRY_CAST(NULLIF(TRIM(r.order_ts), '') AS TIMESTAMP) AS order_ts,
        NULLIF(TRIM(r.customer_id), '') AS customer_id,
        NULLIF(TRIM(r.store_id), '') AS store_id,
        NULLIF(TRIM(r.product_id), '') AS product_id,
        TRY_CAST(NULLIF(TRIM(r.quantity), '') AS DOUBLE) AS quantity,
        TRY_CAST(NULLIF(TRIM(r.unit_price), '') AS DOUBLE) AS unit_price,
        TRY_CAST(NULLIF(TRIM(r.discount_pct), '') AS DOUBLE) AS discount_pct,
        TRY_CAST(NULLIF(TRIM(r.order_amount), '') AS DOUBLE) AS order_amount,
        CASE LOWER(TRIM(r.payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet'
            ELSE TRIM(r.payment_mode)
        END AS payment_mode,
        UPPER(TRIM(r.order_status)) AS order_status,
        NULLIF(TRIM(r.channel), '') AS channel,
        TRY_CAST(NULLIF(TRIM(r.delivery_minutes), '') AS DOUBLE) AS delivery_minutes,
        r._source_file,
        r._source_row_number,
        r.source_key_count > 1 AS _duplicate_order_id,
        c.customer_id AS matched_customer_id,
        p.product_id AS matched_product_id,
        p.category,
        p.cost_price,
        s.store_id AS matched_store_id,
        s.store_name,
        s.zone
    FROM chosen r
    LEFT JOIN stg_customers c
      ON c.customer_id = NULLIF(TRIM(r.customer_id), '') AND c.dq_status <> 'FAIL'
    LEFT JOIN stg_products p
      ON p.product_id = NULLIF(TRIM(r.product_id), '') AND p.dq_status <> 'FAIL'
    LEFT JOIN stg_stores s
      ON s.store_id = NULLIF(TRIM(r.store_id), '') AND s.dq_status <> 'FAIL'
), evaluated AS (
    SELECT *,
        CONCAT_WS('; ',
            CASE WHEN order_id IS NULL THEN 'order_id is required' END,
            CASE WHEN order_ts IS NULL THEN 'order_ts is missing or invalid' END,
            CASE WHEN order_ts > CURRENT_TIMESTAMP THEN 'order_ts is in the future' END,
            CASE WHEN customer_id IS NULL THEN 'customer_id is required' END,
            CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'order_status is invalid' END,
            CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode is invalid' END,
            CASE WHEN quantity IS NULL OR quantity <= 0 THEN 'quantity must be greater than zero' END,
            CASE WHEN discount_pct IS NULL OR discount_pct < 0 OR discount_pct > 100 THEN 'discount_pct must be between 0 and 100' END,
            CASE WHEN order_amount IS NULL OR unit_price IS NULL OR quantity IS NULL OR discount_pct IS NULL
                      OR ABS(order_amount - ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)) > 0.01
                 THEN 'order_amount does not match the order formula' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'DELIVERED order needs delivery_minutes' END,
            CASE WHEN delivery_minutes IS NOT NULL AND (delivery_minutes <= 0 OR delivery_minutes >= 120) THEN 'delivery_minutes must be greater than zero and below 120' END,
            CASE WHEN matched_customer_id IS NULL THEN 'customer_id does not exist in a valid customer dimension row' END,
            CASE WHEN matched_product_id IS NULL THEN 'product_id does not exist in a valid product dimension row' END,
            CASE WHEN matched_store_id IS NULL THEN 'store_id does not exist in a valid store dimension row' END,
            CASE WHEN _duplicate_order_id THEN 'duplicate order_id in the same source file' END,
            CASE WHEN order_status = 'DELIVERED'
                      AND EXISTS (
                SELECT 1 FROM stg_payments p
                WHERE p.order_id = evaluated_candidate.order_id
                  AND p.payment_status = 'SUCCESS'
                  AND (p.amount_paid IS NULL
                       OR ABS(p.amount_paid - evaluated_candidate.order_amount) > 0.01
                       OR p.payment_mode <> evaluated_candidate.payment_mode)
            ) THEN 'successful payment amount or mode does not match the order' END
        ) AS critical_reason
    FROM typed evaluated_candidate
), with_warnings AS (
    SELECT evaluated.*,
           CASE WHEN order_status = 'DELIVERED' AND NOT EXISTS (
               SELECT 1 FROM stg_payments p
               WHERE p.order_id = evaluated.order_id
                 AND p.payment_status = 'SUCCESS'
                 AND p.dq_status <> 'FAIL'
                 AND p.amount_paid IS NOT NULL
                 AND ABS(p.amount_paid - evaluated.order_amount) <= 0.01
                 AND p.payment_mode = evaluated.payment_mode
           ) THEN 'no successful payment has arrived for this delivered order yet' END AS warning_reason
    FROM evaluated
), finalized AS (
    SELECT *, CAST(order_ts AS DATE) AS order_date,
           CASE WHEN critical_reason = '' AND order_status = 'DELIVERED' THEN order_amount ELSE 0 END AS net_revenue,
           CASE WHEN critical_reason = '' AND order_status = 'DELIVERED'
                THEN order_amount - quantity * cost_price ELSE 0 END AS gross_margin,
           CASE WHEN critical_reason = '' AND order_status = 'DELIVERED' AND delivery_minutes <= 15
                THEN TRUE ELSE FALSE END AS within_15_min
    FROM with_warnings
)
SELECT
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount, payment_mode, order_status,
    channel, delivery_minutes, category, cost_price, store_name, zone,
    net_revenue, gross_margin, within_15_min,
    _source_file, _source_row_number,
    CONCAT_WS('; ', NULLIF(critical_reason, ''), NULLIF(warning_reason, '')) AS failure_reason,
    CASE WHEN critical_reason <> '' THEN 'FAIL'
         WHEN warning_reason IS NOT NULL THEN 'WARNING'
         ELSE 'PASS' END AS dq_status
FROM finalized;
