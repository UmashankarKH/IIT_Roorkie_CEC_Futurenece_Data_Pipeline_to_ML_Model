-- NammaMart ELT: order deduplication, incremental upsert and DQ.

-- 1. Remove exact duplicate source rows without deleting raw history.
CREATE OR REPLACE TABLE orders_base_distinct AS
SELECT DISTINCT * FROM raw_orders_current;

CREATE OR REPLACE TABLE orders_increment_distinct AS
SELECT DISTINCT * FROM raw_orders_increment_current;

-- 2. Identify business keys with conflicting versions within a source.
CREATE OR REPLACE TABLE orders_base_conflict_keys AS
SELECT order_id
FROM orders_base_distinct
WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
GROUP BY order_id
HAVING COUNT(*) > 1;

CREATE OR REPLACE TABLE orders_increment_conflict_keys AS
SELECT order_id
FROM orders_increment_distinct
WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
GROUP BY order_id
HAVING COUNT(*) > 1;

CREATE OR REPLACE TABLE orders_conflict_keys AS
SELECT order_id FROM orders_base_conflict_keys
UNION
SELECT order_id FROM orders_increment_conflict_keys;

-- 3. Keep clean inputs. The increment updates status for existing IDs.
CREATE OR REPLACE TEMP TABLE orders_base_clean AS
SELECT b.*
FROM orders_base_distinct b
LEFT JOIN orders_conflict_keys k USING (order_id)
WHERE k.order_id IS NULL;

CREATE OR REPLACE TEMP TABLE orders_increment_clean AS
SELECT i.*
FROM orders_increment_distinct i
LEFT JOIN orders_conflict_keys k USING (order_id)
WHERE k.order_id IS NULL;

CREATE OR REPLACE TABLE orders_upserted AS
SELECT
    b.order_id,
    b.order_ts,
    b.customer_id,
    b.store_id,
    b.product_id,
    b.quantity,
    b.unit_price,
    b.discount_pct,
    b.order_amount,
    b.payment_mode,
    COALESCE(i.order_status, b.order_status) AS order_status,
    b.channel,
    b.delivery_minutes
FROM orders_base_clean b
LEFT JOIN orders_increment_clean i USING (order_id)

UNION ALL

SELECT
    i.order_id,
    i.order_ts,
    i.customer_id,
    i.store_id,
    i.product_id,
    i.quantity,
    i.unit_price,
    i.discount_pct,
    i.order_amount,
    i.payment_mode,
    i.order_status,
    i.channel,
    i.delivery_minutes
FROM orders_increment_clean i
WHERE NOT EXISTS (
    SELECT 1
    FROM orders_base_clean b
    WHERE b.order_id = i.order_id
);

-- 4. Cast values and calculate explicit validation failures.
CREATE OR REPLACE TABLE orders_validated AS
WITH typed AS (
    SELECT
        o.*,
        TRY_CAST(order_ts AS TIMESTAMP) AS parsed_order_ts,
        TRY_CAST(quantity AS INTEGER) AS parsed_quantity,
        TRY_CAST(unit_price AS DECIMAL(12,2)) AS parsed_unit_price,
        TRY_CAST(discount_pct AS DECIMAL(7,2)) AS parsed_discount_pct,
        TRY_CAST(order_amount AS DECIMAL(12,2)) AS parsed_order_amount,
        TRY_CAST(delivery_minutes AS INTEGER) AS parsed_delivery_minutes,
        UPPER(TRIM(order_status)) AS normalized_order_status,
        CASE
            WHEN UPPER(TRIM(payment_mode)) = 'COD'
                THEN 'CASH ON DELIVERY'
            ELSE UPPER(TRIM(payment_mode))
        END AS normalized_payment_mode
            FROM orders_upserted o
),
checked AS (
    SELECT
        t.*,
        CONCAT_WS(',',
            CASE
                WHEN order_id IS NULL OR TRIM(order_id) = ''
                THEN 'missing_order_id'
            END,
            CASE
                WHEN order_ts IS NULL OR TRIM(order_ts) = ''
                THEN 'missing_order_ts'
                WHEN parsed_order_ts IS NULL
                THEN 'invalid_order_ts'
                WHEN parsed_order_ts > CURRENT_TIMESTAMP
                THEN 'future_order_ts'
            END,
            CASE
                WHEN customer_id IS NULL OR TRIM(customer_id) = ''
                THEN 'missing_customer_id'
                WHEN NOT EXISTS (
                    SELECT 1 FROM stg_customers c
                    WHERE c.customer_id = TRIM(t.customer_id)
                ) THEN 'orphan_customer_id'
            END,
            CASE
                WHEN product_id IS NULL OR TRIM(product_id) = ''
                THEN 'missing_product_id'
                WHEN NOT EXISTS (
                    SELECT 1 FROM stg_products p
                    WHERE p.product_id = TRIM(t.product_id)
                ) THEN 'orphan_product_id'
            END,
            CASE
                WHEN store_id IS NULL OR TRIM(store_id) = ''
                THEN 'missing_store_id'
                WHEN NOT EXISTS (
                    SELECT 1 FROM stg_stores s
                    WHERE s.store_id = TRIM(t.store_id)
                ) THEN 'orphan_store_id'
            END,
            CASE
                WHEN parsed_quantity IS NULL OR parsed_quantity <= 0
                THEN 'invalid_quantity'
            END,
            CASE
                WHEN parsed_unit_price IS NULL OR parsed_unit_price <= 0
                THEN 'invalid_unit_price'
            END,
            CASE
                WHEN parsed_discount_pct IS NULL
                  OR parsed_discount_pct < 0
                  OR parsed_discount_pct > 100
                THEN 'invalid_discount_pct'
            END,
            CASE
                WHEN parsed_order_amount IS NULL
                  OR parsed_quantity IS NULL
                  OR parsed_unit_price IS NULL
                  OR parsed_discount_pct IS NULL
                  OR ROUND(parsed_order_amount, 2) <>
                     ROUND(
                         parsed_quantity * parsed_unit_price *
                         (1 - parsed_discount_pct / 100), 2
                     )
                THEN 'order_amount_mismatch'
            END,
            CASE
                WHEN normalized_order_status NOT IN
                     ('DELIVERED', 'CANCELLED', 'RETURNED')
                  OR normalized_order_status IS NULL
                THEN 'invalid_order_status'
            END,
            CASE
                WHEN normalized_payment_mode NOT IN
                     ('UPI', 'CARD', 'CASH ON DELIVERY', 'WALLET')
                  OR normalized_payment_mode IS NULL
                THEN 'invalid_payment_mode'
            END,
            CASE
                WHEN normalized_order_status = 'DELIVERED'
                  AND (parsed_delivery_minutes IS NULL
                       OR TRIM(COALESCE(delivery_minutes, '')) = '')
                THEN 'missing_delivery_minutes'
                WHEN TRIM(COALESCE(delivery_minutes, '')) <> ''
                  AND parsed_delivery_minutes IS NULL
                THEN 'invalid_delivery_minutes'
                WHEN parsed_delivery_minutes IS NOT NULL
                  AND (parsed_delivery_minutes <= 0
                       OR parsed_delivery_minutes >= 120)
                THEN 'invalid_delivery_minutes'
            END
        ) AS failure_reason
    FROM typed t
)
SELECT * FROM checked;

-- 5. Preserve invalid and conflicting records with explicit reasons.
CREATE OR REPLACE TABLE orders_quarantine_final AS
SELECT
    order_id, order_ts, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel, delivery_minutes,
    failure_reason AS quarantine_reason
FROM orders_validated
WHERE COALESCE(failure_reason, '') <> '';

CREATE OR REPLACE TABLE orders_quarantine AS
SELECT
    b.order_id, b.order_ts, b.customer_id, b.store_id, b.product_id,
    b.quantity, b.unit_price, b.discount_pct, b.order_amount,
    b.payment_mode, b.order_status, b.channel, b.delivery_minutes,
    'conflicting_order_id' AS quarantine_reason
FROM orders_base_distinct b
JOIN orders_conflict_keys k USING (order_id)

UNION ALL

SELECT
    i.order_id, i.order_ts, i.customer_id, i.store_id, i.product_id,
    i.quantity, i.unit_price, i.discount_pct, i.order_amount,
    i.payment_mode, i.order_status, i.channel, i.delivery_minutes,
    'conflicting_order_id' AS quarantine_reason
FROM orders_increment_distinct i
JOIN orders_conflict_keys k USING (order_id)

UNION ALL

SELECT
    order_id, order_ts, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel, delivery_minutes,
    quarantine_reason
FROM orders_quarantine_final;

-- 6. Publish only valid, typed orders to staging.
CREATE OR REPLACE TABLE stg_orders AS
SELECT
    TRIM(order_id) AS order_id,
    parsed_order_ts AS order_ts,
    TRIM(customer_id) AS customer_id,
    TRIM(store_id) AS store_id,
    TRIM(product_id) AS product_id,
    parsed_quantity AS quantity,
    parsed_unit_price AS unit_price,
    parsed_discount_pct AS discount_pct,
    ROUND(
        parsed_quantity * parsed_unit_price *
        (1 - parsed_discount_pct / 100), 2
    ) AS order_amount,
    CASE normalized_payment_mode
        WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
        WHEN 'UPI' THEN 'UPI'
        WHEN 'CARD' THEN 'Card'
        WHEN 'WALLET' THEN 'Wallet'
    END AS payment_mode,
    normalized_order_status AS order_status,
    UPPER(TRIM(channel)) AS channel,
    parsed_delivery_minutes AS delivery_minutes,
    'PASS' AS dq_status,
    CAST(NULL AS VARCHAR) AS failure_reason
FROM orders_validated
WHERE COALESCE(failure_reason, '') = '';

-- Compatibility view for downstream ELT steps.
CREATE OR REPLACE TABLE orders_curated AS
SELECT * FROM stg_orders;
