-- include/sql/staging/stg_orders_view.sql
-- ELT layer 2: STAGING (assignment Part B) - orders
--
-- This view holds ALL of the staging logic for orders so that the full build
-- (stg_orders.sql) and the incremental MERGE (stg_orders_merge.sql) can never
-- drift apart.  SQL only: no pandas is involved anywhere in this layer.
--
-- What it does, in order:
--   1. source_rows  - collapse byte-identical re-loads of the same source row,
--                     so running the DAG twice does not change the result
--                     (idempotency, client acceptance criterion 5)
--   2. versioned    - rank the versions of each order_id, newest first, so the
--                     1 October corrections win over the September rows
--   3. typed        - cast text to real types and standardise values
--   4. checked      - apply the Part D quality rules as chk_* flags
--   5. final select - derive dq_status, failure_reason and the revenue columns
--
-- dq_status values:
--   PASS        - loads into the mart layer
--   QUARANTINED - broke at least one Critical rule; kept here with the reason
--   SUPERSEDED  - an older version of an order that was corrected later

CREATE OR REPLACE VIEW v_stg_orders AS
WITH source_rows AS (
    SELECT
        order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price,
        discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes,
        max(_loaded_at)      AS _loaded_at,
        arg_max(_source_file, _loaded_at) AS _source_file,
        arg_max(_run_id, _loaded_at)      AS _run_id,
        min(_source_row_num)    AS _source_row_num,
        count(*)                AS _raw_copies
    FROM raw_orders
    GROUP BY order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price,
             discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes
),
versioned AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(order_id)
                              ORDER BY _loaded_at DESC, _source_row_num DESC) AS _version_rank
    FROM source_rows
),
typed AS (
    SELECT
        nullif(trim(order_id), '')                                   AS order_id,
        try_cast(order_ts AS TIMESTAMP)                              AS order_ts,
        CAST(try_cast(order_ts AS TIMESTAMP) AS DATE)                AS order_date,
        nullif(trim(customer_id), '')                                AS customer_id,
        nullif(trim(store_id), '')                                   AS store_id,
        nullif(trim(product_id), '')                                 AS product_id,
        try_cast(quantity AS DOUBLE)                                 AS quantity,
        try_cast(unit_price AS DOUBLE)                               AS unit_price,
        coalesce(try_cast(discount_pct AS DOUBLE), 0)                AS discount_pct,
        try_cast(discount_pct AS DOUBLE)                             AS discount_pct_raw,
        try_cast(order_amount AS DOUBLE)                             AS order_amount,
        round(try_cast(quantity AS DOUBLE) * try_cast(unit_price AS DOUBLE)
              * (1 - coalesce(try_cast(discount_pct AS DOUBLE), 0) / 100), 2)
                                                                     AS expected_order_amount,
        CASE regexp_replace(lower(trim(payment_mode)), '\s+', ' ', 'g')
             WHEN 'upi'              THEN 'UPI'
             WHEN 'u p i'            THEN 'UPI'
             WHEN 'card'             THEN 'Card'
             WHEN 'credit card'      THEN 'Card'
             WHEN 'debit card'       THEN 'Card'
             WHEN 'cash on delivery' THEN 'Cash on Delivery'
             WHEN 'cod'              THEN 'Cash on Delivery'
             WHEN 'cash'             THEN 'Cash on Delivery'
             WHEN 'wallet'           THEN 'Wallet'
             WHEN 'e-wallet'         THEN 'Wallet'
             ELSE nullif(trim(payment_mode), '')
        END                                                          AS payment_mode,
        upper(nullif(trim(order_status), ''))                        AS order_status,
        -- DuckDB has no initcap(): title-case the first letter manually
        upper(substr(nullif(trim(channel), ''), 1, 1))
            || lower(substr(nullif(trim(channel), ''), 2))            AS channel,
        try_cast(delivery_minutes AS DOUBLE)                         AS delivery_minutes,
        nullif(trim(order_ts), '')                                   AS order_ts_raw,
        _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
    FROM versioned
),
checked AS (
    SELECT *,
        -- Completeness
        order_id IS NULL                                             AS chk_rule_001,
        order_ts_raw IS NULL                                         AS chk_rule_003,
        customer_id IS NULL                                          AS chk_rule_006,
        (order_status = 'DELIVERED' AND delivery_minutes IS NULL)    AS chk_rule_014,
        -- Validity
        (order_ts_raw IS NOT NULL AND order_ts IS NULL)              AS chk_rule_004,
        (order_status IS NOT NULL
         AND order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED'))
                                                                     AS chk_rule_012,
        (discount_pct_raw IS NOT NULL AND (discount_pct_raw < 0 OR discount_pct_raw > 100))
                                                                     AS chk_rule_010,
        (payment_mode IS NOT NULL
         AND payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'))
                                                                     AS chk_rule_013,
        (channel IS NOT NULL AND channel NOT IN ('App', 'Web', 'Phone'))
                                                                     AS chk_rule_033,
        -- Accuracy
        (quantity IS NULL OR quantity <= 0)                          AS chk_rule_009,
        (abs(order_amount - expected_order_amount) > 0.01)           AS chk_rule_011,
        (delivery_minutes IS NOT NULL
         AND (delivery_minutes < 0 OR delivery_minutes > 120))       AS chk_rule_015,
        -- Consistency (foreign keys checked against the raw master files)
        (customer_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM raw_customers c
            WHERE nullif(trim(c.customer_id), '') = orders_typed.customer_id))
                                                                     AS chk_rule_032,
        (product_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM raw_products p
            WHERE nullif(trim(p.product_id), '') = orders_typed.product_id))
                                                                     AS chk_rule_007,
        (store_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM raw_stores s
            WHERE nullif(trim(s.store_id), '') = orders_typed.store_id))
                                                                     AS chk_rule_008,
        -- Timeliness
        (order_ts IS NOT NULL AND order_ts > now())                  AS chk_rule_005
    FROM typed AS orders_typed
)
SELECT
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount, expected_order_amount,
    payment_mode, order_status, channel, delivery_minutes,
    CASE WHEN order_status = 'DELIVERED' THEN round(coalesce(order_amount, 0), 2)
         ELSE 0 END                                                  AS net_revenue,
    (order_status = 'DELIVERED')                                     AS is_revenue,
    (order_status = 'DELIVERED' AND delivery_minutes IS NOT NULL
     AND delivery_minutes <= 15)                                     AS within_15_min,
    chk_rule_001, chk_rule_003, chk_rule_004, chk_rule_005, chk_rule_006, chk_rule_007,
    chk_rule_008, chk_rule_009, chk_rule_010, chk_rule_011, chk_rule_012, chk_rule_013,
    chk_rule_014, chk_rule_015, chk_rule_032, chk_rule_033,
    CASE
        WHEN _version_rank > 1 THEN 'SUPERSEDED'
        WHEN chk_rule_001 OR chk_rule_003 OR chk_rule_004 OR chk_rule_005 OR chk_rule_006
          OR chk_rule_007 OR chk_rule_008 OR chk_rule_009 OR chk_rule_010 OR chk_rule_011
          OR chk_rule_012 OR chk_rule_014 OR chk_rule_015 OR chk_rule_032 THEN 'QUARANTINED'
        ELSE 'PASS'
    END                                                              AS dq_status,
    concat_ws(' | ',
        CASE WHEN _version_rank > 1 THEN 'RULE-002: superseded by a newer version of this order_id' END,
        CASE WHEN chk_rule_001 THEN 'RULE-001: order_id must be present [Completeness/Critical]' END,
        CASE WHEN chk_rule_003 THEN 'RULE-003: order_ts must be present [Completeness/Critical]' END,
        CASE WHEN chk_rule_006 THEN 'RULE-006: customer_id must be present [Completeness/Critical]' END,
        CASE WHEN chk_rule_014 THEN 'RULE-014: DELIVERED orders must have delivery_minutes [Completeness/Critical]' END,
        CASE WHEN chk_rule_004 THEN 'RULE-004: order_ts must parse as a valid timestamp [Validity/Critical]' END,
        CASE WHEN chk_rule_012 THEN 'RULE-012: order_status must be in the approved list [Validity/Critical]' END,
        CASE WHEN chk_rule_010 THEN 'RULE-010: discount_pct must be between 0 and 100 [Validity/Critical]' END,
        CASE WHEN chk_rule_009 THEN 'RULE-009: quantity must be greater than zero [Accuracy/Critical]' END,
        CASE WHEN chk_rule_011 THEN 'RULE-011: order_amount must match quantity x unit_price x (1 - discount_pct/100) [Accuracy/Critical]' END,
        CASE WHEN chk_rule_015 THEN 'RULE-015: delivery_minutes must be between 0 and 120 [Accuracy/Critical]' END,
        CASE WHEN chk_rule_032 THEN 'RULE-032: customer_id must exist in customers.csv [Consistency/Critical]' END,
        CASE WHEN chk_rule_007 THEN 'RULE-007: product_id must exist in products.csv [Consistency/Critical]' END,
        CASE WHEN chk_rule_008 THEN 'RULE-008: store_id must exist in stores.csv [Consistency/Critical]' END,
        CASE WHEN chk_rule_005 THEN 'RULE-005: order_ts must not be future-dated [Timeliness/Critical]' END
    )                                                                AS failure_reason,
    concat_ws(' | ',
        CASE WHEN chk_rule_013 THEN 'RULE-013: payment_mode must be in the approved list [Validity/Warning]' END,
        CASE WHEN chk_rule_033 THEN 'RULE-033: channel must be in the approved list [Validity/Warning]' END
    )                                                                AS dq_warnings,
    _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
FROM checked;
