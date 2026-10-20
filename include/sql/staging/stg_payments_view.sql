-- include/sql/staging/stg_payments_view.sql
-- ELT layer 2: STAGING (assignment Part B) - payments
--
-- Depends on stg_orders: the "payment amount must equal the order amount" rule
-- can only be judged against orders that passed validation.  Mismatches for
-- orders that were themselves rejected are reported by the reconciliation mart
-- instead, so finance still sees them.
CREATE OR REPLACE VIEW v_stg_payments AS
WITH source_rows AS (
    SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status,
           gateway_ref,
           max(_loaded_at) AS _loaded_at, arg_max(_source_file, _loaded_at) AS _source_file,
           arg_max(_run_id, _loaded_at)      AS _run_id, min(_source_row_num) AS _source_row_num,
           count(*) AS _raw_copies
    FROM raw_payments
    GROUP BY payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status,
             gateway_ref
),
versioned AS (
    SELECT *,
           row_number() OVER (PARTITION BY trim(payment_id)
                              ORDER BY _loaded_at DESC, _source_row_num DESC) AS _version_rank
    FROM source_rows
),
typed AS (
    SELECT
        nullif(trim(payment_id), '')                                 AS payment_id,
        nullif(trim(order_id), '')                                   AS order_id,
        try_cast(payment_ts AS TIMESTAMP)                            AS payment_ts,
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
        try_cast(amount_paid AS DOUBLE)                              AS amount_paid,
        upper(nullif(trim(payment_status), ''))                      AS payment_status,
        nullif(trim(gateway_ref), '')                                AS gateway_ref,
        _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
    FROM versioned
),
checked AS (
    SELECT p.*,
        o.order_amount                                               AS matched_order_amount,
        (p.payment_id IS NULL OR p.order_id IS NULL)                 AS chk_rule_039,
        (p.payment_mode <> 'Cash on Delivery' AND p.gateway_ref IS NULL)
                                                                     AS chk_rule_020,
        (p.payment_mode IS NOT NULL
         AND p.payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'))
                                                                     AS chk_rule_036,
        (p.payment_status IS NOT NULL
         AND p.payment_status NOT IN ('SUCCESS', 'FAILED', 'PENDING', 'REFUNDED'))
                                                                     AS chk_rule_037,
        (p.payment_status = 'SUCCESS' AND (p.amount_paid IS NULL OR p.amount_paid <= 0))
                                                                     AS chk_rule_038,
        (p.order_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM raw_orders r
            WHERE nullif(trim(r.order_id), '') = p.order_id))        AS chk_rule_017,
        (p.payment_status = 'SUCCESS' AND o.order_amount IS NOT NULL
         AND abs(p.amount_paid - o.order_amount) > 0.01)             AS chk_rule_018,
        (p.payment_ts IS NOT NULL AND p.payment_ts > now())          AS chk_rule_019
    FROM typed p
    LEFT JOIN (SELECT order_id, order_amount FROM stg_orders WHERE dq_status = 'PASS') o
           ON o.order_id = p.order_id
)
SELECT
    payment_id, order_id, payment_ts, CAST(payment_ts AS DATE) AS payment_date,
    payment_mode, amount_paid, payment_status, gateway_ref, matched_order_amount,
    round(coalesce(amount_paid, 0) - coalesce(matched_order_amount, 0), 2) AS amount_difference,
    chk_rule_039, chk_rule_020, chk_rule_036, chk_rule_037, chk_rule_038,
    chk_rule_017, chk_rule_018, chk_rule_019,
    CASE WHEN _version_rank > 1 THEN 'SUPERSEDED'
         WHEN chk_rule_039 OR chk_rule_038 OR chk_rule_017 OR chk_rule_018 OR chk_rule_019
              THEN 'QUARANTINED'
         ELSE 'PASS' END                                             AS dq_status,
    concat_ws(' | ',
        CASE WHEN _version_rank > 1 THEN 'RULE-016: payment_id must be unique (redundant copy) [Uniqueness/Critical]' END,
        CASE WHEN chk_rule_039 THEN 'RULE-039: payment_id and order_id must be present [Completeness/Critical]' END,
        CASE WHEN chk_rule_038 THEN 'RULE-038: SUCCESS payments must have amount_paid greater than zero [Accuracy/Critical]' END,
        CASE WHEN chk_rule_017 THEN 'RULE-017: order_id must exist in orders.csv [Consistency/Critical]' END,
        CASE WHEN chk_rule_018 THEN 'RULE-018: SUCCESS payment amount must equal the order amount [Consistency/Critical]' END,
        CASE WHEN chk_rule_019 THEN 'RULE-019: payment_ts must not be future-dated [Timeliness/Critical]' END
    )                                                                AS failure_reason,
    concat_ws(' | ',
        CASE WHEN chk_rule_020 THEN 'RULE-020: gateway_ref must be present for non-Cash-on-Delivery payments [Completeness/Warning]' END,
        CASE WHEN chk_rule_036 THEN 'RULE-036: payment_mode must be in the approved list [Validity/Warning]' END,
        CASE WHEN chk_rule_037 THEN 'RULE-037: payment_status must be in the approved list [Validity/Warning]' END
    )                                                                AS dq_warnings,
    _loaded_at, _source_file, _run_id, _source_row_num, _version_rank, _raw_copies
FROM checked;
