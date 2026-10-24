-- One row per order/payment mismatch, including missing or extra payments.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH joined AS (
    SELECT
        -- Payment-only rows keep the payment's order_id for look-up.
        COALESCE(o.order_id, p.order_id) AS order_id,
        p.payment_id,
        o.order_amount,
        p.amount_paid,
        o.payment_mode AS order_payment_mode,
        p.payment_mode AS payment_payment_mode,
        p.payment_status,
        CASE
            WHEN p.order_id IS NULL THEN 'Missing payment for order'
            WHEN o.order_id IS NULL THEN 'Payment references an order outside the clean orders'
            WHEN p.payment_status = 'SUCCESS' AND abs(p.amount_paid - o.order_amount) > 0.01 THEN 'Successful payment amount mismatch'
            WHEN o.payment_mode <> p.payment_mode THEN 'Payment mode mismatch'
            ELSE NULL
        END AS reason
    -- Only passing orders are on the order side (same as the ETL fact table),
    -- so payments for quarantined orders surface as mismatches, not matches.
    FROM (SELECT * FROM stg_orders WHERE dq_status = 'PASS') o
    FULL OUTER JOIN stg_payments p ON o.order_id = p.order_id
)
SELECT * FROM joined WHERE reason IS NOT NULL;
