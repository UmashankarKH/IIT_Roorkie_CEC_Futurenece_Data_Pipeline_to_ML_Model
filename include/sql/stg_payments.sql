-- Cast payments and retain mismatches for the reconciliation mart.
CREATE OR REPLACE TABLE stg_payments AS
SELECT
    payment_id,
    order_id,
    try_cast(payment_ts AS TIMESTAMP) AS payment_ts,
    CASE
        WHEN lower(trim(payment_mode)) = 'upi' THEN 'UPI'
        WHEN lower(trim(payment_mode)) = 'card' THEN 'Card'
        WHEN lower(trim(payment_mode)) IN ('cod', 'cashh', 'cash on delivery') THEN 'Cash on Delivery'
        WHEN lower(trim(payment_mode)) = 'wallet' THEN 'Wallet'
        ELSE trim(payment_mode)
    END AS payment_mode,
    try_cast(amount_paid AS DECIMAL(12, 2)) AS amount_paid,
    upper(trim(payment_status)) AS payment_status,
    gateway_ref,
    CASE WHEN payment_id <> '' AND order_id <> ''
              AND try_cast(amount_paid AS DECIMAL(12, 2)) > 0
         THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    concat_ws('; ',
        CASE WHEN payment_id = '' THEN 'Missing payment_id' END,
        CASE WHEN order_id = '' THEN 'Missing order_id' END,
        CASE WHEN try_cast(amount_paid AS DECIMAL(12, 2)) <= 0 THEN 'Payment amount is not positive' END
    ) AS failure_reason
FROM raw_payments;
