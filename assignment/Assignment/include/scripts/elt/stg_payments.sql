CREATE OR REPLACE TABLE stg_payments AS
WITH t AS (
 SELECT *, TRY_CAST(NULLIF(TRIM(payment_ts),'') AS TIMESTAMP) ts,
 TRY_CAST(NULLIF(TRIM(amount_paid),'') AS DOUBLE) amt,
 UPPER(TRIM(payment_status)) status_norm, UPPER(TRIM(payment_mode)) mode_norm
 FROM raw_payments
), c AS (
 SELECT *, CASE
 WHEN COALESCE(TRIM(payment_id),'')='' THEN 'missing_payment_id'
 WHEN COALESCE(TRIM(order_id),'')='' THEN 'missing_order_id'
 WHEN ts IS NULL THEN 'invalid_payment_ts'
 WHEN amt IS NULL OR amt<0 THEN 'invalid_amount_paid'
 WHEN status_norm NOT IN ('SUCCESS','FAILED','PENDING','REFUNDED') THEN 'invalid_payment_status'
 ELSE NULL END reject_reason FROM t
)
SELECT *, ts AS payment_ts_clean, amt AS amount_paid_clean,
 status_norm AS payment_status_clean, mode_norm AS payment_mode_clean,
 (reject_reason IS NULL) AS is_valid FROM c;
