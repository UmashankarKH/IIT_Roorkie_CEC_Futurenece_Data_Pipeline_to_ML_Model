CREATE OR REPLACE TABLE stg_orders AS
WITH t AS (
 SELECT *,
  TRY_CAST(NULLIF(TRIM(order_ts),'') AS TIMESTAMP) ts,
  TRY_CAST(NULLIF(TRIM(quantity),'') AS INTEGER) qty,
  TRY_CAST(NULLIF(TRIM(unit_price),'') AS DOUBLE) price,
  TRY_CAST(NULLIF(TRIM(discount_pct),'') AS DOUBLE) disc,
  TRY_CAST(NULLIF(TRIM(order_amount),'') AS DOUBLE) amount,
  TRY_CAST(NULLIF(TRIM(delivery_minutes),'') AS DOUBLE) delivery,
  UPPER(TRIM(order_status)) status_norm,
  UPPER(TRIM(payment_mode)) mode_norm
 FROM raw_orders
), c AS (
 SELECT *,
  CASE
   WHEN COALESCE(TRIM(order_id),'')='' THEN 'missing_order_id'
   WHEN ts IS NULL THEN 'invalid_order_ts'
   WHEN qty IS NULL OR qty<=0 THEN 'invalid_quantity'
   WHEN price IS NULL OR price<0 THEN 'invalid_unit_price'
   WHEN disc IS NULL OR disc<0 OR disc>100 THEN 'invalid_discount_pct'
   WHEN amount IS NULL OR ABS(amount-ROUND(qty*price*(1-disc/100),2))>1.00 THEN 'order_amount_mismatch'
   WHEN status_norm NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'invalid_order_status'
   WHEN mode_norm NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET') THEN 'invalid_payment_mode'
   ELSE NULL
  END reject_reason
 FROM t
)
SELECT *, ts AS order_ts_clean, qty AS quantity_clean, price AS unit_price_clean,
 disc AS discount_pct_clean, amount AS order_amount_clean, delivery AS delivery_minutes_clean,
 status_norm AS order_status_clean, mode_norm AS payment_mode_clean,
 (reject_reason IS NULL) AS is_valid FROM c;
