CREATE OR REPLACE TABLE etl_payment_reconciliation AS
WITH paid AS (
  SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    SUM(TRY_CAST(amount_paid AS DOUBLE)) AS paid_amount,
    STRING_AGG(DISTINCT CAST(payment_status AS VARCHAR), ',') AS payment_status
  FROM read_csv_auto(?)
  GROUP BY CAST(order_id AS VARCHAR)
)
SELECT
  CAST(o.order_id AS VARCHAR) AS order_id,
  TRY_CAST(o.order_amount AS DOUBLE) AS order_amount,
  p.paid_amount,
  TRY_CAST(o.order_amount AS DOUBLE) - p.paid_amount AS difference,
  CASE
    WHEN p.order_id IS NULL THEN 'missing_payment'
    WHEN ABS(TRY_CAST(o.order_amount AS DOUBLE) - p.paid_amount) < 0.01 THEN 'matched'
    ELSE 'amount_mismatch'
  END AS reconciliation_status
FROM etl_fact_orders o
LEFT JOIN paid p ON CAST(o.order_id AS VARCHAR) = p.order_id
WHERE p.order_id IS NULL
   OR ABS(TRY_CAST(o.order_amount AS DOUBLE) - p.paid_amount) >= 0.01;
