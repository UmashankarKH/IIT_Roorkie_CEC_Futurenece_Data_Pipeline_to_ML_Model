CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH o AS (SELECT order_id, MAX(order_amount_clean) order_amount FROM stg_orders
 WHERE is_valid AND order_status_clean='DELIVERED' GROUP BY order_id),
p AS (SELECT order_id, SUM(CASE WHEN payment_status_clean='SUCCESS' THEN amount_paid_clean ELSE 0 END) paid_amount
 FROM stg_payments WHERE is_valid GROUP BY order_id)
SELECT COALESCE(o.order_id,p.order_id) order_id, o.order_amount,
 COALESCE(p.paid_amount,0) paid_amount, COALESCE(p.paid_amount,0)-COALESCE(o.order_amount,0) reconciliation_difference
FROM o FULL OUTER JOIN p ON o.order_id=p.order_id;
