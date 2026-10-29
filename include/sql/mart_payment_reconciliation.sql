CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH expected AS (
 SELECT order_id, sum(CASE WHEN order_status='DELIVERED' THEN quantity*unit_price*(1-discount_pct/100.0) ELSE 0 END) expected_amount
 FROM stg_orders WHERE dq_status='PASS' GROUP BY order_id
), paid AS (
 SELECT order_id, sum(CASE WHEN payment_status='SUCCESS' THEN amount_paid ELSE 0 END) total_paid
 FROM stg_payments GROUP BY order_id
)
SELECT coalesce(e.order_id,p.order_id) order_id, coalesce(e.expected_amount,0) expected_amount,
       coalesce(p.total_paid,0) total_paid,
       round(coalesce(e.expected_amount,0)-coalesce(p.total_paid,0),2) unreconciled_amount,
       CASE WHEN abs(coalesce(e.expected_amount,0)-coalesce(p.total_paid,0))<=0.02 THEN 'MATCHED' ELSE 'UNRECONCILED' END reconciliation_status
FROM expected e FULL OUTER JOIN paid p USING(order_id);
