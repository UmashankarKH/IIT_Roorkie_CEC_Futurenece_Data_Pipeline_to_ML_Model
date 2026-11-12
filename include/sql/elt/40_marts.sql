-- Marts: business-ready tables, SQL only. Everything reads staging rows with dq_status <> 'FAIL', so a rejected
-- row cannot reach a mart. Revenue = DELIVERED orders only, after discount (order_amount already is).

CREATE OR REPLACE VIEW v_clean_orders AS
SELECT o.order_id, o.order_ts, CAST(o.order_ts AS DATE) AS order_date, o.customer_id, o.store_id, o.product_id,
       CAST(o.quantity AS INTEGER) AS quantity, o.order_amount, o.payment_mode, o.order_status, o.delivery_minutes,
       CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END AS net_revenue,
       CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount - o.quantity * p.cost_price ELSE 0 END AS gross_margin,
       o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15 AS within_15_min
FROM stg_orders o JOIN stg_products p USING (product_id)
WHERE o.dq_status <> 'FAIL' AND p.dq_status <> 'FAIL';

-- Q1: daily net revenue by zone and store (also carries the delivery KPIs)
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date, s.zone, o.store_id, s.store_name, count(*) AS delivered_orders,
       round(sum(o.net_revenue), 2) AS net_revenue,
       round(avg(o.delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * avg(o.within_15_min::INT), 2) AS pct_within_15_min
FROM v_clean_orders o JOIN stg_stores s USING (store_id)
WHERE o.order_status = 'DELIVERED' AND s.dq_status <> 'FAIL'
GROUP BY ALL ORDER BY 1, 2, 3;

-- Q2: revenue and gross margin by category
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category, round(sum(o.net_revenue), 2) AS net_revenue,
       round(sum(o.net_revenue - o.gross_margin), 2) AS cogs, round(sum(o.gross_margin), 2) AS gross_margin,
       round(100 * sum(o.gross_margin) / sum(o.net_revenue), 2) AS margin_pct
FROM v_clean_orders o JOIN stg_products p USING (product_id)
WHERE o.order_status = 'DELIVERED' AND p.dq_status <> 'FAIL'
GROUP BY 1 ORDER BY 2 DESC;

-- Q3: delivery SLA per store
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT o.store_id, s.store_name, s.zone, count(*) AS delivered_orders,
       round(avg(o.delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * avg(o.within_15_min::INT), 2) AS pct_within_15_min
FROM v_clean_orders o JOIN stg_stores s USING (store_id)
WHERE o.order_status = 'DELIVERED' AND s.dq_status <> 'FAIL'
GROUP BY 1, 2, 3 ORDER BY 1;

-- Q4a: revenue by payment mode
CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM v_clean_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC;

-- Q4b: one row per order/payment mismatch, with a reason
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH p AS (SELECT * FROM stg_payments WHERE dq_status <> 'FAIL'),
j AS (
    SELECT o.order_id, p.payment_id, o.order_status, o.order_amount, p.payment_status, p.amount_paid,
           o.payment_mode AS o_mode, p.payment_mode AS p_mode
    FROM v_clean_orders o LEFT JOIN p ON p.order_id = o.order_id
),
classified AS (
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       CASE WHEN payment_status = 'FAILED' THEN -order_amount ELSE round(coalesce(amount_paid, 0) - order_amount, 2) END AS difference,
       CASE
         WHEN order_status = 'DELIVERED' AND payment_id IS NULL THEN 'missing_payment'
         WHEN order_status = 'DELIVERED' AND payment_status = 'FAILED' THEN 'payment_failed_for_delivered_order'
         WHEN order_status = 'DELIVERED' AND payment_status = 'SUCCESS' AND abs(amount_paid - order_amount) > 0.01
              THEN 'amount_mismatch'
         WHEN order_status = 'DELIVERED' AND payment_status = 'REFUNDED' THEN 'refund_without_return'
         WHEN order_status = 'RETURNED' AND payment_id IS NOT NULL AND payment_status <> 'REFUNDED'
              THEN 'return_not_refunded'
         WHEN order_status = 'RETURNED' AND payment_status = 'REFUNDED' AND abs(amount_paid - order_amount) > 0.01
              THEN 'amount_mismatch'
         WHEN order_status = 'CANCELLED' AND payment_id IS NOT NULL THEN 'payment_for_cancelled_order'
         WHEN payment_id IS NOT NULL AND o_mode <> p_mode THEN 'payment_mode_mismatch'
       END AS mismatch_reason
FROM j
)
SELECT * FROM classified WHERE mismatch_reason IS NOT NULL
UNION ALL
SELECT order_id, payment_id, NULL, NULL, payment_status, amount_paid, amount_paid, 'orphan_payment'
FROM stg_payments WHERE failure_reason LIKE '%payments_order_exists%';

-- Q5: top 10 customers, masked PII only
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT c.customer_id, c.full_name, c.loyalty_tier, c.email_masked, c.phone_masked,
       count(*) AS delivered_orders, round(sum(o.net_revenue), 2) AS total_spend
FROM v_clean_orders o JOIN stg_customers c USING (customer_id)
WHERE o.order_status = 'DELIVERED' AND c.dq_status <> 'FAIL'
GROUP BY ALL ORDER BY total_spend DESC LIMIT 10;

-- Q6: rejected rows by file and reason (one row per row and reason; a row failing 2 rules appears twice)
CREATE OR REPLACE TABLE mart_rejected_rows AS
WITH rej AS (
    SELECT 'orders' AS source_file, _row_num AS source_row, order_id AS row_key, failure_reason
      FROM stg_orders WHERE dq_status = 'FAIL'
    UNION ALL SELECT 'payments', _row_num, payment_id, failure_reason FROM stg_payments WHERE dq_status = 'FAIL'
    UNION ALL SELECT 'customers', _row_num, customer_id, failure_reason FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL SELECT 'products', _row_num, product_id, failure_reason FROM stg_products WHERE dq_status = 'FAIL'
    UNION ALL SELECT 'stores', _row_num, store_id, failure_reason FROM stg_stores WHERE dq_status = 'FAIL'
)
SELECT '{{run_id}}' AS run_id, source_file, source_row, row_key,
       unnest(string_split(failure_reason, '; ')) AS failure_reason, now() AS logged_at
FROM rej;
