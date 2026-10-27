-- The six business questions from the brief, answered twice: once off the ETL tables
-- and once off the ELT mart. Run them with
--     python -m nammamart.business_questions
-- which prints every query and its result as markdown (that output is pasted into
-- SUBMISSION.md so the numbers there are never hand-typed).
--
-- Blocks are separated by "-- name: <id>" lines.

-- name: q1_etl
-- Q1 ETL - daily net revenue by zone and store, delivered orders only, after discount
SELECT order_date, zone, store_name,
       delivered_orders,
       net_revenue
FROM etl_daily_zone_revenue
ORDER BY net_revenue DESC
LIMIT 10;

-- name: q1_elt
-- Q1 ELT - same question off the mart
SELECT order_date, zone, store_name,
       delivered_orders,
       net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
ORDER BY net_revenue DESC
LIMIT 10;

-- name: q1_totals
-- Q1 cross-check - September + increment totals per zone, both pipelines side by side
SELECT coalesce(e.zone, l.zone)            AS zone,
       round(e.etl_revenue, 2)             AS etl_revenue,
       round(l.elt_revenue, 2)             AS elt_revenue,
       round(e.etl_revenue - l.elt_revenue, 2) AS difference
FROM (SELECT zone, sum(revenue_amount) AS etl_revenue
      FROM etl_fact_orders GROUP BY zone) e
FULL OUTER JOIN (SELECT zone, sum(revenue_amount) AS elt_revenue
                 FROM mart_fact_orders GROUP BY zone) l
     ON l.zone IS NOT DISTINCT FROM e.zone
ORDER BY etl_revenue DESC NULLS LAST;

-- name: q2_etl
-- Q2 ETL - revenue and gross margin by category (margin = revenue - quantity x cost_price)
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM etl_category_margin
ORDER BY net_revenue DESC;

-- name: q2_elt
-- Q2 ELT - same question off the mart
SELECT category, delivered_orders, orders_missing_cost,
       net_revenue, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC;

-- name: q3_etl
-- Q3 ETL - average delivery time per store and % of delivered orders inside 15 minutes
SELECT store_id, store_name, zone,
       count(*)                                                   AS delivered_orders,
       round(avg(delivery_minutes), 1)                            AS avg_delivery_minutes,
       round(100.0 * avg(CASE WHEN within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min
FROM etl_fact_orders
WHERE is_revenue AND delivery_minutes IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY pct_within_15_min;

-- name: q3_elt
-- Q3 ELT - same question off the mart
SELECT store_id, store_name, zone,
       sum(delivered_orders)                                      AS delivered_orders,
       round(sum(avg_delivery_minutes * delivered_orders) / sum(delivered_orders), 1)
                                                                  AS avg_delivery_minutes,
       round(sum(pct_within_15_min * delivered_orders) / sum(delivered_orders), 1)
                                                                  AS pct_within_15_min
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
GROUP BY 1, 2, 3
ORDER BY pct_within_15_min;

-- name: q4_etl
-- Q4 ETL - revenue by payment mode, and the amount that does not reconcile
SELECT f.payment_mode,
       count(*)                            AS delivered_orders,
       round(sum(f.revenue_amount), 2)     AS net_revenue,
       round(coalesce(r.unreconciled, 0), 2) AS unreconciled_amount,
       coalesce(r.mismatches, 0)           AS mismatched_orders
FROM etl_fact_orders f
LEFT JOIN (SELECT o.payment_mode,
                  sum(abs(r.variance)) AS unreconciled,
                  count(*)             AS mismatches
           FROM etl_payment_reconciliation r
           JOIN etl_fact_orders o ON o.order_id = r.order_id
           GROUP BY o.payment_mode) r ON r.payment_mode = f.payment_mode
WHERE f.is_revenue
GROUP BY f.payment_mode, r.unreconciled, r.mismatches
ORDER BY net_revenue DESC;

-- name: q4_elt
-- Q4 ELT - same question off the mart
SELECT f.payment_mode,
       count(*)                              AS delivered_orders,
       round(sum(f.revenue_amount), 2)       AS net_revenue,
       round(coalesce(r.unreconciled, 0), 2) AS unreconciled_amount,
       coalesce(r.mismatches, 0)             AS mismatched_orders
FROM mart_fact_orders f
LEFT JOIN (SELECT o.payment_mode,
                  sum(abs(r.variance)) AS unreconciled,
                  count(*)             AS mismatches
           FROM mart_payment_reconciliation r
           JOIN mart_fact_orders o ON o.order_id = r.order_id
           GROUP BY o.payment_mode) r ON r.payment_mode = f.payment_mode
WHERE f.is_revenue
GROUP BY f.payment_mode, r.unreconciled, r.mismatches
ORDER BY net_revenue DESC;

-- name: q4_unreconciled_total
-- Q4 - the single number finance wants: total value that does not reconcile
SELECT 'etl' AS pipeline, count(*) AS mismatched_rows,
       round(sum(abs(variance)), 2) AS total_unreconciled
FROM etl_payment_reconciliation
UNION ALL
SELECT 'elt', count(*), round(sum(abs(variance)), 2)
FROM mart_payment_reconciliation;

-- name: q5_etl
-- Q5 ETL - top 10 customers by spend with loyalty tier, masked contact details only
SELECT f.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked,
       f.loyalty_tier, c.city,
       count(*)                        AS delivered_orders,
       round(sum(f.revenue_amount), 2) AS lifetime_revenue
FROM etl_fact_orders f
JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.is_revenue
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY lifetime_revenue DESC
LIMIT 10;

-- name: q5_elt
-- Q5 ELT - same question off the mart
SELECT t.customer_id, t.customer_name_masked, c.email_masked, c.phone_masked,
       t.loyalty_tier, t.city, t.delivered_orders, t.lifetime_revenue
FROM mart_top_customers t
JOIN mart_dim_customer c ON c.customer_id = t.customer_id
ORDER BY t.lifetime_revenue DESC
LIMIT 10;

-- name: q6_etl
-- Q6 ETL - rows rejected in the latest run, by file and reason
SELECT split_part(check_name, '_', 1) AS source_file,
       dimension, severity, check_name, detail, rows_failed
FROM dq_results
WHERE dag_id LIKE 'nammamart_etl%' AND status = 'FAIL'
  AND run_id = (SELECT max(run_id) FROM dq_results WHERE dag_id LIKE 'nammamart_etl%')
ORDER BY rows_failed DESC;

-- name: q6_elt
-- Q6 ELT - same question, read straight off the quarantine table
SELECT source_table, failure_reason, count(*) AS rows_rejected
FROM elt_quarantine
WHERE run_id = (SELECT max(run_id) FROM elt_quarantine)
GROUP BY 1, 2
ORDER BY rows_rejected DESC
LIMIT 15;

-- name: q6_summary
-- Q6 - the headline count, by file, both pipelines
SELECT source_table AS source_file, count(*) AS rows_rejected
FROM elt_quarantine
WHERE run_id = (SELECT max(run_id) FROM elt_quarantine)
GROUP BY 1
ORDER BY rows_rejected DESC;
