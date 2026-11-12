-- Q1 Daily net revenue by zone and store (delivered, after discount)
SELECT order_date, zone, store_id, store_name, net_revenue FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id LIMIT 10;
-- Q1b Total by zone
SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY 2 DESC;
-- Q2 Revenue and gross margin by category
SELECT category, net_revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY net_revenue DESC;
-- Q3 Average delivery time per store and % delivered within 15 minutes
SELECT f.store_id, s.store_name, round(avg(f.delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * avg(f.within_15_min::INT), 2) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id) WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2 ORDER BY 1;
-- Q4 Revenue by payment mode, and total amount that does not reconcile with the gateway
SELECT * FROM etl_payment_mode_revenue;
SELECT mismatch_reason, count(*) AS rows, round(sum(difference), 2) AS total_difference, round(sum(abs(difference)), 2) AS abs_difference
FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC;
-- Q5 Top 10 customers (masked)
SELECT customer_id, full_name, loyalty_tier, email_masked, phone_masked, delivered_orders, total_spend FROM etl_top_customers ORDER BY total_spend DESC;
-- Q6 Rows rejected in the latest run, by file and reason
SELECT source_file, failure_reason, count(*) AS rows FROM etl_quarantine_log
WHERE run_id = (SELECT run_id FROM etl_quarantine_log ORDER BY logged_at DESC LIMIT 1) GROUP BY 1, 2 ORDER BY 1, 3 DESC;
