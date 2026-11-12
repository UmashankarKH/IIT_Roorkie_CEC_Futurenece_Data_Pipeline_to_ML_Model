-- Q1 Daily net revenue by zone and store (delivered, after discount)
SELECT order_date, zone, store_id, store_name, net_revenue FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id LIMIT 10;
-- Q1b Total by zone
SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY 1 ORDER BY 2 DESC;
-- Q2 Revenue and gross margin by category
SELECT category, net_revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY net_revenue DESC;
-- Q3 Average delivery time per store and % delivered within 15 minutes
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY store_id;
-- Q4 Revenue by payment mode, and total amount that does not reconcile with the gateway
SELECT * FROM mart_payment_mode_revenue;
SELECT mismatch_reason, count(*) AS rows, round(sum(difference), 2) AS total_difference, round(sum(abs(difference)), 2) AS abs_difference
FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC;
-- Q5 Top 10 customers (masked)
SELECT customer_id, full_name, loyalty_tier, email_masked, phone_masked, delivered_orders, total_spend FROM mart_top_customers ORDER BY total_spend DESC;
-- Q6 Rows rejected, by file and reason
SELECT source_file, failure_reason, count(*) AS rows FROM mart_rejected_rows GROUP BY 1, 2 ORDER BY 1, 3 DESC;
