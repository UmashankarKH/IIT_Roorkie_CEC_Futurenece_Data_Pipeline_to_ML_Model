-- NammaMart business questions: each is one simple query on the final tables.
-- "ETL" queries read the etl_* tables, "ELT" queries read the mart_* tables. Both warehouses live in
-- include/warehouse_sbonagiri.duckdb. Lines starting with "-- @" label a query.

-- @Q1 ETL: daily net revenue by zone and store (delivered orders only, after discount)
SELECT order_date, zone, store_id, store_name, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone, store_id;

-- @Q1 ELT
SELECT order_date, zone, store_id, store_name, net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
ORDER BY order_date, zone, store_id;

-- @Q2 ETL: revenue and gross margin by product category
SELECT category, revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY revenue DESC;

-- @Q2 ELT
SELECT category, revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY revenue DESC;

-- @Q3 ETL: average delivery time per store and % of delivered orders within 15 minutes
SELECT s.store_id, s.store_name, COUNT(*) AS delivered_orders,
       ROUND(AVG(f.delivery_minutes), 2) AS avg_delivery_minutes,
       ROUND(100.0 * AVG(CAST(f.within_15_min AS INTEGER)), 2) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name ORDER BY s.store_id;

-- @Q3 ELT
SELECT store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla ORDER BY store_id;

-- @Q4a ETL: revenue by payment mode
SELECT payment_mode, COUNT(*) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY payment_mode ORDER BY net_revenue DESC;

-- @Q4a ELT
SELECT payment_mode, delivered_orders, net_revenue FROM mart_payment_mode_revenue ORDER BY net_revenue DESC;

-- @Q4b ETL: amount that does not reconcile with the payment gateway, by reason
SELECT COALESCE(mismatch_reason, 'TOTAL') AS mismatch_reason, COUNT(*) AS records, ROUND(SUM(ABS(difference)), 2) AS unreconciled_amount
FROM etl_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason IS NULL, mismatch_reason;

-- @Q4b ELT
SELECT COALESCE(mismatch_reason, 'TOTAL') AS mismatch_reason, COUNT(*) AS records, ROUND(SUM(ABS(difference)), 2) AS unreconciled_amount
FROM mart_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason IS NULL, mismatch_reason;

-- @Q5 ETL: top 10 customers by spend with loyalty tier (masked contact details only)
SELECT ROW_NUMBER() OVER (ORDER BY SUM(f.net_revenue) DESC, c.customer_id) AS spend_rank,
       c.customer_id, c.full_name, c.email_masked, c.loyalty_tier,
       COUNT(*) AS delivered_orders, ROUND(SUM(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.email_masked, c.loyalty_tier
QUALIFY spend_rank <= 10 ORDER BY spend_rank;

-- @Q5 ELT
SELECT spend_rank, customer_id, full_name, email_masked, loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers ORDER BY spend_rank;

-- @Q6 ETL: rows rejected in the latest run, by file and reason
SELECT q.source_file, u.reason, COUNT(*) AS rows_rejected
FROM etl_quarantine q, UNNEST(STRING_SPLIT(q.failure_reason, '; ')) AS u(reason)
WHERE q.run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_sbonagiri'
                  ORDER BY start_time DESC LIMIT 1)
GROUP BY q.source_file, u.reason ORDER BY q.source_file, rows_rejected DESC;

-- @Q6 ELT
SELECT r.source_file, u.reason, COUNT(*) AS rows_rejected
FROM mart_rejected_rows r, UNNEST(STRING_SPLIT(r.failure_reason, '; ')) AS u(reason)
GROUP BY r.source_file, u.reason ORDER BY r.source_file, rows_rejected DESC;
