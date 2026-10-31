"""Answers the six business questions from both pipelines. Writes business_answers.md for SUBMISSION.md."""
import sys
import textwrap

import duckdb
import pandas as pd

DB = sys.argv[1] if len(sys.argv) > 1 else "include/warehouse_anikesh.duckdb"
ETL_DAG, ELT_DAG = "nammamart_etl_anikesh", "nammamart_elt_anikesh"
LATEST = ("run_id = (SELECT run_id FROM dq_results WHERE dag_id = '{dag}' "
          "ORDER BY checked_at DESC LIMIT 1)")

# (title, ETL sql, ELT sql, same_expected)
QUESTIONS = [
    ("Q1a. Net revenue by zone (delivered orders, after discount)",
     "SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue GROUP BY zone ORDER BY zone",
     "SELECT zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY zone ORDER BY zone",
     True),
    ("Q1b. Net revenue by store",
     "SELECT store_id, store_name, zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue "
     "GROUP BY 1, 2, 3 ORDER BY net_revenue DESC, store_id",
     "SELECT store_id, store_name, zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis "
     "GROUP BY 1, 2, 3 ORDER BY net_revenue DESC, store_id",
     True),
    ("Q1c. Daily net revenue by zone (30 Sep 2026 shown; the table holds every day)",
     "SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue "
     "WHERE order_date = DATE '2026-09-30' GROUP BY 1, 2 ORDER BY 2",
     "SELECT order_date, zone, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis "
     "WHERE order_date = DATE '2026-09-30' GROUP BY 1, 2 ORDER BY 2",
     True),
    ("Q2. Revenue and gross margin by category (margin = revenue - quantity x cost_price)",
     "SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY net_revenue DESC",
     "SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY net_revenue DESC",
     True),
    ("Q3. Average delivery time per store and % of delivered orders within 15 minutes",
     "SELECT f.store_id, coalesce(s.store_name, 'UNKNOWN') AS store_name, count(*) AS delivered_orders, "
     "round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes, "
     "round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min "
     "FROM etl_fact_orders f LEFT JOIN etl_dim_store s ON s.store_id = f.store_id "
     "WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2 ORDER BY 1",
     "SELECT store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY store_id",
     True),
    ("Q4a. Revenue by payment mode (delivered orders)",
     "SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue "
     "FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY net_revenue DESC",
     "SELECT payment_mode, count(*) AS delivered_orders, round(sum(order_amount), 2) AS net_revenue "
     "FROM stg_orders WHERE dq_status <> 'REJECTED' AND order_status = 'DELIVERED' GROUP BY 1 ORDER BY net_revenue DESC",
     True),
    ("Q4b. Amount that does not reconcile with the payment gateway, by reason (TOTAL = answer)",
     "SELECT coalesce(reason, 'TOTAL') AS reason, count(*) AS mismatch_rows, round(sum(abs(difference)), 2) AS amount "
     "FROM etl_payment_reconciliation GROUP BY ROLLUP(reason) ORDER BY reason IS NULL, amount DESC",
     "SELECT coalesce(reason, 'TOTAL') AS reason, count(*) AS mismatch_rows, round(sum(abs(difference)), 2) AS amount "
     "FROM mart_payment_reconciliation GROUP BY ROLLUP(reason) ORDER BY reason IS NULL, amount DESC",
     True),
    ("Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)",
     "SELECT f.customer_id, c.email_masked, c.phone_masked, c.loyalty_tier, count(*) AS delivered_orders, "
     "round(sum(f.net_revenue), 2) AS total_spend FROM etl_fact_orders f JOIN etl_dim_customer c ON c.customer_id = f.customer_id "
     "WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2, 3, 4 ORDER BY total_spend DESC, f.customer_id LIMIT 10",
     "SELECT customer_id, email_masked, phone_masked, loyalty_tier, delivered_orders, total_spend FROM mart_top_customers ORDER BY spend_rank",
     True),
    ("Q6a. Rows rejected in the latest run, by file and by reason (a row failing 2 rules is counted under each)",
     "SELECT split_part(check_name, '.', 1) AS file, split_part(check_name, '.', 2) AS reason, rows_failed FROM dq_results "
     "WHERE dag_id = '" + ETL_DAG + "' AND " + LATEST.format(dag=ETL_DAG) + " AND severity = 'Critical' AND rows_failed > 0 ORDER BY 1, 3 DESC",
     "SELECT split_part(check_name, '.', 1) AS file, split_part(check_name, '.', 2) AS reason, rows_failed FROM dq_results "
     "WHERE dag_id = '" + ELT_DAG + "' AND " + LATEST.format(dag=ELT_DAG) + " AND severity = 'Critical' AND rows_failed > 0 ORDER BY 1, 3 DESC",
     False),
    ("Q6b. Total rows set aside in the latest run (from the run audit)",
     "SELECT dag_id, status, rows_extracted, rows_loaded, rows_quarantined FROM pipeline_runs WHERE dag_id = '" + ETL_DAG + "' ORDER BY start_time DESC LIMIT 1",
     "SELECT dag_id, status, rows_extracted, rows_loaded, rows_quarantined FROM pipeline_runs WHERE dag_id = '" + ELT_DAG + "' ORDER BY start_time DESC LIMIT 1",
     False),
]

con = duckdb.connect(DB, read_only=True)
md = ["# Business questions: answers from both pipelines", ""]
print(f"{'question':92}{'ETL = ELT?'}")
for title, etl_sql, elt_sql, same_expected in QUESTIONS:
    etl_df, elt_df = con.execute(etl_sql).df(), con.execute(elt_sql).df()
    verdict = "n/a (different sources)"
    if same_expected:
        try:
            pd.testing.assert_frame_equal(etl_df.reset_index(drop=True), elt_df.reset_index(drop=True),
                                          check_dtype=False, rtol=1e-6, atol=0.011)
            verdict = "SAME"
        except AssertionError:
            verdict = "DIFFERENT"
    print(f"{title[:90]:92}{verdict}")
    md += [f"## {title}", "", f"**Same result in ETL and ELT:** {verdict}", "",
           "**ETL SQL**", "```sql", textwrap.fill(etl_sql, 110), "```", "```text", etl_df.to_string(index=False), "```", "",
           "**ELT SQL**", "```sql", textwrap.fill(elt_sql, 110), "```", "```text", elt_df.to_string(index=False), "```", ""]
open("business_answers.md", "w").write("\n".join(md))
print("\nwrote business_answers.md")
