"""Parity check: do the ETL and ELT pipelines agree on the same data? Prints a table and exits 1 on any difference."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))
import nammamart_common as nc  # noqa: E402

POST_INCREMENT = "--post-increment" in sys.argv  # quarantine log is per run, staging is cumulative
con = nc.connect(read_only=True)
q = lambda sql: con.execute(sql).fetchall()  # noqa: E731
checks = []


def cmp(name, a, b):
    checks.append((name, a, b, a == b))


cmp("clean orders (fact vs stg non-FAIL)", q("SELECT count(*) FROM etl_fact_orders"),
    q("SELECT count(*) FROM stg_orders WHERE dq_status <> 'FAIL'"))
cmp("rejected order rows", q("SELECT count(DISTINCT source_row) FROM etl_quarantine_log WHERE source_file='orders' AND run_id=(SELECT max(run_id) FROM etl_quarantine_log)"),
    q("SELECT count(*) FROM stg_orders WHERE dq_status = 'FAIL'"))
cmp("net revenue (all delivered)", q("SELECT round(sum(net_revenue),2) FROM etl_fact_orders"),
    q("SELECT round(sum(net_revenue),2) FROM mart_payment_mode_revenue"))
cmp("daily/store revenue rows+sum", q("SELECT count(*), round(sum(net_revenue),2) FROM etl_daily_zone_revenue"),
    q("SELECT count(*), round(sum(net_revenue),2) FROM mart_daily_store_kpis"))
cmp("category margin", q("SELECT category, net_revenue, gross_margin FROM etl_category_margin ORDER BY 1"),
    q("SELECT category, net_revenue, gross_margin FROM mart_category_margin ORDER BY 1"))
cmp("payment mode revenue", q("SELECT * FROM etl_payment_mode_revenue ORDER BY 1"),
    q("SELECT * FROM mart_payment_mode_revenue ORDER BY 1"))
cmp("reconciliation rows", q("SELECT order_id, payment_id, mismatch_reason, difference FROM etl_payment_reconciliation ORDER BY 1,2,3"),
    q("SELECT order_id, payment_id, mismatch_reason, difference FROM mart_payment_reconciliation ORDER BY 1,2,3"))
cmp("top customers", q("SELECT customer_id, total_spend FROM etl_top_customers ORDER BY 2 DESC, 1"),
    q("SELECT customer_id, total_spend FROM mart_top_customers ORDER BY 2 DESC, 1"))
cmp("PII masked (dim vs stg)", q("SELECT customer_id, email_masked, phone_masked, loyalty_tier FROM etl_dim_customer ORDER BY 1"),
    q("SELECT customer_id, email_masked, phone_masked, loyalty_tier FROM stg_customers WHERE dq_status<>'FAIL' ORDER BY 1"))
cmp("fact rows identical", q("SELECT order_id, order_status, order_amount, net_revenue, round(gross_margin,2), within_15_min FROM etl_fact_orders ORDER BY 1"),
    q("SELECT order_id, order_status, order_amount, net_revenue, round(gross_margin,2), within_15_min FROM v_clean_orders ORDER BY 1"))
cmp("failure reasons per rejected order", q("SELECT row_key, failure_reason FROM etl_quarantine_log WHERE source_file='orders' AND run_id=(SELECT max(run_id) FROM etl_quarantine_log) ORDER BY 1, 2"),
    q("SELECT row_key, failure_reason FROM mart_rejected_rows WHERE source_file='orders' ORDER BY 1, 2"))
cmp("dq_results failed counts (latest run per DAG)", q("""SELECT check_name, rows_failed FROM dq_results WHERE dag_id='nammamart_etl_mohanrajs'
    AND run_id=(SELECT run_id FROM dq_results WHERE dag_id='nammamart_etl_mohanrajs' ORDER BY checked_at DESC LIMIT 1)
    AND check_name NOT LIKE '%volume%' ORDER BY 1"""), q("""SELECT check_name, rows_failed FROM dq_results WHERE dag_id='nammamart_elt_mohanrajs'
    AND run_id=(SELECT run_id FROM dq_results WHERE dag_id='nammamart_elt_mohanrajs' ORDER BY checked_at DESC LIMIT 1)
    AND check_name NOT LIKE '%volume%' ORDER BY 1"""))
if POST_INCREMENT:
    checks = [c for c in checks if not c[0].startswith(("rejected order rows", "failure reasons per rejected"))]
bad = 0
for name, a, b, ok in checks:
    print(f"{'OK  ' if ok else 'DIFF'} {name}" + ("" if ok else f"\n   ETL: {a[:6]}\n   ELT: {b[:6]}"))
    bad += not ok
sys.exit(1 if bad else 0)
