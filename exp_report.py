"""Observation report for Part F: what did the LATEST run of each pipeline do?
  python3 exp_report.py                 summary
  python3 exp_report.py orders payments also list failing checks whose name contains those words
"""
import sys
from pathlib import Path

import duckdb
import pandas as pd

DB, QDIR = "include/warehouse_anikesh.duckdb", Path("include/quarantine/anikesh")
ETL, ELT = "nammamart_etl_anikesh", "nammamart_elt_anikesh"
FILES = {"orders": "order_id_not_null", "payments": "payment_id_not_null", "customers": "customer_id_not_null",
         "products": "product_id_not_null", "stores": "store_id_not_null"}
pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)
con = duckdb.connect(DB, read_only=True)
one = lambda sql, p=None: con.execute(sql, p or []).fetchone()


def table_rows(t):
    try:
        return one(f"select count(*) from {t}")[0]
    except Exception:
        return "missing"


def latest_run(dag):
    r = con.execute("select * from pipeline_runs where dag_id = ? order by start_time desc limit 1", [dag]).df()
    return None if r.empty else r.iloc[0]


def dq_run(dag):
    r = one("select run_id from dq_results where dag_id = ? order by checked_at desc limit 1", [dag])
    return r[0] if r else None


words = [w.lower() for w in sys.argv[1:]]
for dag in (ETL, ELT):
    r = latest_run(dag)
    print("=" * 100)
    if r is None:
        print(dag, ": no runs recorded")
        continue
    secs = round((r.end_time - r.start_time).total_seconds(), 1)
    print(f"{dag}\n  run_id={r.run_id}\n  status={r.status}  duration={secs}s  extracted={r.rows_extracted}  "
          f"loaded={r.rows_loaded}  quarantined/rejected={r.rows_quarantined}")
    drun = dq_run(dag)
    print(f"  (dq_results latest run_id: {drun})")
    rows = []
    for f, chk in FILES.items():
        got = one("select rows_checked from dq_results where dag_id=? and run_id=? and check_name=?", [dag, drun, f"{f}.{chk}"])
        n_in = int(got[0]) if got else None
        if dag == ETL:
            qf = QDIR / f"{f}_failed_{r.run_id}.csv"
            bad = len(pd.read_csv(qf, dtype=str, keep_default_na=False)) if qf.exists() else 0
        else:
            t = one(f"select count(*) filter (where dq_status = 'REJECTED') from stg_{f}") if table_rows(f"stg_{f}") != "missing" else None
            bad = int(t[0]) if t else None
        stg_total = int(one(f"select count(*) from stg_{f}")[0]) if (dag == ELT and table_rows(f"stg_{f}") != "missing") else None
        total = stg_total if dag == ELT else n_in
        row = dict(file=f, rows_in=n_in, rows_set_aside=bad,
                   rows_passed=(total - bad) if (total is not None and bad is not None) else None)
        if dag == ELT:
            row["rows_in_staging"] = stg_total
        rows.append(row)
    if dag == ETL:
        qf = QDIR / f"orders_increment_failed_{r.run_id}.csv"
        rows.append(dict(file="orders_increment", rows_in=None, rows_set_aside=len(pd.read_csv(qf)) if qf.exists() else 0, rows_passed=None))
    print(pd.DataFrame(rows).to_string(index=False))
    if dag == ELT:
        print("  note: ELT rows_passed = rows in staging minus rejected; staging holds the deduplicated base file plus the increment")
    if drun:
        s = con.execute("select status, count(*) n from dq_results where dag_id=? and run_id=? group by 1 order by 1", [dag, drun]).df()
        print("  dq_results by status:", dict(zip(s.status, s.n)))
        if words:
            c = con.execute("select check_name, severity, rows_checked, rows_failed, status from dq_results "
                            "where dag_id=? and run_id=? and rows_failed > 0 order by 1", [dag, drun]).df()
            c = c[c.check_name.str.lower().apply(lambda x: any(w in x for w in words))]
            print(c.to_string(index=False) if len(c) else "  (no failing checks match)")

print("=" * 100)
print("row counts: etl_fact_orders =", table_rows("etl_fact_orders"), "| raw_orders =", table_rows("raw_orders"),
      "| stg_orders =", table_rows("stg_orders"))
rev = lambda t, expr="net_revenue": con.execute(f"select zone, round(sum({expr}),2) net_revenue from {t} group by 1 order by 1").df()
try:
    z = rev("etl_daily_zone_revenue").merge(rev("mart_daily_store_kpis"), on="zone", how="outer", suffixes=("_etl", "_elt"))
    print(z.to_string(index=False))
    print("total net revenue  ETL =", round(z.net_revenue_etl.sum(), 2), " ELT =", round(z.net_revenue_elt.sum(), 2))
except Exception as e:
    print("revenue tables not readable:", e)
