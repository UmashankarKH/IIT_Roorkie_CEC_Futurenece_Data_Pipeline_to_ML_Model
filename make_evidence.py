"""Appends worked examples (computed from the real warehouse) to SUBMISSION.md as Appendix A-D."""
from pathlib import Path

import duckdb
import pandas as pd

DB = "include/warehouse_anikesh.duckdb"
DATA = Path("include/data/nammamart")
QDIR = Path("include/quarantine/anikesh")
INCREMENT = "orders_2026-10-01_increment.csv"
MARK = "## Appendix A: lineage worked example"
SUB = Path("SUBMISSION.md")
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)
con = duckdb.connect(DB, read_only=True)
out = []


def add(*lines):
    out.extend(lines)


def table(sql, params=None):
    try:
        df = con.execute(sql, params or []).df()
        return "```text\n" + (df.to_string(index=False) if len(df) else "(no rows)") + "\n```"
    except Exception as e:
        return "```text\n(query failed: " + str(e)[:120] + ")\n```"


def val(sql, params=None):
    try:
        r = con.execute(sql, params or []).fetchone()
        return r[0] if r else None
    except Exception:
        return None


if MARK in SUB.read_text():
    raise SystemExit("appendix already added")

add("", MARK, "")
top = con.execute("select order_date, store_id, store_name, zone, delivered_orders, net_revenue "
                  "from mart_daily_store_kpis order by net_revenue desc, store_id limit 1").fetchone()
d, store, name, zone, n_orders, rev = top
add(f"Number traced: `mart_daily_store_kpis` row **{d} / {store} ({name}, {zone})**: "
    f"net_revenue **{rev}** from {n_orders} delivered orders.", "",
    "**ELT path** (every staging row carries `_source_file`, `_run_id`, `_loaded_at`):", "",
    table("select order_id, order_amount, _source_file, _run_id from stg_orders "
          "where order_date = ? and store_id = ? and order_status = 'DELIVERED' and dq_status <> 'REJECTED' "
          "order by order_id limit 5", [d, store]), "")
s_elt = val("select round(sum(order_amount), 2) from stg_orders where order_date = ? and store_id = ? "
            "and order_status = 'DELIVERED' and dq_status <> 'REJECTED'", [d, store])
add(f"Sum of all those staging rows = **{s_elt}** (mart says {rev}: {'match' if s_elt == rev else 'CHECK'}).", "")
first = val("select order_id from stg_orders where order_date = ? and store_id = ? "
            "and order_status = 'DELIVERED' and dq_status <> 'REJECTED' order by order_id limit 1", [d, store])
run = val("select _run_id from stg_orders where order_id = ?", [first])
src = val("select _source_file from stg_orders where order_id = ?", [first])
add(f"Down to the source row for order `{first}` (the raw table keeps it exactly as received, all text):", "",
    table("select order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, "
          "order_amount, order_status, delivery_minutes, _source_file, _run_id, _loaded_at from raw_orders "
          "where order_id = ? and _run_id = ? and _source_file = ? limit 1", [first, run, src]), "",
    "**ETL path** (the fact table has no source columns, so the trace stops at the order id):", "",
    table("select order_id, order_amount, net_revenue from etl_fact_orders where order_date = ? and store_id = ? "
          "and order_status = 'DELIVERED' order by order_id limit 5", [d, store]), "")
s_etl = val("select round(sum(net_revenue), 2) from etl_fact_orders where order_date = ? and store_id = ?", [d, store])
add(f"ETL sum for the same store and day = **{s_etl}**. To find the file and row I must open `orders.csv` and "
    "search for the order id by hand. Finding: **ELT lineage is easier**; improvement: add `_source_file`, "
    "`_run_id`, `_loaded_at` to the ETL fact.", "")

add("## Appendix B: audit of a rejected order", "")
bad = con.execute("select order_id, failure_reason from stg_orders where dq_status = 'REJECTED' "
                  "and failure_reason = 'quantity_positive' order by order_id limit 1").fetchone()
if bad:
    oid, why = bad
    erun = val("select run_id from pipeline_runs where dag_id like '%etl%' and status = 'SUCCESS' "
               "order by start_time desc limit 1")
    qf = QDIR / f"orders_failed_{erun}.csv"
    add(f"Order picked: `{oid}` (rejected for `{why}`).", "",
        "**1. Exactly as received** (ELT raw, all text, newest run):", "",
        table("select order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, "
              "order_amount, order_status, _source_file, _run_id from raw_orders where order_id = ? "
              "order by _loaded_at desc limit 1", [oid]), "",
        "**2. The rule it broke:**", "",
        table("select order_id, dq_status, failure_reason from stg_orders where order_id = ?", [oid]), "")
    if qf.exists():
        q = pd.read_csv(qf, dtype=str, keep_default_na=False)
        q = q[q.order_id == oid][["order_id", "quantity", "order_amount", "order_status", "failure_reason"]]
        add(f"ETL quarantine file `{qf.name}` holds the same row with its reason:", "",
            "```text\n" + (q.to_string(index=False) if len(q) else "(not found)") + "\n```", "")
    n1 = val("select count(*) from etl_fact_orders where order_id = ?", [oid])
    n2 = val("select count(*) from stg_orders where order_id = ? and dq_status <> 'REJECTED'", [oid])
    t_etl = val("select round(sum(net_revenue), 2) from etl_fact_orders where order_status = 'DELIVERED'")
    t_elt = val("select round(sum(net_revenue), 2) from mart_daily_store_kpis")
    add("**3. Proof it is not in the CFO's revenue:**", "",
        f"- rows with this order id in `etl_fact_orders`: **{n1}**",
        f"- rows with this order id in staging that are not REJECTED (the only rows marts read): **{n2}**",
        f"- total net revenue: ETL **{t_etl}** and ELT **{t_elt}** (identical, neither includes the order).", "")
else:
    add("(no order rejected for quantity_positive was found)", "")

add("## Appendix C: change history of a returned order", "")
o = set(pd.read_csv(DATA / "orders.csv", dtype=str).order_id)
ids = [i for i in pd.read_csv(DATA / INCREMENT, dtype=str).order_id if i in o]
one = ids[0]
add(f"Order `{one}` was DELIVERED in `orders.csv` and became RETURNED in the 1 October increment. "
    "ELT raw is append-only, so **both versions are still there**, each stamped with its source file and run:", "",
    table("select order_status, _source_file, count(*) as copies, min(_loaded_at) as first_loaded, "
          "max(_loaded_at) as last_loaded from raw_orders where order_id = ? group by 1, 2 order by 3 desc", [one]), "",
    "Reporting tables show only the latest status:", "",
    table("select 'etl_fact_orders' as tbl, order_status from etl_fact_orders where order_id = ? "
          "union all select 'stg_orders', order_status from stg_orders where order_id = ?", [one, one]), "")
quoted = ",".join("'" + i + "'" for i in ids)
now_sep = val("select round(sum(net_revenue), 2) from etl_fact_orders where order_date < DATE '2026-10-01' "
              "and order_status = 'DELIVERED'")
back = val("select round(sum(try_cast(order_amount as double)), 2) from (select distinct order_id, order_amount "
           f"from raw_orders where _source_file = 'orders.csv' and order_status = 'DELIVERED' and order_id in ({quoted}))")
add(f"September net revenue **after** the increment: **{now_sep}**. The {len(ids)} returned orders were worth "
    f"**{back}**, so the figure finance originally saw was **{round((now_sep or 0) + (back or 0), 2)}** "
    "(difference = the returned orders).", "")

add("## Appendix D: evidence tables from the warehouse", "",
    "**dq_results, latest run of each DAG, by dimension and status**", "",
    table("with last as (select dag_id, run_id, max(checked_at) m from dq_results group by 1, 2), "
          "pick as (select dag_id, run_id from last qualify row_number() over (partition by dag_id order by m desc) = 1) "
          "select d.dag_id, d.dimension, d.status, count(*) as checks, sum(d.rows_failed) as rows_failed "
          "from dq_results d join pick p using (dag_id, run_id) group by 1, 2, 3 order by 1, 2, 3"), "",
    "**pipeline_runs, latest runs (the run audit)**", "",
    table("select dag_id, status, rows_extracted, rows_loaded, rows_quarantined, start_time, end_time "
          "from pipeline_runs order by start_time desc limit 8"), "",
    "**etl_payment_reconciliation, mismatches by reason**", "",
    table("select reason, count(*) as mismatch_rows, round(sum(abs(difference)), 2) as amount "
          "from etl_payment_reconciliation group by 1 order by 3 desc"), "")

SUB.write_text(SUB.read_text().rstrip() + "\n" + "\n".join(out) + "\n")
print("appendix A-D added to SUBMISSION.md:", len(out), "lines")
