#!/usr/bin/env python3
"""
NammaMart — answer the six business questions from BOTH pipelines.

Runs each question's SQL against the ETL tables and the ELT marts, checks the
two answers agree, and writes the results as Markdown for SUBMISSION.md.
Also writes a dq_results scorecard sample (Part D).

    astro dev bash
    python include/reports/business_questions.py
    -> include/reports/business_questions.md

Read-only: it never changes the warehouse. Only masked customer data is shown.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

WAREHOUSE = Path("include/warehouse_anaz.duckdb")
OUT = Path("include/reports/business_questions.md")
SAMPLE_DAY = "2026-09-05"

Q = [
    {
        "title": "1. Daily net revenue by zone and by store (delivered orders, after discount)",
        "note": f"Full table has one row per day and store; shown here for {SAMPLE_DAY}, plus the September total by zone.",
        "etl": f"""SELECT zone, store_id, store_name, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
WHERE order_date = DATE '{SAMPLE_DAY}'
ORDER BY zone, store_id""",
        "elt": f"""SELECT zone, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis
WHERE order_date = DATE '{SAMPLE_DAY}' AND delivered_orders > 0
ORDER BY zone, store_id""",
        "extra_etl": """SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
WHERE order_date < DATE '2026-10-01'
GROUP BY zone ORDER BY net_revenue DESC""",
        "extra_elt": """SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
WHERE order_date < DATE '2026-10-01'
GROUP BY zone ORDER BY net_revenue DESC""",
        "extra_title": "September net revenue by zone",
    },
    {
        "title": "2. Revenue and gross margin by product category (margin = revenue − quantity × cost_price)",
        "note": "All loaded orders (September and the 1 October increment). Orders whose product has no valid cost are counted in revenue but left out of margin (`orders_without_cost`).",
        "etl": """SELECT category, delivered_orders, revenue, cost, gross_margin, margin_pct, orders_without_cost
FROM etl_category_margin ORDER BY revenue DESC""",
        "elt": """SELECT category, delivered_orders, revenue, cost, gross_margin, margin_pct, orders_without_cost
FROM mart_category_margin ORDER BY revenue DESC""",
    },
    {
        "title": "3. Average delivery time per store, and % of delivered orders within 15 minutes",
        "note": "",
        "etl": """SELECT s.zone, f.store_id, s.store_name,
       count(*) AS delivered_orders,
       round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE f.within_15_min) / count(*), 1) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY pct_within_15_min DESC, f.store_id""",
        "elt": """SELECT zone, store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla ORDER BY pct_within_15_min DESC, store_id""",
    },
    {
        "title": "4. Revenue by payment mode, and the total amount that doesn't reconcile with the gateway",
        "note": "Unreconciled = sum of the absolute differences in the reconciliation table (every reason).",
        "etl": """SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED'
GROUP BY payment_mode ORDER BY net_revenue DESC""",
        "elt": """SELECT payment_mode, delivered_orders, net_revenue
FROM mart_payment_mode_revenue WHERE delivered_orders > 0 ORDER BY net_revenue DESC""",
        "extra_etl": """SELECT reason, count(*) AS rows, round(sum(abs(difference)), 2) AS amount
FROM etl_payment_reconciliation GROUP BY ROLLUP (reason) ORDER BY reason NULLS LAST""",
        "extra_elt": """SELECT reason, count(*) AS rows, round(sum(abs(difference)), 2) AS amount
FROM mart_payment_reconciliation GROUP BY ROLLUP (reason) ORDER BY reason NULLS LAST""",
        "extra_title": "Unreconciled amount by reason (last row = total)",
    },
    {
        "title": "5. Top 10 customers by spend, with loyalty tier (no raw email or phone)",
        "note": "",
        "etl": """SELECT row_number() OVER (ORDER BY sum(f.net_revenue) DESC, f.customer_id) AS spend_rank,
       f.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       count(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
       round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
GROUP BY f.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier
ORDER BY spend_rank LIMIT 10""",
        "elt": """SELECT spend_rank, customer_id, customer_name_masked, email_masked, phone_masked,
       loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers ORDER BY spend_rank""",
    },
    {
        "title": "6. Rows rejected today, by file and by reason",
        "note": "Latest run of each DAG. A row that broke two rules is counted under both.",
        "etl": """SELECT CASE split_part(check_name, '.', 1)
            WHEN 'orders_increment' THEN 'orders_2026-10-01_increment.csv'
            ELSE split_part(check_name, '.', 1) || '.csv' END AS source_file,
       split_part(check_name, '.', 2) AS failed_check, rows_failed AS rows_rejected
FROM dq_results
WHERE dag_id = 'nammamart_etl_anaz' AND severity = 'Critical' AND rows_failed > 0
  AND check_name NOT LIKE 'observability.%'
  AND run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_anaz'
                ORDER BY start_time DESC LIMIT 1)
ORDER BY source_file, rows_rejected DESC, failed_check""",
        "elt": """SELECT source_file, failed_check, rows_rejected
FROM mart_rejected_summary ORDER BY source_file, rows_rejected DESC, failed_check""",
    },
]

SCORECARD = """SELECT dag_id, check_name, dimension, severity, rows_checked, rows_failed, status
FROM dq_results
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_anaz' ORDER BY start_time DESC LIMIT 1)
  AND dag_id = 'nammamart_etl_anaz' AND status <> 'PASS'
ORDER BY status, check_name"""

SCORECARD_TREND = """SELECT dag_id, count(DISTINCT run_id) AS runs,
       count(*) FILTER (WHERE status = 'PASS') AS pass,
       count(*) FILTER (WHERE status = 'WARN') AS warn,
       count(*) FILTER (WHERE status = 'FAIL') AS fail
FROM dq_results GROUP BY dag_id ORDER BY dag_id"""


def md(df: pd.DataFrame) -> str:
    if df.empty:
        return "_no rows_\n"
    cols = [str(c) for c in df.columns]
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.itertuples(index=False):
        cells = []
        for v in row:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                cells.append("")
            elif isinstance(v, float):
                cells.append(f"{v:,.2f}" if v != int(v) or abs(v) >= 1000 else f"{int(v):,}")
            else:
                cells.append(str(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def same(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    if a.shape != b.shape:
        return False
    a2, b2 = a.reset_index(drop=True).copy(), b.reset_index(drop=True).copy()
    b2.columns = a2.columns
    for c in a2.columns:
        x, y = a2[c], b2[c]
        if pd.api.types.is_numeric_dtype(x) and pd.api.types.is_numeric_dtype(y):
            if not ((x.astype(float) - y.astype(float)).abs().fillna(0) < 0.011).all():
                return False
        elif not (x.astype(str).fillna("") == y.astype(str).fillna("")).all():
            return False
    return True


def main() -> int:
    if not WAREHOUSE.exists():
        print(f"{WAREHOUSE} not found: run from the project root after both DAGs have run")
        return 1
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    out = ["## Business questions",
           "",
           "Generated by `include/reports/business_questions.py` from the warehouse after the final runs "
           "(September plus the 1 October increment). Each question is answered from the ETL tables and from "
           "the ELT marts; **Match** says whether the two answers are identical.",
           ""]
    summary = []
    for q in Q:
        etl_df, elt_df = con.execute(q["etl"]).df(), con.execute(q["elt"]).df()
        ok = same(etl_df, elt_df)
        parts = [ok]
        out += [f"### {q['title']}", ""]
        if q["note"]:
            out += [q["note"], ""]
        out += ["**ETL SQL**", "", "```sql", q["etl"], "```", "", "**ELT SQL**", "", "```sql", q["elt"], "```", "",
                "**Result**", "", md(elt_df)]
        if "extra_etl" in q:
            e1, e2 = con.execute(q["extra_etl"]).df(), con.execute(q["extra_elt"]).df()
            ok2 = same(e1, e2)
            parts.append(ok2)
            out += [f"**{q['extra_title']}**", "", "```sql", q["extra_elt"], "```", "", md(e2),
                    f"ETL vs ELT: **{'match' if ok2 else 'DIFFERENT'}**", ""]
        out += [f"ETL vs ELT: **{'match' if ok else 'DIFFERENT'}**", ""]
        summary.append((q["title"].split(" ", 1)[0].rstrip("."), all(parts)))
    out += ["### Summary", "", "| Question | ETL and ELT agree |", "|---|---|"]
    out += [f"| {n} | {'yes' if ok else '**no**'} |" for n, ok in summary]
    out += ["", "## `dq_results` scorecard sample (Part D)", "",
            "Checks that did not pass in the latest ETL run:", "", "```sql", SCORECARD, "```", "",
            md(con.execute(SCORECARD).df()), "Totals across every run recorded so far:", "",
            "```sql", SCORECARD_TREND, "```", "", md(con.execute(SCORECARD_TREND).df())]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(out), encoding="utf-8")
    print("\n".join(f"Q{n}: {'match' if ok else 'DIFFERENT'}" for n, ok in summary))
    print(f"Wrote {OUT}")
    return 0 if all(ok for _, ok in summary) else 2


if __name__ == "__main__":
    sys.exit(main())
