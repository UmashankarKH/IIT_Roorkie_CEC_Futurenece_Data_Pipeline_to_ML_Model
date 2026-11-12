"""
NammaMart — run-level observability checks (Part F, added after Experiments 7 and 8)
====================================================================================

Row-level rules (include/dq_rules.py) judge each row on its own. They cannot see
that half the rows are missing (Experiment 7) or that one store is 100x too big
while every row is internally consistent (Experiment 8b). These checks look at
the run as a whole:

  observability.orders_volume        Volume        orders loaded vs median of the last successful runs
  observability.stores_without_orders Distribution active store with no orders in this run
  observability.store_revenue_outlier Distribution store revenue far above the typical store
  observability.gross_margin_range    Distribution overall margin outside what a grocer can earn

Results go to two places:
  dq_results              one scorecard row per check (same table as the row-level checks)
  observability_metrics   the numbers behind each check (value, baseline, ratio, detail)

A Critical result makes the task fail, so the run turns red and pipeline_runs
records status 'anomaly'. Warnings are logged and recorded; the run stays green.
Used by both DAGs, so ETL and ELT apply identical thresholds.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

# ---- thresholds (owner: data team; agree changes with finance / ops, see Part E1) ----
VOLUME_HISTORY_RUNS = 7          # baseline = median of the last N successful runs
VOLUME_MIN_HISTORY = 3           # fewer runs than this: not enough history to judge
VOLUME_WARN = (0.5, 2.0)         # ratio to baseline outside this range: Warning
VOLUME_CRITICAL = (0.1, 10.0)    # outside this range: Critical
STORE_LOW_WARN = 0.2             # store revenue below 20% of the median store: Warning
STORE_HIGH_CRITICAL = 5.0        # store revenue above 5x the median store: Critical
MARGIN_RANGE = (5.0, 60.0)       # % gross margin a grocery business can plausibly earn

METRICS_DDL = """
CREATE TABLE IF NOT EXISTS observability_metrics (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, metric VARCHAR,
    value DOUBLE, baseline DOUBLE, ratio DOUBLE, status VARCHAR, detail VARCHAR,
    checked_at TIMESTAMP
)"""
DQ_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP
)"""


class AnomalyDetected(Exception):
    """Raised when a Critical observability check fails."""


def _volume(con, dag_id: str, rows_loaded: int) -> dict:
    hist = con.execute(
        """SELECT rows_loaded FROM pipeline_runs
           WHERE dag_id = ? AND status = 'success' AND rows_loaded > 0
           ORDER BY start_time DESC LIMIT ?""", [dag_id, VOLUME_HISTORY_RUNS]).fetchall() \
        if _exists(con, "pipeline_runs") else []
    hist = [h[0] for h in hist]
    if len(hist) < VOLUME_MIN_HISTORY:
        return dict(check="observability.orders_volume", dimension="Volume", metric="orders_loaded",
                    value=rows_loaded, baseline=None, ratio=None, status="PASS",
                    detail=f"only {len(hist)} earlier successful runs; need {VOLUME_MIN_HISTORY} to judge")
    base = float(pd.Series(hist).median())
    ratio = rows_loaded / base if base else None
    status = "PASS"
    if ratio is None or not (VOLUME_CRITICAL[0] <= ratio <= VOLUME_CRITICAL[1]):
        status = "FAIL"
    elif not (VOLUME_WARN[0] <= ratio <= VOLUME_WARN[1]):
        status = "WARN"
    return dict(check="observability.orders_volume", dimension="Volume", metric="orders_loaded",
                value=rows_loaded, baseline=base, ratio=ratio, status=status,
                detail=f"{rows_loaded:,} orders vs median {base:,.0f} of last {len(hist)} successful runs "
                       f"({ratio:.0%})")


def _store_and_margin(con, orders_sql: str, stores_sql: str) -> list[dict]:
    """orders_sql: this run's reportable orders with store_id, order_status, net_revenue,
    gross_margin. stores_sql: the active stores (store_id)."""
    per_store = con.execute(f"""
        SELECT s.store_id,
               coalesce(sum(o.net_revenue) FILTER (WHERE o.order_status = 'DELIVERED'), 0) AS revenue,
               count(o.store_id) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered
        FROM ({stores_sql}) s LEFT JOIN ({orders_sql}) o ON o.store_id = s.store_id
        GROUP BY s.store_id ORDER BY s.store_id""").df()
    out = []
    if per_store.empty:
        return out
    med = float(per_store["revenue"].median())
    per_store["ratio"] = per_store["revenue"] / med if med else 0.0

    empty = per_store[per_store["delivered"] == 0]["store_id"].tolist()
    low = per_store[(per_store["delivered"] > 0) & (per_store["ratio"] < STORE_LOW_WARN)]["store_id"].tolist()
    out.append(dict(check="observability.stores_without_orders", dimension="Distribution",
                    metric="stores_without_delivered_orders", value=len(empty) + len(low), baseline=len(per_store),
                    ratio=None, status="WARN" if (empty or low) else "PASS",
                    detail=(f"no delivered orders: {empty or 'none'}; below {STORE_LOW_WARN:.0%} of median store: "
                            f"{low or 'none'} (median store revenue ₹{med:,.2f})")))

    high = per_store[per_store["ratio"] > STORE_HIGH_CRITICAL]
    out.append(dict(check="observability.store_revenue_outlier", dimension="Distribution",
                    metric="max_store_revenue_vs_median", value=float(per_store["revenue"].max()), baseline=med,
                    ratio=float(per_store["ratio"].max()) if med else None,
                    status="FAIL" if len(high) else "PASS",
                    detail=("stores above " + f"{STORE_HIGH_CRITICAL:g}x median: "
                            + (", ".join(f"{r.store_id} ₹{r.revenue:,.0f} ({r.ratio:.1f}x)" for r in high.itertuples())
                               if len(high) else "none"))))

    m = con.execute(f"""
        SELECT sum(gross_margin) AS margin,
               sum(net_revenue) FILTER (WHERE gross_margin IS NOT NULL) AS revenue_with_cost
        FROM ({orders_sql}) o WHERE order_status = 'DELIVERED'""").fetchone()
    pct = 100 * m[0] / m[1] if m and m[1] else None
    ok = pct is not None and MARGIN_RANGE[0] <= pct <= MARGIN_RANGE[1]
    out.append(dict(check="observability.gross_margin_range", dimension="Distribution", metric="gross_margin_pct",
                    value=pct, baseline=None, ratio=None,
                    status="PASS" if ok or pct is None else "FAIL",
                    detail=(f"gross margin {pct:.1f}% (allowed {MARGIN_RANGE[0]:g}–{MARGIN_RANGE[1]:g}%)"
                            if pct is not None else "no delivered orders with known cost")))
    return out


def _exists(con, table: str) -> bool:
    return con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]).fetchone()[0] > 0


def run_checks(con, run_id: str, dag_id: str, rows_loaded: int, orders_sql: str, stores_sql: str) -> dict:
    """Run all checks, record them, print a summary. Raises AnomalyDetected on any Critical failure."""
    results = [_volume(con, dag_id, rows_loaded)] + _store_and_margin(con, orders_sql, stores_sql)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    severity = {"observability.orders_volume": "Critical", "observability.stores_without_orders": "Warning",
                "observability.store_revenue_outlier": "Critical", "observability.gross_margin_range": "Critical"}

    con.execute(METRICS_DDL)
    con.execute(DQ_DDL)
    con.execute("DELETE FROM observability_metrics WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND check_name LIKE 'observability.%'",
                [run_id, dag_id])
    for r in results:
        con.execute("INSERT INTO observability_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, dag_id, r["check"], r["metric"], r["value"], r["baseline"], r["ratio"],
                     r["status"], r["detail"], now])
        con.execute("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, dag_id, r["check"], r["dimension"], severity[r["check"]], 1,
                     0 if r["status"] == "PASS" else 1, r["status"], now])

    table = pd.DataFrame([{k: r[k] for k in ("check", "status", "detail")} for r in results])
    with pd.option_context("display.width", 250, "display.max_colwidth", 140):
        print("\n=== Run-level observability checks ===")
        print(table.to_string(index=False))
    failed = [r for r in results if r["status"] == "FAIL"]
    warned = [r for r in results if r["status"] == "WARN"]
    if warned:
        print("\nWARNING: " + " | ".join(r["detail"] for r in warned))
    if failed:
        raise AnomalyDetected("Critical anomaly - today's numbers are suspect: "
                              + " | ".join(r["detail"] for r in failed))
    print("\nAll run-level checks passed" if not warned else "\nNo critical anomaly (warnings above)")
    return {"checks": len(results), "warnings": len(warned), "critical": 0}
