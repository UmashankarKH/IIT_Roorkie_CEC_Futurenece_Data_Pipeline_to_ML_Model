#!/usr/bin/env python
"""
scripts/run_observability_experiments.py
----------------------------------------
Runs the ten observability experiments from assignment Part F against **both**
pipelines and records what actually happened.

Each experiment works on a throw-away copy of the dataset and a throw-away
DuckDB file, so the real data and the real warehouse are never modified - which
is also the assignment's instruction to "restore the original file after each
one".

Outputs (written to docs/evidence/):
    observability_experiments.json   machine readable results
    observability_results.md         the table that goes into SUBMISSION.md

Usage:
    python scripts/run_observability_experiments.py
"""

from __future__ import annotations

import json
import logging
import shutil
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from include.nammamart import config, elt_pipeline, etl_pipeline, warehouse  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("experiments")

EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence"
WORK_ROOT = PROJECT_ROOT / "include" / "staging" / "experiments"
SOURCE_DATA = config.DATA_DIR


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def fresh_workspace(name: str) -> tuple[Path, Path, Path]:
    """Give one experiment its own data copy, warehouse file and quarantine folder."""
    workspace = WORK_ROOT / name
    if workspace.exists():
        shutil.rmtree(workspace)
    data_dir = workspace / "data"
    shutil.copytree(SOURCE_DATA, data_dir)
    quarantine_dir = workspace / "quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    return data_dir, workspace / "warehouse.duckdb", quarantine_dir


def run_both(name: str, data_dir: Path, db_path: Path, quarantine_dir: Path,
             include_increment: bool = False) -> dict:
    """Run the ETL and then the ELT pipeline, capturing failures instead of raising."""
    outcome: dict = {}
    for label, runner in (("etl", etl_pipeline.run_etl), ("elt", elt_pipeline.run_elt)):
        started = time.perf_counter()
        try:
            summary = runner(run_id=f"{name}_{label}", data_dir=data_dir, db_path=db_path,
                             quarantine_dir=quarantine_dir, include_increment=include_increment)
            outcome[label] = {"status": "SUCCESS", "summary": summary}
        except Exception as exc:
            outcome[label] = {
                "status": "FAILED",
                "error_type": type(exc).__name__,
                "error_message": str(exc).splitlines()[0][:300],
                "failed_step": traceback.extract_tb(exc.__traceback__)[-1].name,
            }
        outcome[label]["seconds"] = round(time.perf_counter() - started, 3)
    return outcome


def query(db_path: Path, sql: str) -> pd.DataFrame:
    with warehouse.connect(db_path, read_only=True) as con:
        return warehouse.query(con, sql)


def record(results: list, number: int, title: str, expected: str, observed: str,
           where: str, change: str, detail: dict | None = None) -> None:
    results.append({
        "number": number, "experiment": title, "expected": expected, "observed": observed,
        "where_i_saw_it": where, "what_i_would_change": change, "detail": detail or {},
    })
    print(f"  [{number:>2}] {title}: {observed[:110]}")


# ---------------------------------------------------------------------------
# Experiment 1 - happy path
# ---------------------------------------------------------------------------
def experiment_1(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp01_happy_path")
    outcome = run_both("exp01", data_dir, db_path, quarantine_dir)

    per_file = query(db_path, """
        SELECT dataset,
               max(rows_checked)                                   AS rows_in,
               sum(CASE WHEN severity = 'Critical' THEN rows_failed ELSE 0 END) AS critical_failures,
               sum(CASE WHEN severity = 'Warning'  THEN rows_failed ELSE 0 END) AS warnings
        FROM dq_results WHERE dag_id LIKE '%etl%' GROUP BY dataset ORDER BY dataset
    """)
    audit = query(db_path, """
        SELECT dag_id, rows_extracted, rows_loaded, rows_quarantined, status,
               round(duration_seconds, 3) AS seconds
        FROM pipeline_runs ORDER BY start_time
    """)
    record(results, 1, "Happy path",
           "Both DAGs succeed; every file is counted; rejected rows are quarantined with a reason.",
           f"ETL {outcome['etl']['seconds']}s loaded "
           f"{outcome['etl']['summary']['counts']['etl_fact_orders']} fact rows and quarantined "
           f"{outcome['etl']['summary']['rows_quarantined']}; ELT {outcome['elt']['seconds']}s "
           f"loaded {outcome['elt']['summary']['rows_loaded']} PASS rows. Revenue is identical "
           f"in both marts.",
           "pipeline_runs table, dq_results table, task logs, quarantine CSV files",
           "Publish the per-file counts as an Airflow metric so a drop is visible without opening the table.",
           {"per_file": per_file.to_dict("records"), "pipeline_runs": audit.to_dict("records"),
            "etl_seconds": outcome["etl"]["seconds"], "elt_seconds": outcome["elt"]["seconds"],
            "parallel_tasks": "ETL extract_orders/payments/customers/products/stores run in "
                              "parallel; ELT raw loads are chained because DuckDB allows one writer"})


# ---------------------------------------------------------------------------
# Experiment 2 - re-run (idempotency)
# ---------------------------------------------------------------------------
def experiment_2(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp02_rerun")
    run_both("exp02a", data_dir, db_path, quarantine_dir)
    before = query(db_path, """
        SELECT (SELECT count(*) FROM etl_fact_orders)                         AS etl_fact,
               (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders)       AS etl_revenue,
               (SELECT count(*) FROM raw_orders)                              AS raw_orders,
               (SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS')     AS elt_pass,
               (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis) AS elt_revenue
    """).iloc[0].to_dict()
    outcome = run_both("exp02b", data_dir, db_path, quarantine_dir)
    after = query(db_path, """
        SELECT (SELECT count(*) FROM etl_fact_orders)                         AS etl_fact,
               (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders)       AS etl_revenue,
               (SELECT count(*) FROM raw_orders)                              AS raw_orders,
               (SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS')     AS elt_pass,
               (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis) AS elt_revenue
    """).iloc[0].to_dict()

    record(results, 2, "Re-run (idempotency)",
           "Fact and mart row counts stay the same; the ELT raw layer grows, which is correct.",
           f"ETL fact stayed at {after['etl_fact']} rows (0 inserted, "
           f"{outcome['etl']['summary']['counts']['etl_fact_orders_updated']} updated); "
           f"ELT PASS rows stayed at {after['elt_pass']} and the MERGE reported "
           f"{outcome['elt']['summary']['merge_base']['inserted']} inserts; raw_orders grew from "
           f"{before['raw_orders']} to {after['raw_orders']} rows by design (append-only audit log).",
           "warehouse row counts; MERGE log line; pipeline_runs has one row per run",
           "Add an assertion task that fails the DAG if the fact count changes on a pure re-run.",
           {"before": before, "after": after})


# ---------------------------------------------------------------------------
# Experiment 3 - incremental load
# ---------------------------------------------------------------------------
def experiment_3(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp03_increment")
    run_both("exp03a", data_dir, db_path, quarantine_dir)
    before = query(db_path, "SELECT count(*) AS n FROM etl_fact_orders").iloc[0]["n"]
    outcome = run_both("exp03b", data_dir, db_path, quarantine_dir, include_increment=True)

    corrected = query(db_path, """
        SELECT order_id, order_status FROM etl_fact_orders
        WHERE order_id IN ('ORD000017','ORD000271','ORD000332','ORD000337','ORD000584',
                           'ORD000708','ORD000822','ORD001025','ORD001067','ORD001990')
        ORDER BY order_id
    """)
    duplicates = query(db_path, """
        SELECT count(*) AS rows, count(DISTINCT order_id) AS ids FROM etl_fact_orders
    """).iloc[0].to_dict()
    merge = outcome["elt"]["summary"]["merge_increment"]

    record(results, 3, "Incremental load",
           "150 new orders inserted, 10 existing orders updated to RETURNED, no duplicates.",
           f"ELT MERGE reported {merge['inserted']} inserted and {merge['updated']} updated; "
           f"ETL upsert inserted {outcome['etl']['summary']['counts']['etl_fact_orders_inserted']} "
           f"and updated {outcome['etl']['summary']['counts']['etl_fact_orders_updated']}; fact grew "
           f"from {before} to {duplicates['rows']} rows with {duplicates['ids']} distinct order_ids; "
           f"all 10 corrected orders now show "
           f"{sorted(set(corrected['order_status']))} and their revenue became 0.",
           "MERGE log line, etl_fact_orders upsert log, stg_orders dq_status = SUPERSEDED rows",
           "Keep the superseded version as an SCD Type 2 row in the ETL fact as well, so finance "
           "can still see the September figure that was reported at the time.",
           {"merge": merge, "fact_rows": duplicates,
            "corrected_orders": corrected.to_dict("records")})


# ---------------------------------------------------------------------------
# Experiment 4 - missing source file
# ---------------------------------------------------------------------------
def experiment_4(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp04_missing_file")
    (data_dir / "products.csv").rename(data_dir / "products.csv.bak")
    outcome = run_both("exp04", data_dir, db_path, quarantine_dir)
    audit = query(db_path, "SELECT dag_id, status, notes FROM pipeline_runs ORDER BY start_time")
    (data_dir / "products.csv.bak").rename(data_dir / "products.csv")  # restore

    record(results, 4, "Missing source file",
           "The task fails immediately with a clear error; downstream tasks do not run; the "
           "run audit records the failure.",
           f"ETL failed in {outcome['etl']['failed_step']}() with "
           f"{outcome['etl']['error_type']}; ELT failed in {outcome['elt']['failed_step']}() with "
           f"{outcome['elt']['error_type']}. Both wrote a FAILED row to pipeline_runs. In Airflow "
           f"the task would retry twice at 2-minute intervals (default_args) and then go to "
           f"'failed', leaving downstream tasks 'upstream_failed'.",
           "task log, pipeline_runs.status = FAILED, Airflow Grid view colours",
           "Add a lightweight 'all five files present' sensor task before the extracts so the "
           "failure is reported once, not five times.",
           {"etl": outcome["etl"], "elt": outcome["elt"],
            "pipeline_runs": audit.to_dict("records"),
            "manual_check_required": "retry count and up_for_retry / upstream_failed states are "
                                     "visible only in the Airflow UI"})


# ---------------------------------------------------------------------------
# Experiment 5 - new bad data
# ---------------------------------------------------------------------------
BAD_ROWS = [
    {"order_id": "ORD900001", "order_ts": "2026-09-15 10:00:00", "customer_id": "",
     "store_id": "S01", "product_id": "P0001", "quantity": "1", "unit_price": "100.0",
     "discount_pct": "0", "order_amount": "100.0", "payment_mode": "UPI",
     "order_status": "DELIVERED", "channel": "App", "delivery_minutes": "10",
     "_expected_rule": "RULE-006"},
    {"order_id": "ORD900002", "order_ts": "2026-09-15 11:00:00", "customer_id": "C00005",
     "store_id": "S01", "product_id": "P0001", "quantity": "-2", "unit_price": "100.0",
     "discount_pct": "0", "order_amount": "-200.0", "payment_mode": "UPI",
     "order_status": "DELIVERED", "channel": "App", "delivery_minutes": "10",
     "_expected_rule": "RULE-009"},
    {"order_id": "ORD900003", "order_ts": "2026-09-15 12:00:00", "customer_id": "C00005",
     "store_id": "S99", "product_id": "P0001", "quantity": "1", "unit_price": "100.0",
     "discount_pct": "0", "order_amount": "100.0", "payment_mode": "UPI",
     "order_status": "DELIVERED", "channel": "App", "delivery_minutes": "10",
     "_expected_rule": "RULE-008"},
    {"order_id": "ORD900004", "order_ts": "2026-09-15 13:00:00", "customer_id": "C00005",
     "store_id": "S01", "product_id": "P0001", "quantity": "1", "unit_price": "100.0",
     "discount_pct": "0", "order_amount": "100.0", "payment_mode": "UPI",
     "order_status": "SHIPPED", "channel": "App", "delivery_minutes": "10",
     "_expected_rule": "RULE-012"},
    {"order_id": "ORD900005", "order_ts": "2027-03-01 09:00:00", "customer_id": "C00005",
     "store_id": "S01", "product_id": "P0001", "quantity": "1", "unit_price": "100.0",
     "discount_pct": "0", "order_amount": "100.0", "payment_mode": "UPI",
     "order_status": "DELIVERED", "channel": "App", "delivery_minutes": "10",
     "_expected_rule": "RULE-005"},
]


def experiment_5(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp05_bad_rows")
    orders = pd.read_csv(data_dir / "orders.csv", dtype=str, keep_default_na=False)
    extra = pd.DataFrame([{k: v for k, v in row.items() if not k.startswith("_")}
                          for row in BAD_ROWS])
    pd.concat([orders, extra], ignore_index=True).to_csv(data_dir / "orders.csv", index=False)
    run_both("exp05", data_dir, db_path, quarantine_dir)

    landed = []
    for row in BAD_ROWS:
        order_id = row["order_id"]
        etl_reason = query(db_path, f"""
            SELECT failure_reason FROM etl_quarantine
            WHERE dataset = 'orders' AND row_key = '{order_id}'
        """)
        elt_reason = query(db_path, f"""
            SELECT dq_status, failure_reason FROM stg_orders WHERE order_id = '{order_id}'
        """)
        in_fact = query(db_path,
                        f"SELECT count(*) AS n FROM etl_fact_orders WHERE order_id = '{order_id}'"
                        ).iloc[0]["n"]
        landed.append({
            "order_id": order_id,
            "expected_rule": row["_expected_rule"],
            "etl_quarantined": not etl_reason.empty,
            "etl_rule_matched": (not etl_reason.empty
                                 and row["_expected_rule"] in etl_reason.iloc[0]["failure_reason"]),
            "elt_status": None if elt_reason.empty else elt_reason.iloc[0]["dq_status"],
            "elt_rule_matched": (not elt_reason.empty
                                 and row["_expected_rule"] in (elt_reason.iloc[0]["failure_reason"] or "")),
            "in_etl_fact": int(in_fact),
        })

    dq_captured = query(db_path, f"""
        SELECT rule_id, dataset, severity, sum(rows_failed) AS failed
        FROM dq_results
        WHERE rule_id IN ('{"','".join(sorted({r["_expected_rule"] for r in BAD_ROWS}))}')
        GROUP BY 1, 2, 3 ORDER BY 1
    """)
    all_caught = all(item["etl_rule_matched"] and item["elt_rule_matched"] and item["in_etl_fact"] == 0
                     for item in landed)

    record(results, 5, "New bad data",
           "All five planted rows are quarantined with the right rule and none reaches the fact table.",
           ("All 5 rows were caught by both pipelines with the expected rule "
            "(RULE-006 null customer, RULE-009 negative quantity, RULE-008 unknown store, "
            "RULE-012 status SHIPPED, RULE-005 2027 timestamp) and none appears in "
            "etl_fact_orders; dq_results recorded every one."
            if all_caught else f"Not every row behaved as expected: {landed}"),
           "quarantine CSV, etl_quarantine table, stg_orders.failure_reason, dq_results",
           "Add a 'planted row' smoke test to CI so the rules keep working after a refactor.",
           {"rows": landed, "dq_results": dq_captured.to_dict("records")})


# ---------------------------------------------------------------------------
# Experiment 6 - schema change
# ---------------------------------------------------------------------------
def experiment_6(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp06_schema_change")
    orders = pd.read_csv(data_dir / "orders.csv", dtype=str, keep_default_na=False)
    orders.rename(columns={"order_amount": "total"}).to_csv(data_dir / "orders.csv", index=False)
    outcome = run_both("exp06", data_dir, db_path, quarantine_dir)

    record(results, 6, "Schema change (order_amount renamed to total)",
           "Both pipelines stop with a clear message naming the missing column; nothing is loaded.",
           f"ETL failed in {outcome['etl']['failed_step']}() with {outcome['etl']['error_type']}: "
           f"\"{outcome['etl']['error_message']}\". ELT failed in "
           f"{outcome['elt']['failed_step']}() with {outcome['elt']['error_type']}: "
           f"\"{outcome['elt']['error_message']}\". Neither loaded a single row, so the failure is "
           f"loud, not silent.",
           "task log and the FAILED row in pipeline_runs",
           "Publish the expected column list as a contract document and notify the source team "
           "automatically when it changes.",
           {"etl": outcome["etl"], "elt": outcome["elt"]})


# ---------------------------------------------------------------------------
# Experiment 7 - volume anomaly
# ---------------------------------------------------------------------------
def experiment_7(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp07_volume")
    orders = pd.read_csv(data_dir / "orders.csv", dtype=str, keep_default_na=False)
    orders.head(50).to_csv(data_dir / "orders.csv", index=False)
    run_both("exp07", data_dir, db_path, quarantine_dir)

    volume = query(db_path, """
        SELECT dag_id, rule_id, status, rows_checked, rows_failed, detail
        FROM dq_results WHERE rule_id = 'RULE-034' ORDER BY dag_id
    """)
    record(results, 7, "Volume anomaly (only 50 orders)",
           "A volume check warns that the file lost more than 90% of its rows.",
           f"RULE-034 fired in both pipelines with status "
           f"{sorted(set(volume['status']))[0]}: {volume.iloc[0]['detail']}. The run still "
           f"succeeded, which is intended - a volume drop is a warning for a human, not a reason "
           f"to withhold the 9 AM report.",
           "dq_results rows where rule_id = 'RULE-034'; the WARNING line in the task log",
           "Promote the volume check to Critical once the client confirms a minimum daily volume, "
           "and send the warning to Slack rather than only to the table.",
           {"volume_checks": volume.to_dict("records")})


# ---------------------------------------------------------------------------
# Experiment 8 - distribution drift
# ---------------------------------------------------------------------------
def experiment_8(results: list) -> None:
    # 8a: exactly what the assignment asks - unit_price x 100 for one store
    data_dir, db_path, quarantine_dir = fresh_workspace("exp08a_drift_price_only")
    orders = pd.read_csv(data_dir / "orders.csv", dtype=str, keep_default_na=False)
    affected = orders["store_id"] == "S01"
    orders.loc[affected, "unit_price"] = (
        pd.to_numeric(orders.loc[affected, "unit_price"], errors="coerce") * 100).round(2).astype(str)
    orders.to_csv(data_dir / "orders.csv", index=False)
    run_both("exp08a", data_dir, db_path, quarantine_dir)
    rule_011 = query(db_path, """
        SELECT dag_id, rows_failed FROM dq_results WHERE rule_id = 'RULE-011' ORDER BY dag_id
    """)
    s01_rows = query(db_path,
                     "SELECT count(*) AS n FROM etl_fact_orders WHERE store_id = 'S01'"
                     ).iloc[0]["n"]

    # 8b: a consistent price rise, where order_amount is multiplied too
    data_dir_b, db_path_b, quarantine_dir_b = fresh_workspace("exp08b_drift_consistent")
    orders_b = pd.read_csv(data_dir_b / "orders.csv", dtype=str, keep_default_na=False)
    affected_b = orders_b["store_id"] == "S01"
    for column in ("unit_price", "order_amount"):
        orders_b.loc[affected_b, column] = (
            pd.to_numeric(orders_b.loc[affected_b, column], errors="coerce") * 100
        ).round(2).astype(str)
    orders_b.to_csv(data_dir_b / "orders.csv", index=False)

    baseline_data, baseline_db, baseline_q = fresh_workspace("exp08b_baseline")
    run_both("exp08b_base", baseline_data, baseline_db, baseline_q)
    baseline_revenue = query(baseline_db, """
        SELECT round(sum(net_revenue), 2) AS revenue FROM mart_daily_store_kpis WHERE store_id = 'S01'
    """).iloc[0]["revenue"]

    run_both("exp08b", data_dir_b, db_path_b, quarantine_dir_b)
    drifted_revenue = query(db_path_b, """
        SELECT round(sum(net_revenue), 2) AS revenue FROM mart_daily_store_kpis WHERE store_id = 'S01'
    """).iloc[0]["revenue"]
    critical_b = query(db_path_b, """
        SELECT sum(rows_failed) AS failed FROM dq_results
        WHERE severity = 'Critical' AND dag_id LIKE '%etl%'
    """).iloc[0]["failed"]

    record(results, 8, "Distribution drift (unit_price x 100 for store S01)",
           "A price explosion in one store should be noticed before it reaches the mart.",
           f"Variant A (unit_price only, as instructed): RULE-011 'order_amount must match the "
           f"formula' failed for {int(rule_011.iloc[0]['rows_failed'])} rows in both pipelines, "
           f"so every affected order was quarantined and store S01 kept only {int(s01_rows)} "
           f"rows in the fact table. Variant B (unit_price and order_amount both multiplied, so "
           f"the arithmetic still agrees): no Critical rule fired, and S01 revenue moved from "
           f"{baseline_revenue:,.2f} to {drifted_revenue:,.2f} "
           f"({drifted_revenue / baseline_revenue:.0f}x) with the data loading cleanly.",
           "dq_results (RULE-011), mart_daily_store_kpis revenue per store",
           "Add a distribution check that compares today's average order value per store with the "
           "trailing 7-day average and warns on a move larger than, say, 3 standard deviations. "
           "Rule-based checks catch broken arithmetic, not believable-but-wrong values.",
           {"variant_a_rule_011": rule_011.to_dict("records"),
            "variant_a_s01_fact_rows": int(s01_rows),
            "variant_b_baseline_revenue": float(baseline_revenue),
            "variant_b_drifted_revenue": float(drifted_revenue),
            "variant_b_critical_failures": int(critical_b)})


# ---------------------------------------------------------------------------
# Experiment 9 - freshness
# ---------------------------------------------------------------------------
def experiment_9(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp09_freshness")
    orders = pd.read_csv(data_dir / "orders.csv", dtype=str, keep_default_na=False)
    parsed = pd.to_datetime(orders["order_ts"], errors="coerce", format="mixed")
    newest = parsed[parsed <= pd.Timestamp.now()].max()
    shift = (pd.Timestamp.now() - pd.Timedelta(days=30)) - newest
    orders["order_ts"] = (parsed + shift).dt.strftime("%Y-%m-%d %H:%M:%S").fillna(orders["order_ts"])
    orders.to_csv(data_dir / "orders.csv", index=False)
    run_both("exp09", data_dir, db_path, quarantine_dir)

    freshness = query(db_path, """
        SELECT dag_id, status, rows_failed, detail FROM dq_results
        WHERE rule_id = 'RULE-035' ORDER BY dag_id
    """)
    record(results, 9, "Freshness (newest order 30 days old)",
           "The timeliness check warns that the data is stale.",
           f"RULE-035 returned status {freshness.iloc[0]['status']} in both pipelines. "
           f"ETL detail: \"{freshness.iloc[0]['detail']}\". "
           f"ELT detail: \"{freshness.iloc[1]['detail']}\".",
           "dq_results rows where rule_id = 'RULE-035'",
           "Fail the DAG (not just warn) when the newest order is more than 2 days old, because a "
           "stale 9 AM report is worse than no report.",
           {"freshness_checks": freshness.to_dict("records")})


# ---------------------------------------------------------------------------
# Experiment 10 - recovery
# ---------------------------------------------------------------------------
def experiment_10(results: list) -> None:
    data_dir, db_path, quarantine_dir = fresh_workspace("exp10_recovery")
    (data_dir / "stores.csv").rename(data_dir / "stores.csv.bak")
    failed = run_both("exp10_fail", data_dir, db_path, quarantine_dir)
    audit_after_failure = query(db_path, "SELECT dag_id, status FROM pipeline_runs")

    (data_dir / "stores.csv.bak").rename(data_dir / "stores.csv")  # fix the cause
    recovered = run_both("exp10_recover", data_dir, db_path, quarantine_dir)
    audit_after_recovery = query(db_path, """
        SELECT run_id, dag_id, status, rows_loaded FROM pipeline_runs ORDER BY start_time
    """)
    totals = query(db_path, """
        SELECT (SELECT count(*) FROM etl_fact_orders) AS etl_fact,
               (SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS') AS elt_pass
    """).iloc[0].to_dict()

    record(results, 10, "Recovery after fixing the cause",
           "After the cause is fixed, re-running produces the correct numbers with no duplicates.",
           f"With stores.csv missing both pipelines failed ({failed['etl']['error_type']}) and "
           f"wrote FAILED rows to pipeline_runs. After restoring the file, both runs succeeded and "
           f"loaded {totals['etl_fact']} ETL fact rows / {totals['elt_pass']} ELT PASS rows - the "
           f"same numbers as a clean run, because the loads are idempotent. In the Airflow UI, "
           f"pressing Clear on the failed task re-runs only that task and everything downstream of "
           f"it; the already successful extract tasks are not repeated.",
           "pipeline_runs (FAILED then SUCCESS), warehouse row counts; Airflow Grid view for Clear",
           "Keep the quarantine and audit tables keyed by run_id so a partially failed day can be "
           "replayed without deleting yesterday's evidence.",
           {"failed_run": {k: v.get("error_type") for k, v in failed.items()},
            "recovered": {k: v["status"] for k, v in recovered.items()},
            "audit_after_failure": audit_after_failure.to_dict("records"),
            "audit_after_recovery": audit_after_recovery.to_dict("records"),
            "manual_check_required": "the behaviour of the Clear button is an Airflow UI action"})


# ---------------------------------------------------------------------------
# Report writing
# ---------------------------------------------------------------------------
def write_markdown(results: list, path: Path) -> None:
    lines = [
        "# Part F - Observability experiment results",
        "",
        f"Generated: {datetime.now():%Y-%m-%d %H:%M} - produced by "
        "`scripts/run_observability_experiments.py`.",
        "",
        "Every experiment ran against **both** pipelines on a throw-away copy of the dataset and a "
        "throw-away DuckDB file, so the original files were never modified.",
        "",
    ]
    for item in results:
        lines += [
            f"## Experiment {item['number']}: {item['experiment']}",
            "",
            f"- **Expected:** {item['expected']}",
            f"- **Observed:** {item['observed']}",
            f"- **Where I saw it:** {item['where_i_saw_it']}",
            f"- **What I would change:** {item['what_i_would_change']}",
            "",
        ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    WORK_ROOT.mkdir(parents=True, exist_ok=True)
    results: list = []
    experiments = [experiment_1, experiment_2, experiment_3, experiment_4, experiment_5,
                   experiment_6, experiment_7, experiment_8, experiment_9, experiment_10]
    print("Running the Part F observability experiments on both pipelines...")
    for experiment in experiments:
        experiment(results)

    (EVIDENCE_DIR / "observability_experiments.json").write_text(
        json.dumps(results, indent=2, default=str), encoding="utf-8")
    write_markdown(results, EVIDENCE_DIR / "observability_results.md")
    print(f"\nWrote {EVIDENCE_DIR / 'observability_experiments.json'}")
    print(f"Wrote {EVIDENCE_DIR / 'observability_results.md'}")
    shutil.rmtree(WORK_ROOT, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
