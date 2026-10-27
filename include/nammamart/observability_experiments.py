"""Part F: run the ten observability experiments and print what actually happened.

    python -m nammamart.observability_experiments          # all of them
    python -m nammamart.observability_experiments 5 7      # just those

Everything runs in a sandbox: the CSVs are copied to include/tmp/<owner>/experiments/
and the warehouse is a throwaway file, so include/data/nammamart/ is never edited and
the real warehouse keeps the numbers quoted in SUBMISSION.md. The original file is
restored after each experiment.

Experiment 10 (Clear a failed task and watch what re-runs) is an Airflow UI action,
so it is not run here - it is recorded in SUBMISSION.md from the UI.
"""

from __future__ import annotations

import logging
import shutil
import sys
import time
import traceback
from pathlib import Path

from . import config as cfg

log = logging.getLogger(__name__)

SANDBOX = cfg.INCLUDE_DIR / "tmp" / cfg.OWNER / "experiments"
PRISTINE = SANDBOX / "pristine"
DATA = SANDBOX / "data"


def _point_config_at_sandbox() -> None:
    """Redirect the package at the sandbox before anything else imports from it."""
    SANDBOX.mkdir(parents=True, exist_ok=True)
    if PRISTINE.exists():
        shutil.rmtree(PRISTINE)
    shutil.copytree(cfg.DATA_DIR, PRISTINE)
    if DATA.exists():
        shutil.rmtree(DATA)
    shutil.copytree(PRISTINE, DATA)

    cfg.DATA_DIR = DATA
    cfg.WAREHOUSE = SANDBOX / "warehouse_experiments.duckdb"
    cfg.QUARANTINE_DIR = SANDBOX / "quarantine"


_point_config_at_sandbox()

# Imported after the redirect so every default argument picks up the sandbox paths.
from . import elt, etl, run_pipelines, warehouse as wh      # noqa: E402

RESULTS: list[dict] = []


# ------------------------------------------------------------------- helpers

def restore(*names: str) -> None:
    for name in names or tuple(p.name for p in PRISTINE.iterdir()):
        shutil.copy2(PRISTINE / name, DATA / name)
    for leftover in DATA.iterdir():
        if leftover.name not in {p.name for p in PRISTINE.iterdir()}:
            leftover.unlink()


def run(pipeline: str, run_id: str, increment: bool = False,
        reset: bool = False) -> dict:
    """Run one pipeline and never raise: a failure is the observation."""
    if reset:
        run_pipelines.reset()
    started = time.time()
    try:
        result = (run_pipelines.run_etl(run_id, increment) if pipeline == "etl"
                  else run_pipelines.run_elt(run_id, increment))
        return {"ok": True, "seconds": round(time.time() - started, 1), "result": result}
    except Exception as exc:                                  # noqa: BLE001
        return {"ok": False, "seconds": round(time.time() - started, 1),
                "error": f"{type(exc).__name__}: {exc}",
                "where": traceback.extract_tb(exc.__traceback__)[-1].name}


def query(sql: str, params: list | None = None):
    with wh.connect() as con:
        try:
            return con.execute(sql, params or []).fetchall()
        except Exception as exc:                              # noqa: BLE001
            return [(f"query failed: {exc}",)]


def scalar(sql: str, default=None):
    rows = query(sql)
    return rows[0][0] if rows and not str(rows[0][0]).startswith("query failed") else default


def record(number: int, title: str, expected: str, observed: str,
           where: str, change: str) -> None:
    RESULTS.append({"number": number, "title": title, "expected": expected,
                    "observed": observed, "where": where, "change": change})
    print(f"\n### {number}. {title}\n")
    print(f"- **Expected:** {expected}")
    print(f"- **Observed:** {observed}")
    print(f"- **Where I saw it:** {where}")
    print(f"- **What I would change:** {change}")


def dq_counts(dag_like: str, run_id: str) -> tuple:
    return query("""
        SELECT status, count(*) FROM dq_results
        WHERE dag_id LIKE ? AND run_id = ? GROUP BY status ORDER BY status
    """, [dag_like, run_id])


# --------------------------------------------------------------- experiments

def experiment_1() -> None:
    etl_run = run("etl", "exp1", reset=True)
    elt_run = run("elt", "exp1")
    fact = scalar("SELECT count(*) FROM etl_fact_orders")
    mart = scalar("SELECT count(*) FROM mart_fact_orders")
    quarantined = scalar("SELECT count(*) FROM elt_quarantine WHERE run_id = 'exp1'")
    record(
        1, "Happy path",
        "both DAGs green, 2,310 clean September orders in each fact table, "
        "127 rows quarantined with reasons",
        f"ETL finished in {etl_run['seconds']}s with {fact} fact rows; "
        f"ELT finished in {elt_run['seconds']}s with {mart} mart rows; "
        f"{quarantined} rows quarantined on the ELT side. "
        f"dq_results ETL {dq_counts('nammamart_etl%', 'exp1')}, "
        f"ELT {dq_counts('nammamart_elt%', 'exp1')}. "
        "The five extract tasks have no dependencies on each other, so in Airflow "
        "they run in parallel; here they run one after another.",
        "task duration in the Airflow Grid view; the row counts in the load task log; "
        "dq_results and elt_quarantine",
        "nothing - but the ETL log should print the per-file passed/failed split as a "
        "small table instead of one long dict, it is hard to read at 9 AM",
    )


def experiment_2() -> None:
    before_fact = scalar("SELECT count(*) FROM etl_fact_orders")
    before_mart = scalar("SELECT count(*) FROM mart_fact_orders")
    before_raw = scalar("SELECT count(*) FROM raw_orders")
    run("etl", "exp2")
    run("elt", "exp2")
    after_fact = scalar("SELECT count(*) FROM etl_fact_orders")
    after_mart = scalar("SELECT count(*) FROM mart_fact_orders")
    after_raw = scalar("SELECT count(*) FROM raw_orders")
    record(
        2, "Re-run (idempotency)",
        "fact row counts identical, revenue unchanged, no duplicated orders",
        f"ETL fact {before_fact} -> {after_fact}, ELT mart {before_mart} -> {after_mart}, "
        f"raw_orders {before_raw} -> {after_raw}. Both loads are CREATE OR REPLACE, and "
        "the raw loader deletes the rows of the file it is re-loading before inserting "
        "them again, so raw does not grow either. Raw growing would also have been "
        "defensible (append-only audit), but then every order would be reported as a "
        "duplicate, which is worse.",
        "the row counts in both load task logs; pipeline_runs has two rows with the "
        "same rows_loaded",
        "nothing",
    )


def experiment_3() -> None:
    before = scalar("SELECT count(*) FROM etl_fact_orders")
    returned_before = scalar("""
        SELECT count(*) FROM etl_fact_orders
        WHERE order_status = 'RETURNED' AND order_date < DATE '2026-10-01'
    """)
    etl_run = run("etl", "exp3", increment=True)
    elt_run = run("elt", "exp3", increment=True)
    after = scalar("SELECT count(*) FROM etl_fact_orders")
    mart_after = scalar("SELECT count(*) FROM mart_fact_orders")
    dupes = scalar("""
        SELECT count(*) FROM (SELECT order_id FROM mart_fact_orders
                              GROUP BY order_id HAVING count(*) > 1)
    """)
    returned_after = scalar("""
        SELECT count(*) FROM etl_fact_orders
        WHERE order_status = 'RETURNED' AND order_date < DATE '2026-10-01'
    """)
    record(
        3, "Incremental load",
        "150 new orders inserted, 10 September orders updated to RETURNED, no duplicates",
        f"ETL: {etl_run['result']['increment']}. ELT mart MERGE: "
        f"{elt_run['result']['increment']}. Fact {before} -> {after} (ELT mart "
        f"{mart_after}), duplicate order_ids in the mart: {dupes}. September RETURNED "
        f"orders went {returned_before} -> {returned_after}, which is the 10 corrections "
        "landing on the original rows. Both MERGEs report UPDATE/INSERT separately via "
        "RETURNING merge_action, so the split is in the log rather than inferred.",
        "merge_staging and merge_mart task logs; "
        "SELECT order_id, count(*) FROM mart_fact_orders GROUP BY 1 HAVING count(*) > 1",
        "the 150 new orders have no payments yet, so they all show up in the "
        "reconciliation as 'delivered order with no successful payment'. That is "
        "correct but noisy - the reconciliation should ignore orders younger than the "
        "latest payment export instead of flagging them",
    )


def experiment_4() -> None:
    (DATA / "products.csv").rename(DATA / "products.csv.bak")
    etl_run = run("etl", "exp4", reset=True)
    elt_run = run("elt", "exp4")
    restore()
    record(
        4, "Missing source file",
        "the task that reads products.csv fails loudly, retries twice, and everything "
        "downstream is marked upstream_failed - no partial load",
        f"ETL: {etl_run['error']} raised in {etl_run['where']}(). "
        f"ELT: {elt_run['error']} raised in {elt_run['where']}(). Both pipelines raise "
        "FileNotFoundError at the extract/load_raw step. I also ran this one in "
        "Airflow proper (screenshots/experiment_4_up_for_retry.jpg and "
        "experiment_4_missing_file.jpg): extract_products went up_for_retry, waited "
        "the 2 minute retry_delay, retried twice (Try Number 3 on the task page) and "
        "then failed; validate_products, transform, load_star, record_transform_dq, "
        "load_increment, reconcile and report all went upstream_failed, so the "
        "warehouse kept the previous numbers instead of loading four files out of "
        "five. audit_run still wrote its pipeline_runs row - "
        "status FAILED, notes \'no result from: [validate_products, transform, "
        "record_validation_dq, load_star, record_transform_dq, load_increment, "
        "reconcile, report]\'. "
        "And it caught a bug in my own DAG: the run was marked SUCCESS in the UI "
        "while pipeline_runs said FAILED, because audit_run is the only leaf task and "
        "Airflow takes the run state from its leaves. audit_run now re-raises after "
        "writing the audit row, so the evidence is saved and the run goes red.",
        "the task log line \'Source file missing: .../products.csv\'; Try Number 3 on "
        "the extract_products page; the Graph view (red extract_products, orange "
        "upstream_failed downstream, four green extract/validate pairs); "
        "pipeline_runs.status and .notes",
        "nothing alerted me - the only signal is the red square in the UI. The bonus "
        "Slack alert on failure should be wired to on_failure_callback, because at "
        "9 AM nobody is watching the Grid view",
    )


def experiment_5() -> None:
    import csv

    bad_rows = [
        # order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price,
        # discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes
        ["EXP5001", "2026-09-15 10:00:00", "", "S01", "P0001", "2", "100.0", "0",
         "200.0", "UPI", "DELIVERED", "App", "12"],
        ["EXP5002", "2026-09-15 10:05:00", "C00005", "S01", "P0001", "-3", "100.0", "0",
         "-300.0", "UPI", "DELIVERED", "App", "12"],
        ["EXP5003", "2026-09-15 10:10:00", "C00005", "S99", "P0001", "1", "100.0", "0",
         "100.0", "UPI", "DELIVERED", "App", "12"],
        ["EXP5004", "2026-09-15 10:15:00", "C00005", "S01", "P0001", "1", "100.0", "0",
         "100.0", "UPI", "SHIPPED", "App", "12"],
        ["EXP5005", "2027-03-01 10:20:00", "C00005", "S01", "P0001", "1", "100.0", "0",
         "100.0", "UPI", "DELIVERED", "App", "12"],
    ]
    with (DATA / "orders.csv").open("a", newline="") as handle:
        csv.writer(handle).writerows(bad_rows)

    run("etl", "exp5", reset=True)
    run("elt", "exp5")

    reasons = query("""
        SELECT record_key, failure_reason FROM elt_quarantine
        WHERE record_key LIKE 'EXP5%' ORDER BY record_key
    """)
    in_fact = scalar("""
        SELECT count(*) FROM mart_fact_orders WHERE order_id LIKE 'EXP5%'
    """)
    quarantine_csv = sorted(cfg.QUARANTINE_DIR.glob("orders_failed_exp5*.csv"))
    restore()
    found = "; ".join(f"{key} -> {reason}" for key, reason in reasons)
    record(
        5, "New bad data",
        "all five rows quarantined, each with the reason that matches the defect, "
        "none of them in the fact table, and dq_results counts up by one per rule",
        f"{len(reasons)} of 5 rows quarantined and {in_fact} of them reached the mart. "
        f"{found}. EXP5003 is interesting: it fails the Consistency check against "
        "stores rather than a column check, because S99 is a well-formed store_id that "
        "simply does not exist. The ETL side wrote the same rows to "
        f"{quarantine_csv[0].name if quarantine_csv else '(no file)'}.",
        "elt_quarantine and the quarantine CSV; dq_results rows for "
        "orders_customer_id_present, orders_quantity_positive, "
        "orders_store_id_exists_in_stores, orders_status_allowed, "
        "orders_not_future_dated",
        "the quarantine row keeps only the parsed values, so for a row that failed "
        "because a value would not parse you have to go back to raw to see what was "
        "actually in the file. Keeping the raw text alongside would make the audit "
        "answer in Part E a single query",
    )


def experiment_6() -> None:
    path = DATA / "orders.csv"
    text = path.read_text()
    header, rest = text.split("\n", 1)
    path.write_text(header.replace("order_amount", "total") + "\n" + rest)

    etl_run = run("etl", "exp6", reset=True)
    elt_run = run("elt", "exp6")
    errors = query("""
        SELECT check_name, detail FROM dq_results
        WHERE run_id = 'exp6' AND status = 'ERROR' LIMIT 3
    """)
    restore()
    record(
        6, "Schema change",
        "a loud failure, not a silent load of wrong numbers",
        f"ETL: {etl_run['error']} in {etl_run['where']}(). ELT: {elt_run['error']} in "
        f"{elt_run['where']}(). The ELT raw load is happy - raw takes whatever columns "
        "arrive - and the failure lands one layer later when stg_orders cannot bind "
        "order_amount, which is the right place for it. On the ETL side the validate "
        "task does not crash: run_checks catches the missing column and records the "
        f"affected checks with status ERROR ({errors}), and the transform step is what "
        "fails. So the amount check was skipped rather than failed, which is a hole: "
        "a dashboard reading only FAIL counts would have seen nothing wrong.",
        "the transform task log (KeyError); the stg_orders task log (Binder Error); "
        "dq_results rows with status = 'ERROR'",
        "treat status = 'ERROR' in dq_results as a pipeline failure, not a note. A "
        "check that could not run is not a check that passed. I would add an explicit "
        "schema assertion at the top of validate: compare the column list against an "
        "expected set and fail there, with the diff in the message",
    )


def experiment_7() -> None:
    # A clean baseline run first, so pipeline_runs has something to compare against.
    run("etl", "exp7_baseline", reset=True)
    run("elt", "exp7_baseline")

    path = DATA / "orders.csv"
    lines = path.read_text().splitlines()
    path.write_text("\n".join(lines[:51]) + "\n")

    run("etl", "exp7")
    run("elt", "exp7")
    volume = query("""
        SELECT dag_id, status, rows_checked, detail FROM dq_results
        WHERE check_name = 'orders_volume_not_collapsed' AND run_id IN ('exp7', 'exp7_baseline')
        ORDER BY run_id, dag_id
    """)
    restore()
    record(
        7, "Volume anomaly",
        "a warning that the order volume collapsed by more than 90%",
        f"orders_volume_not_collapsed fired on both pipelines: {volume}. The baseline is "
        "the previous successful run's rows_loaded from pipeline_runs, so on the very "
        "first run the check reports SKIP instead of inventing an expectation. My first "
        "version compared the fact row count against the total rows read across all "
        "five files, which is about a 50% 'drop' on a perfectly healthy run - it would "
        "have warned every single morning and been muted within a week.",
        "dq_results rows for orders_volume_not_collapsed; pipeline_runs.rows_loaded "
        "for the two runs",
        "90% is far too loose for a business that does ~2,400 orders a month. A 30% "
        "drop day over day is already an incident. I would compare against the median "
        "of the last 7 runs and warn at 30%, fail at 60%",
    )


def experiment_8() -> None:
    import csv

    def scale(unit_price_factor: float, also_scale_amount: bool) -> None:
        path = DATA / "orders.csv"
        rows = list(csv.DictReader(path.read_text().splitlines()))
        fields = list(rows[0].keys())
        for row in rows:
            if row["store_id"] == "S08":
                row["unit_price"] = str(float(row["unit_price"]) * unit_price_factor)
                if also_scale_amount:
                    row["order_amount"] = str(round(
                        float(row["quantity"]) * float(row["unit_price"])
                        * (1 - float(row["discount_pct"]) / 100), 2))
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    run("etl", "exp8_baseline", reset=True)
    run("elt", "exp8_baseline")
    base_revenue = scalar("""
        SELECT round(sum(revenue_amount), 2) FROM mart_fact_orders WHERE store_id = 'S08'
    """)

    # Case A: only unit_price is inflated, so order_amount no longer matches the formula.
    scale(100, also_scale_amount=False)
    run("etl", "exp8a")
    run("elt", "exp8a")
    caught = scalar("""
        SELECT rows_failed FROM dq_results
        WHERE run_id = 'exp8a' AND check_name = 'orders_amount_matches_formula'
          AND dag_id LIKE 'nammamart_etl%'
    """)
    revenue_a = scalar("""
        SELECT round(coalesce(sum(revenue_amount), 0), 2) FROM mart_fact_orders
        WHERE store_id = 'S08'
    """)
    restore()

    # Case B: the exporter scales both columns, so the row is internally consistent.
    scale(100, also_scale_amount=True)
    run("etl", "exp8b")
    run("elt", "exp8b")
    caught_b = scalar("""
        SELECT sum(rows_failed) FROM dq_results
        WHERE run_id = 'exp8b' AND status IN ('FAIL', 'WARN') AND dag_id LIKE 'nammamart_etl%'
    """)
    revenue_b = scalar("""
        SELECT round(sum(revenue_amount), 2) FROM mart_fact_orders WHERE store_id = 'S08'
    """)
    restore()
    record(
        8, "Distribution drift",
        "something notices that one store's prices moved by two orders of magnitude",
        f"Two versions, and they give opposite answers. (a) unit_price x100 only: the "
        f"Accuracy check orders_amount_matches_formula rejected {caught} rows and "
        f"S08 revenue went {base_revenue} -> {revenue_a}, so the drift never reached "
        "the mart - but it reached it as a hole, not as a warning. (b) unit_price and "
        f"order_amount both x100, which is what a broken exporter would really do: "
        f"{caught_b} rows failed anything at all and S08 revenue went {base_revenue} -> "
        f"{revenue_b}. Nothing caught it. Every row is internally consistent, every "
        "value is 'valid', and the mart is wrong by two orders of magnitude.",
        "dq_results for orders_amount_matches_formula; "
        "SELECT sum(revenue_amount) FROM mart_fact_orders WHERE store_id = 'S08'",
        "this is the real gap in my pipeline. I have no distribution check at all. I "
        "would add one that compares each store's median unit_price and average order "
        "value against the last 7 runs and warns on a move beyond about 3 standard "
        "deviations, written as a SQL check on the mart so both pipelines share it",
    )


def experiment_9() -> None:
    import pandas as pd

    path = DATA / "orders.csv"
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    # pandas is used rather than datetime.fromisoformat because a few rows use a
    # different timestamp format. My first attempt skipped exactly those rows, so the
    # newest order in the file was still 21 September and the check reported 11 days
    # instead of the 30 I thought I had set up.
    parsed = pd.to_datetime(frame["order_ts"], errors="coerce", format="mixed")
    shifted = (parsed - pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
    # Rows that genuinely do not parse stay exactly as they were - they are supposed
    # to fail the validity check, not the freshness one.
    frame["order_ts"] = shifted.where(parsed.notna(), frame["order_ts"])
    frame.to_csv(path, index=False)

    run("etl", "exp9", reset=True)
    run("elt", "exp9")
    fresh = query("""
        SELECT dag_id, status, detail FROM dq_results
        WHERE check_name = 'orders_freshness' AND run_id = 'exp9' ORDER BY dag_id
    """)
    restore()
    record(
        9, "Freshness",
        "a timeliness warning that the newest order is a month old",
        f"orders_freshness warned on both pipelines: {fresh}. It says 10-11 days, not "
        "30, and the reason is worth keeping: two orders in the file are dated "
        "21 October, which the timeliness check normally rejects as future-dated. "
        "Shift everything back 30 days and those two become 21 September, perfectly "
        "valid, and they are then the newest order in the file. So the measured "
        "staleness is 11 days. The check fires either way, but it is a reminder that "
        "'newest row' is only as trustworthy as the validity checks in front of it. "
        "Worth being honest about "
        "the limitation: the check compares the newest order against the fixed end of "
        "the data window (2026-10-02) rather than against the clock, because the "
        "dataset is a 2026 extract and 'now' would make every run look stale. In "
        "production the comparison has to be now() in Asia/Kolkata.",
        "dq_results rows for orders_freshness; the ELT detail line gives the age in days",
        "compare against now() with the window end only as a development override, and "
        "raise it from Warning to Critical - a stale file at 9 AM means the upstream "
        "export did not run, and reporting yesterday's numbers as today's is worse "
        "than reporting nothing",
    )


def experiment_10() -> None:
    record(
        10, "Recovery (Clear a failed task)",
        "clearing the failed task re-runs only that task and the ones downstream of it, "
        "not the whole DAG",
        "Done in the Airflow UI after experiment 4, not in this script. I restored "
        "products.csv and hit Clear on the failed extract_products. The dialog said "
        "\'Affected Tasks: 10\' and listed exactly extract_products and its nine "
        "downstream tasks (screenshots/experiment_10_clear_downstream.jpg); the other "
        "four extract and validate pairs were not in the list and kept their green "
        "state from the first attempt. All 10 re-ran, the DAG run went from failed to "
        "success, and the pipeline_runs row for that run_id changed from FAILED to "
        "SUCCESS rather than a second row appearing - which is why record_run deletes "
        "the existing row for that (run_id, dag_id) before inserting.",
        "the Clear Task Instance dialog (Affected Tasks: 10); the task states before "
        "and after; pipeline_runs for that run_id",
        "nothing on the mechanism. What I would add is making the extract tasks safe to "
        "clear in isolation, which they already are because every load is a replace or "
        "a merge on a key",
    )


EXPERIMENTS = {
    1: experiment_1, 2: experiment_2, 3: experiment_3, 4: experiment_4,
    5: experiment_5, 6: experiment_6, 7: experiment_7, 8: experiment_8,
    9: experiment_9, 10: experiment_10,
}


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    wanted = [int(a) for a in sys.argv[1:]] or sorted(EXPERIMENTS)
    print("## Part F: observability experiments\n")
    print(f"Sandbox: `{SANDBOX}` (the real dataset and warehouse are untouched).\n")
    for number in wanted:
        EXPERIMENTS[number]()
    print("\n---\n")


if __name__ == "__main__":
    main()
