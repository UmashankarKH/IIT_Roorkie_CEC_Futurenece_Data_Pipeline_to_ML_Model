"""NammaMart ELT pipeline (Part B): Extract -> Load RAW -> Transform inside DuckDB with SQL.

ELT means the raw files are loaded FIRST, untouched, and all cleaning happens afterwards in SQL.

 extract x5 --> load_raw --> stg_customers --> stg_products --> stg_stores --> stg_orders
                                                       --> stg_payments --> stg_orders_merge
                 --> mart_* (7 tables) --> compare_with_etl --> (audit always runs)

Layers (all in include/warehouse_chandini.duckdb)
  raw_*   exact copy of each CSV, all text, append-only, tagged with _run_id/_source_file/_source_row
  stg_*   typed + cleaned + quality-flagged; bad rows stay here with dq_status = 'FAIL' + failure_reason
  mart_*  business tables built only from rows with dq_status = 'PASS'
  dq_results, pipeline_runs   shared audit tables (same layout as the ETL DAG)

All transformation logic lives in include/sql/*.sql - this file only runs those files in order.

Run options (Trigger DAG w/ config)
  load_increment = true   also load orders_2026-10-01_increment.csv; the staging step then MERGES it
                          into stg_orders on order_id (new orders added, corrected orders replaced).

Idempotent: staging/marts are rebuilt with CREATE OR REPLACE; the raw batch of a run is deleted before
it is re-inserted; the merge is DELETE + INSERT on order_id. Running twice never double counts.
"""
from __future__ import annotations

import logging
import re
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum

try:  # Airflow 3
    from airflow.sdk import Param, Variable, dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task
    from airflow.models import Variable
    from airflow.models.param import Param

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from include.dbutil import connect, render, run_sql_file  # noqa: E402
from include.dq_checks import DQ_RESULTS_DDL, write_dq_results  # noqa: E402
from include.elt_checks_meta import ELT_CHECKS  # noqa: E402

log = logging.getLogger("airflow.task")

# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #
OWNER = "chandini"
DAG_ID = f"nammamart_elt_{OWNER}"
ETL_DAG_ID = f"nammamart_etl_{OWNER}"
INCLUDE = PROJECT / "include"
SQL_DIR = INCLUDE / "sql"
DATA_DIR = INCLUDE / "data" / "nammamart"
WAREHOUSE = INCLUDE / f"warehouse_{OWNER}.duckdb"
TABLES = ["orders", "payments", "customers", "products", "stores"]
INCREMENT_GLOB = "orders_*_increment.csv"
DEFAULT_AS_OF = "2026-10-02 00:00:00"      # same "now" as the ETL DAG (the data is a historic Sept 2026 extract)
STAGING_ORDER = ["stg_customers", "stg_products", "stg_stores"]
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla", "mart_revenue_by_payment_mode",
         "mart_top_customers", "mart_payment_reconciliation", "mart_rejected_rows"]

EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
               "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date",
                  "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
                 "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"],
}


def _setting(name: str, default: str) -> str:
    try:
        value = Variable.get(name)
        return default if value in (None, "") else str(value)
    except Exception:
        return default


def _connect():
    return connect(WAREHOUSE)


def _sql_path(p) -> str:
    return str(p).replace("\\", "/").replace("'", "''")


def _write_dq(con, run_id: str, table: str, source: str, flag_cols: dict, extra_rows: list | None = None) -> int:
    """Turn the chk_<name> flag columns of a staging table into dq_results rows.

    flag_cols: {check_name: (dimension, severity)}.  `source` is a table name or a (select ...) to count over.
    """
    n = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
    now = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)
    rows = []
    if flag_cols:
        sums = ", ".join(f"coalesce(sum(CASE WHEN chk_{c} THEN 1 ELSE 0 END), 0)" for c in flag_cols)
        vals = con.execute(f"SELECT {sums} FROM {source}").fetchone()
        for (name, (dim, sev)), failed in zip(flag_cols.items(), vals):
            failed = int(failed)
            status = "PASS" if failed == 0 else ("FAIL" if sev == "Critical" else "WARN")
            rows.append((run_id, DAG_ID, f"{table}.{name}", dim, sev, n, failed, status, now))
    for (name, dim, sev, failed) in (extra_rows or []):
        status = "PASS" if failed == 0 else ("FAIL" if sev == "Critical" else "WARN")
        rows.append((run_id, DAG_ID, f"{table}.{name}", dim, sev, n, int(failed), status, now))
    df = pd.DataFrame(rows, columns=["run_id", "dag_id", "check_name", "dimension", "severity",
                                     "rows_checked", "rows_failed", "status", "checked_at"])
    write_dq_results(con, df)
    return n


def _flags(table: str) -> dict:
    return {name: (dim, sev) for (name, dim, sev) in ELT_CHECKS[table]}


def _dataset_checks(con, table: str, source: str, ts_col: str, baseline: int, as_of: str, max_age: float):
    """Dataset-level Warnings (same as the ETL DAG): row-count collapse and stale data."""
    n = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
    newest = con.execute(
        f"SELECT max({ts_col}) FROM {source} WHERE {ts_col} IS NOT NULL AND {ts_col} <= CAST('{as_of}' AS TIMESTAMP)"
    ).fetchone()[0]
    cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=max_age)
    stale = newest is None or pd.Timestamp(newest) < cutoff
    collapsed = bool(baseline) and n < baseline * 0.1
    out = [("volume_not_collapsed", "Completeness", "Warning", n if collapsed else 0)]
    if ts_col in ("order_ts", "payment_ts"):
        label = "latest_order_not_stale" if table == "orders" else "latest_payment_not_stale"
        out.append((label, "Timeliness", "Warning", n if stale else 0))
    return out


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
@task(retries=2)
def extract(table: str, **context):
    """Check the source file exists and has the expected columns. Nothing is changed or copied."""
    src = DATA_DIR / f"{table}.csv"
    if not src.exists():
        raise FileNotFoundError(f"Source file not found: {src}")
    header = pd.read_csv(src, dtype=str, nrows=0)
    missing = [c for c in EXPECTED_COLUMNS[table] if c not in header.columns]
    if missing:
        raise ValueError(f"SCHEMA CHANGE in {src.name}: missing column(s) {missing}. Found: {list(header.columns)}")
    rows = len(pd.read_csv(src, dtype=str, usecols=[0], keep_default_na=False))
    result = {"table": table, "path": str(src), "rows": rows, "increment_files": [], "increment_rows": 0}
    if table == "orders" and context["params"].get("load_increment"):
        files = sorted(DATA_DIR.glob(INCREMENT_GLOB))
        if not files:
            raise FileNotFoundError(f"load_increment=true but no file matching {INCREMENT_GLOB} in {DATA_DIR}")
        for f in files:
            h = pd.read_csv(f, dtype=str, nrows=0)
            miss = [c for c in EXPECTED_COLUMNS["orders"] if c not in h.columns]
            if miss:
                raise ValueError(f"SCHEMA CHANGE in {f.name}: missing column(s) {miss}")
            result["increment_files"].append(f.name)
            result["increment_rows"] += len(pd.read_csv(f, dtype=str, usecols=[0], keep_default_na=False))
    log.info("extract %s: %s rows (+%s increment rows)", table, result["rows"], result["increment_rows"])
    return result


@task
def load_raw(extracts: list, **context):
    """LOAD step of ELT: copy each CSV as-is (all text) into raw_<table>. No cleaning at all."""
    run_id = context["run_id"]
    loaded_at = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None).strftime("%Y-%m-%d %H:%M:%S")
    con = _connect()
    counts = {}
    try:
        con.execute("BEGIN")
        for e in extracts:
            jobs = [(e["path"], Path(e["path"]).name)]
            jobs += [(str(DATA_DIR / name), name) for name in e["increment_files"]]
            for path, source_file in jobs:
                run_sql_file(con, SQL_DIR / "raw_load.sql", table=e["table"], path=_sql_path(path),
                             source_file=source_file, run_id=run_id, loaded_at=loaded_at)
        con.execute("COMMIT")
        for e in extracts:
            counts[e["table"]] = con.execute(
                f"SELECT count(*) FROM raw_{e['table']} WHERE _run_id = ?", [run_id]).fetchone()[0]
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    log.info("raw rows loaded this run: %s", counts)
    return counts


def _stage(sql_file: str, table: str, run_id: str, **extra):
    as_of = _setting("NAMMAMART_AS_OF", DEFAULT_AS_OF)
    con = _connect()
    try:
        con.execute("BEGIN")
        run_sql_file(con, SQL_DIR / sql_file, run_id=run_id, as_of=as_of, **extra)
        con.execute("COMMIT")
        return con, as_of
    except Exception:
        con.execute("ROLLBACK")
        con.close()
        raise


def _dim_task(sql_file: str, table: str, run_id: str):
    con, as_of = _stage(sql_file, table, run_id)
    try:
        n = _write_dq(con, run_id, table, f"stg_{table}", _flags(table))
        failed = con.execute(f"SELECT count(*) FROM stg_{table} WHERE dq_status = 'FAIL'").fetchone()[0]
    finally:
        con.close()
    log.info("stg_%s: %s rows, %s failed a Critical check", table, n, failed)
    return {"rows": int(n), "failed": int(failed)}


@task
def stg_customers(raw_counts: dict, **context):
    return _dim_task("stg_customers.sql", "customers", context["run_id"])


@task
def stg_products(prev: dict, **context):
    return _dim_task("stg_products.sql", "products", context["run_id"])


@task
def stg_stores(prev: dict, **context):
    return _dim_task("stg_stores.sql", "stores", context["run_id"])


@task
def stg_orders(prev: dict, **context):
    """Clean orders.csv into stg_orders and any increment file into stg_orders_increment."""
    run_id = context["run_id"]
    as_of = _setting("NAMMAMART_AS_OF", DEFAULT_AS_OF)
    max_age = float(_setting("NAMMAMART_MAX_AGE_DAYS", "2"))
    baseline = int(_setting("NAMMAMART_BASELINE_ORDERS", "2400"))
    con = _connect()
    try:
        con.execute("BEGIN")
        run_sql_file(con, SQL_DIR / "stg_orders.sql", run_id=run_id, as_of=as_of, target="stg_orders",
                     file_filter="_source_file = 'orders.csv'")
        run_sql_file(con, SQL_DIR / "stg_orders.sql", run_id=run_id, as_of=as_of, target="stg_orders_increment",
                     file_filter="_source_file <> 'orders.csv'")
        con.execute("COMMIT")
        # counted BEFORE the merge so a retry of the merge task cannot change these numbers
        inc_pass = con.execute("SELECT count(*) FROM stg_orders_increment WHERE dq_status = 'PASS'").fetchone()[0]
        updated = con.execute("""SELECT count(*) FROM stg_orders_increment i
                                 WHERE i.dq_status = 'PASS' AND i.order_id IN (SELECT order_id FROM stg_orders)""").fetchone()[0]
        flags = _flags("orders")
        n = _write_dq(con, run_id, "orders", "stg_orders", flags,
                      _dataset_checks(con, "orders", "stg_orders", "order_ts", baseline, as_of, max_age))
        n_inc = con.execute("SELECT count(*) FROM stg_orders_increment").fetchone()[0]
        if n_inc:
            _write_dq(con, run_id, "orders_increment", "stg_orders_increment", flags)
        failed = con.execute("SELECT count(*) FROM stg_orders WHERE dq_status = 'FAIL'").fetchone()[0]
        failed_inc = con.execute("SELECT count(*) FROM stg_orders_increment WHERE dq_status = 'FAIL'").fetchone()[0]
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    res = {"rows": int(n), "failed": int(failed), "increment_rows": int(n_inc), "increment_failed": int(failed_inc),
           "increment_pass": int(inc_pass), "to_update": int(updated), "to_insert": int(inc_pass - updated)}
    log.info("stg_orders: %s", res)
    return res


@task
def stg_payments(prev: dict, **context):
    run_id = context["run_id"]
    as_of = _setting("NAMMAMART_AS_OF", DEFAULT_AS_OF)
    max_age = float(_setting("NAMMAMART_MAX_AGE_DAYS", "2"))
    baseline = int(_setting("NAMMAMART_BASELINE_PAYMENTS", "2200"))
    con, _ = _stage("stg_payments.sql", "payments", run_id)
    try:
        n = _write_dq(con, run_id, "payments", "stg_payments", _flags("payments"),
                      _dataset_checks(con, "payments", "stg_payments", "payment_ts", baseline, as_of, max_age))
        failed = con.execute("SELECT count(*) FROM stg_payments WHERE dq_status = 'FAIL'").fetchone()[0]
    finally:
        con.close()
    log.info("stg_payments: %s rows, %s failed", n, failed)
    return {"rows": int(n), "failed": int(failed)}


@task
def stg_orders_merge(prev: dict, orders: dict, **context):
    """UPSERT: passed increment rows replace older rows with the same order_id, new ones are added."""
    con, _ = _stage("stg_orders_merge.sql", "orders", context["run_id"])
    try:
        total = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
        clean = con.execute("SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'").fetchone()[0]
    finally:
        con.close()
    res = {"updated": orders["to_update"], "inserted": orders["to_insert"], "stg_orders_rows": int(total),
           "clean_orders": int(clean)}
    log.info("merge: %s", res)
    return res


def _mart(sql_file: str, run_id: str):
    con, _ = _stage(sql_file, sql_file[:-4], run_id)
    try:
        table = sql_file[:-4]
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    finally:
        con.close()
    log.info("%s: %s rows", sql_file[:-4], n)
    return int(n)


@task
def build_mart(name: str, prev=None, **context):
    return {name: _mart(f"{name}.sql", context["run_id"])}


@task
def compare_with_etl(prev=None, **context):
    """Part B check: ELT marts must give the same totals as the ETL tables (skipped if ETL has not run)."""
    con = _connect()
    try:
        have = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        needed = {"etl_fact_orders", "etl_category_margin", "etl_payment_reconciliation"}
        if not needed <= have:
            log.warning("ETL tables not found (%s missing) - run the ETL DAG with the same settings first.",
                        sorted(needed - have))
            return {"compared": False, "different": None}
        run_sql_file(con, SQL_DIR / "elt_vs_etl_comparison.sql")
        df = con.execute("SELECT * FROM elt_vs_etl_comparison").df()
    finally:
        con.close()
    log.info("ELT vs ETL comparison:\n%s", df.to_string(index=False))
    different = int((df["status"] != "MATCH").sum())
    if different:
        log.warning("%s metric(s) DIFFER between ETL and ELT. (Normal if the two DAGs ran with different "
                    "load_increment settings.)", different)
    return {"compared": True, "different": different}


@task(trigger_rule="all_done")
def audit(**context):
    """Runs even when something failed: one row per run in pipeline_runs."""
    ti, run_id = context["ti"], context["run_id"]

    def pull(task_id):
        try:
            return ti.xcom_pull(task_ids=task_id)
        except Exception:
            return None

    extracts = [pull(f"extract_{t}") for t in TABLES]
    rows_extracted = sum((e["rows"] + e["increment_rows"]) for e in extracts if e)
    staged = [pull(t) for t in ["stg_customers", "stg_products", "stg_stores", "stg_orders", "stg_payments"]]
    quarantined = sum(s["failed"] + s.get("increment_failed", 0) for s in staged if s)
    merge = pull("stg_orders_merge")
    loaded = merge["clean_orders"] if merge else 0
    status = "SUCCESS" if (all(extracts) and pull("compare_with_etl") is not None) else "FAILED"
    dag_run = context.get("dag_run")
    start = getattr(dag_run, "start_date", None) or pendulum.now("Asia/Kolkata")
    start = pd.Timestamp(start).tz_convert("Asia/Kolkata").tz_localize(None)
    end = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)
    row = pd.DataFrame([{
        "run_id": run_id, "dag_id": DAG_ID, "start_time": start, "end_time": end,
        "rows_extracted": int(rows_extracted), "rows_loaded": int(loaded),
        "rows_quarantined": int(quarantined), "status": status,
    }])
    con = _connect()
    try:
        con.execute(
            """CREATE TABLE IF NOT EXISTS pipeline_runs (
                   run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
                   rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR)"""
        )
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
        con.register("_run", row)
        con.execute("INSERT INTO pipeline_runs SELECT * FROM _run")
        con.unregister("_run")
    finally:
        con.close()
    log.info("audit: %s", row.to_dict("records")[0])
    if status == "FAILED":
        raise RuntimeError("Pipeline run FAILED - see the failed task's log. (audit row was written first)")
    return status


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: load raw CSVs into DuckDB first, then clean and model them with SQL",
    schedule="0 9 * * 1-6",                                   # 9:00 AM IST, Monday to Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": OWNER,
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=15),
    },
    params={"load_increment": Param(False, type="boolean",
                                    description="Also load orders_*_increment.csv and merge it into stg_orders")},
    tags=["nammamart", "elt", OWNER],
    doc_md=__doc__,
)
def nammamart_elt():
    extracts = [extract.override(task_id=f"extract_{t}")(t) for t in TABLES]   # 5 tasks run in parallel
    raw = load_raw(extracts)
    c = stg_customers(raw)
    p = stg_products(c)
    s = stg_stores(p)
    o = stg_orders(s)
    pay = stg_payments(o)
    merged = stg_orders_merge(pay, o)
    prev = merged
    for m in MARTS:                                  # one at a time: DuckDB allows a single writer
        prev = build_mart.override(task_id=m)(m, prev)
    cmp_ = compare_with_etl(prev)
    audit_task = audit()
    [*extracts, raw, c, p, s, o, pay, merged, cmp_] >> audit_task


nammamart_elt()
