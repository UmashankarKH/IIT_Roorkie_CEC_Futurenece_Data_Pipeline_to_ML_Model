"""NammaMart ETL pipeline (Part A): Extract -> Validate -> Transform -> Reconcile -> Load -> Report.

ETL means the cleaning happens in Python (pandas) BEFORE anything reaches the warehouse.

                      +--> quarantine/<file>_failed_<run_id>.csv  (bad rows + failure_reason)
extract x5 (parallel) -+--> validate --> transform --> reconcile --> load --> report
                                                                      |
                                                       audit (always runs, writes pipeline_runs)

Tables written to include/warehouse_chandini.duckdb
  etl_fact_orders, etl_dim_customer, etl_dim_product, etl_dim_store
  etl_payment_reconciliation, etl_quarantine_rows
  etl_daily_zone_revenue, etl_category_margin           (report task)
  dq_results, pipeline_runs                             (audit tables, one set of rows per run)

Run options (Trigger DAG w/ config in the Airflow UI)
  load_increment = true   also load orders_2026-10-01_increment.csv on top of orders.csv
                          (new orders are added, corrected orders overwrite the old row)

Idempotent: every run rebuilds the etl_* tables inside one transaction, so running
the same DAG twice gives the same row counts and revenue (nothing is double counted).

Optional Airflow Variables (Admin > Variables); defaults shown
  NAMMAMART_AS_OF            2026-10-02 00:00:00   "now" for the future-date check. The data is a
                                                   historic Sept 2026 extract; in production use the
                                                   real current time (delete the variable and set
                                                   DEFAULT_AS_OF = None below).
  NAMMAMART_MAX_AGE_DAYS     2                     freshness limit (Warning)
  NAMMAMART_BASELINE_ORDERS  2400                  expected order rows, for the volume-drop check
  NAMMAMART_BASELINE_PAYMENTS 2200
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

from include.dq_checks import (  # noqa: E402
    CHECKS,
    DQ_RESULTS_DDL,
    build_context,
    canonical_payment_mode,
    canonical_status,
    canonical_tier,
    run_checks,
    write_dq_results,
    write_quarantine,
)
from include.dbutil import connect  # noqa: E402
from include.masking import mask_email_series, mask_phone_series  # noqa: E402

log = logging.getLogger("airflow.task")

# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #
OWNER = "chandini"
DAG_ID = f"nammamart_etl_{OWNER}"
INCLUDE = PROJECT / "include"
DATA_DIR = INCLUDE / "data" / "nammamart"
WAREHOUSE = INCLUDE / f"warehouse_{OWNER}.duckdb"
QUARANTINE_DIR = INCLUDE / "quarantine" / OWNER
WORK_ROOT = INCLUDE / "_work" / "etl"          # temporary files passed between tasks
TABLES = ["orders", "payments", "customers", "products", "stores"]
INCREMENT_GLOB = "orders_*_increment.csv"
DEFAULT_AS_OF = "2026-10-02 00:00:00"
# Repairs for rows that fail ONE specific check and can be fixed from other data.
# The row stays in the quarantine file (so it is not silently changed) but is reinstated.
ZONE_FIXES = {"S07": "South"}  # BTM Layout: blank zone, neighbours S04-S06 are South (confirm with client)

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


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _safe(run_id) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(run_id))


def _work_dir(run_id) -> Path:
    d = WORK_ROOT / _safe(run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _read(path) -> pd.DataFrame:
    """Read a CSV with everything as text, so nothing is silently converted."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _setting(name: str, default: str) -> str:
    try:
        value = Variable.get(name)
        return default if value in (None, "") else str(value)
    except Exception:  # variable not defined
        return default


def _check_columns(df: pd.DataFrame, label: str, expected: list[str]) -> None:
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(
            f"SCHEMA CHANGE in {label}: missing column(s) {missing}. Found columns: {list(df.columns)}"
        )


def _connect():
    return connect(WAREHOUSE)       # waits and retries if the ELT DAG is writing at the same moment


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
@task(retries=2)
def extract(table: str, **context):
    """Read one source file and park a copy in this run's work folder."""
    run_id = context["run_id"]
    src = DATA_DIR / f"{table}.csv"
    if not src.exists():
        raise FileNotFoundError(f"Source file not found: {src}")
    df = _read(src)
    _check_columns(df, f"{table}.csv", EXPECTED_COLUMNS[table])
    out = _work_dir(run_id) / f"raw_{table}.csv"
    df.to_csv(out, index=False)
    result = {"table": table, "path": str(out), "rows": len(df), "increment_path": None, "increment_rows": 0}

    if table == "orders" and context["params"].get("load_increment"):
        files = sorted(DATA_DIR.glob(INCREMENT_GLOB))
        if not files:
            raise FileNotFoundError(f"load_increment=true but no file matching {INCREMENT_GLOB} in {DATA_DIR}")
        inc = pd.concat([_read(f) for f in files], ignore_index=True)
        _check_columns(inc, "increment file", EXPECTED_COLUMNS["orders"])
        inc_out = _work_dir(run_id) / "raw_orders_increment.csv"
        inc.to_csv(inc_out, index=False)
        result["increment_path"] = str(inc_out)
        result["increment_rows"] = len(inc)
    log.info("extract %s: %s rows (+%s increment rows)", table, result["rows"], result["increment_rows"])
    return result


@task
def validate(extracts: list, **context):
    """Run every quality check. Critical failures are quarantined; Warnings load but are logged."""
    run_id = context["run_id"]
    work = _work_dir(run_id)
    info = {e["table"]: e for e in extracts}
    raw = {t: _read(info[t]["path"]) for t in TABLES}
    inc = _read(info["orders"]["increment_path"]) if info["orders"]["increment_path"] else None

    ctx = build_context(
        raw,
        as_of=pd.Timestamp(_setting("NAMMAMART_AS_OF", DEFAULT_AS_OF)),
        baseline_rows={
            "orders": int(_setting("NAMMAMART_BASELINE_ORDERS", "2400")),
            "payments": int(_setting("NAMMAMART_BASELINE_PAYMENTS", "2200")),
        },
        max_age_days=float(_setting("NAMMAMART_MAX_AGE_DAYS", "2")),
    )

    passed, failed, results = {}, {}, []
    for t in TABLES:
        passed[t], failed[t], res = run_checks(raw[t], t, ctx, run_id, DAG_ID)
        results.append(res)
    if inc is not None:
        passed["orders_increment"], failed["orders_increment"], res = run_checks(inc, "orders", ctx, run_id, DAG_ID)
        res["check_name"] = res["check_name"].str.replace("orders.", "orders_increment.", n=1, regex=False)
        results.append(res)
    results = pd.concat(results, ignore_index=True)

    # ---- repairs: rows that fail exactly one fixable check ----
    repaired = {}
    fs = failed["stores"]
    fix = fs[(fs["failed_checks"] == "zone_not_null") & fs["store_id"].isin(ZONE_FIXES)].copy()
    if len(fix):
        fix["zone"] = fix["store_id"].map(ZONE_FIXES)
        fix["dq_warnings"] = "zone repaired from ZONE_FIXES"
        passed["stores"] = pd.concat([passed["stores"], fix[passed["stores"].columns]], ignore_index=True)
        repaired["stores"] = fix["store_id"].tolist()
    fp = failed["products"]
    cat_map = passed["products"].groupby("sub_category")["category"].agg(lambda s: s.mode().iat[0]).to_dict()
    rep = fp[fp["failed_checks"] == "category_not_null"].copy()
    rep["category"] = rep["sub_category"].map(cat_map)
    rep = rep[rep["category"].notna()]
    if len(rep):
        rep["dq_warnings"] = "category repaired from sub_category"
        passed["products"] = pd.concat([passed["products"], rep[passed["products"].columns]], ignore_index=True)
        repaired["products"] = rep["product_id"].tolist()
    # Bad price on an otherwise good product: keep the product (orders need its category) but blank the
    # untrustworthy value, so margin for those orders is NULL instead of wrong.
    #   cost_price > mrp                  -> cost_price blanked
    #   mrp <= 0 (which also trips cost)  -> mrp blanked, cost_price kept
    for checks, col, note in [("cost_price_not_above_mrp", "cost_price", "cost_price blanked (above mrp)"),
                              ("mrp_positive, cost_price_not_above_mrp", "mrp", "mrp blanked (not positive)")]:
        fx = fp[fp["failed_checks"] == checks].copy()
        if len(fx):
            fx[col] = ""
            fx["dq_warnings"] = note
            passed["products"] = pd.concat([passed["products"], fx[passed["products"].columns]], ignore_index=True)
            repaired.setdefault("products", []).extend(fx["product_id"].tolist())

    # ---- quarantine files (customer contact details masked even here) ----
    quarantine_files, summary = {}, []
    key_col = {"orders": "order_id", "orders_increment": "order_id", "payments": "payment_id",
               "customers": "customer_id", "products": "product_id", "stores": "store_id"}
    for label, f in failed.items():
        to_write = f.copy()
        if label == "customers" and len(to_write):
            to_write["email"] = mask_email_series(to_write["email"])
            to_write["phone"] = mask_phone_series(to_write["phone"])
        path = write_quarantine(to_write, QUARANTINE_DIR, label, run_id)
        quarantine_files[label] = str(path) if path else None
        for _, r in f.iterrows():
            summary.append({"source_file": f"{label}.csv", "row_key": r[key_col[label]],
                            "source_row": int(r["_source_row"]), "failure_reason": r["failure_reason"],
                            "failed_checks": r["failed_checks"], "run_id": run_id})
    pd.DataFrame(summary, columns=["source_file", "row_key", "source_row", "failure_reason",
                                   "failed_checks", "run_id"]).to_pickle(work / "quarantine_rows.pkl")

    # ---- hand clean rows to the next task ----
    for label, p in passed.items():
        p.to_pickle(work / f"passed_{label}.pkl")

    con = _connect()
    try:
        write_dq_results(con, results)
    finally:
        con.close()

    not_ok = results[results["status"] != "PASS"]
    for _, r in not_ok.iterrows():
        log.warning("DQ %-4s %-8s %-48s failed %s of %s rows", r["status"], r["severity"], r["check_name"],
                    r["rows_failed"], r["rows_checked"])
    quarantined = {label: len(f) for label, f in failed.items()}
    log.info("validate: passed=%s quarantined=%s repaired=%s",
             {k: len(v) for k, v in passed.items()}, quarantined, repaired)
    return {
        "has_increment": inc is not None,
        "passed": {k: len(v) for k, v in passed.items()},
        "quarantined": quarantined,
        "rows_quarantined": int(sum(quarantined.values())),
        "repaired": repaired,
        "quarantine_files": quarantine_files,
        "dq_checks_run": int(len(results)),
        "dq_checks_not_pass": int(len(not_ok)),
    }


@task
def transform(validation: dict, **context):
    """Standardise, deduplicate, mask personal data, join and derive the business columns."""
    run_id = context["run_id"]
    work = _work_dir(run_id)

    # ---- dimensions ----
    cust = pd.read_pickle(work / "passed_customers.pkl").drop(columns=["dq_warnings"])
    cust["loyalty_tier"] = canonical_tier(cust["loyalty_tier"])
    cust["email"] = mask_email_series(cust["email"])           # personal data masked
    cust["phone"] = mask_phone_series(cust["phone"])
    cust["signup_date"] = pd.to_datetime(cust["signup_date"]).dt.date
    cust["is_active"] = cust["is_active"].eq("Y")
    cust = cust.drop_duplicates("customer_id")

    prod = pd.read_pickle(work / "passed_products.pkl").drop(columns=["dq_warnings"])
    for c in ["mrp", "cost_price"]:
        prod[c] = pd.to_numeric(prod[c], errors="coerce")   # blanked values become NULL
    prod["is_perishable"] = prod["is_perishable"].eq("True")
    prod["launch_date"] = pd.to_datetime(prod["launch_date"]).dt.date
    prod = prod.drop_duplicates("product_id")

    store = pd.read_pickle(work / "passed_stores.pkl").drop(columns=["dq_warnings"])
    store["capacity_orders_per_day"] = pd.to_numeric(store["capacity_orders_per_day"]).astype(int)
    store["open_date"] = pd.to_datetime(store["open_date"]).dt.date
    store = store.drop_duplicates("store_id")

    # ---- orders: base file, then the increment on top (increment wins on the same order_id) ----
    base = pd.read_pickle(work / "passed_orders.pkl").drop(columns=["dq_warnings"])
    orders, inserted, updated = base, 0, 0
    if validation["has_increment"]:
        inc = pd.read_pickle(work / "passed_orders_increment.pkl").drop(columns=["dq_warnings"])
        updated = int(inc["order_id"].isin(base["order_id"]).sum())
        inserted = int(len(inc) - updated)
        orders = pd.concat([base[~base["order_id"].isin(inc["order_id"])], inc], ignore_index=True)
    orders = orders.copy()
    orders["order_status"] = canonical_status(orders["order_status"])
    orders["payment_mode"] = canonical_payment_mode(orders["payment_mode"])
    orders["channel"] = orders["channel"].str.strip()
    orders["order_ts"] = pd.to_datetime(orders["order_ts"])
    for c in ["quantity", "delivery_minutes"]:
        orders[c] = pd.to_numeric(orders[c])
    for c in ["unit_price", "discount_pct", "order_amount"]:
        orders[c] = pd.to_numeric(orders[c])
    orders["quantity"] = orders["quantity"].astype(int)
    orders["order_amount"] = orders["order_amount"].round(2)

    # ---- star schema join + derived columns ----
    n_before = len(orders)
    fact = (
        orders.merge(store[["store_id", "store_name", "zone"]], on="store_id", how="left")
        .merge(prod[["product_id", "product_name", "category", "cost_price"]], on="product_id", how="left")
        .merge(cust[["customer_id", "loyalty_tier"]], on="customer_id", how="left")
    )
    assert len(fact) == n_before, "a join duplicated or dropped orders"
    delivered = fact["order_status"].eq("DELIVERED")
    fact["order_date"] = fact["order_ts"].dt.date
    fact["net_revenue"] = fact["order_amount"].where(delivered, 0.0)          # only DELIVERED counts as revenue
    fact["gross_margin"] = (fact["net_revenue"] - fact["quantity"] * fact["cost_price"]).where(delivered).round(2)
    fact["within_15_min"] = delivered & (fact["delivery_minutes"] <= 15)
    fact = fact[["order_id", "order_ts", "order_date", "customer_id", "loyalty_tier", "store_id", "store_name",
                 "zone", "product_id", "product_name", "category", "quantity", "unit_price", "cost_price",
                 "discount_pct", "order_amount", "net_revenue", "gross_margin", "payment_mode", "order_status",
                 "channel", "delivery_minutes", "within_15_min"]].sort_values("order_id")

    paths = {}
    for name, df in [("fact", fact), ("customer", cust), ("product", prod), ("store", store)]:
        paths[name] = str(work / f"t_{name}.pkl")
        df.to_pickle(paths[name])
    counts = {k: int(len(v)) for k, v in [("fact", fact), ("customer", cust), ("product", prod), ("store", store)]}
    log.info("transform: %s | increment inserted=%s updated=%s | orders without category=%s",
             counts, inserted, updated, int(fact["category"].isna().sum()))
    return {"paths": paths, "counts": counts, "increment_inserted": inserted, "increment_updated": updated}


@task
def reconcile(transformed: dict, **context):
    """List every order/payment mismatch, one row per problem, with the reason."""
    run_id = context["run_id"]
    work = _work_dir(run_id)
    fact = pd.read_pickle(transformed["paths"]["fact"])
    o = fact[["order_id", "order_status", "order_amount"]]

    pay = _read(work / "raw_payments.csv").drop_duplicates("payment_id")       # exact repeats counted once
    pay["amount_paid"] = pd.to_numeric(pay["amount_paid"], errors="coerce")
    raw_ids = set(_read(work / "raw_orders.csv")["order_id"])
    inc_path = work / "raw_orders_increment.csv"
    if inc_path.exists():
        raw_ids |= set(_read(inc_path)["order_id"])
    succ = pay[pay["payment_status"] == "SUCCESS"]

    cols = ["order_id", "payment_id", "order_status", "order_amount", "payment_status", "amount_paid",
            "unreconciled_amount", "reason"]
    parts = []

    def add(df, reason, amount_col):
        if len(df):
            d = df.copy()
            d["reason"] = reason
            d["unreconciled_amount"] = d[amount_col].abs().round(2)
            for c in cols:
                if c not in d.columns:
                    d[c] = None
            parts.append(d[cols])

    delivered = o[o["order_status"] == "DELIVERED"]
    add(delivered[~delivered["order_id"].isin(pay["order_id"])], "DELIVERED_ORDER_HAS_NO_PAYMENT", "order_amount")
    only_failed = delivered[delivered["order_id"].isin(pay["order_id"]) & ~delivered["order_id"].isin(succ["order_id"])]
    add(only_failed.merge(pay, on="order_id", how="left"), "DELIVERED_ORDER_ONLY_FAILED_PAYMENT", "order_amount")

    m = succ.merge(o, on="order_id", how="inner")
    m["difference"] = (m["amount_paid"] - m["order_amount"]).round(2)
    add(m[m["difference"].abs() > 0.01], "AMOUNT_MISMATCH", "difference")

    returned = o[o["order_status"] == "RETURNED"]
    refunded = pay[pay["payment_status"] == "REFUNDED"]["order_id"]
    add(returned[~returned["order_id"].isin(pay["order_id"])], "RETURNED_ORDER_HAS_NO_PAYMENT", "order_amount")
    add(succ.merge(returned, on="order_id")[lambda d: ~d["order_id"].isin(refunded)],
        "RETURNED_ORDER_NOT_REFUNDED", "amount_paid")

    outside = pay[~pay["order_id"].isin(o["order_id"])]
    add(outside[outside["order_id"].isin(raw_ids)], "PAYMENT_FOR_REJECTED_ORDER", "amount_paid")
    add(outside[~outside["order_id"].isin(raw_ids)], "ORPHAN_PAYMENT_NO_SUCH_ORDER", "amount_paid")

    recon = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=cols)
    for c in ["order_amount", "amount_paid", "unreconciled_amount"]:
        recon[c] = pd.to_numeric(recon[c])
    recon = recon.sort_values(["reason", "order_id"]).reset_index(drop=True)
    path = work / "t_reconciliation.pkl"
    recon.to_pickle(path)
    by_reason = recon["reason"].value_counts().to_dict()
    total = float(recon["unreconciled_amount"].sum())
    log.info("reconcile: %s mismatches %s, unreconciled amount Rs %.2f", len(recon), by_reason, total)
    return {"path": str(path), "rows": int(len(recon)), "by_reason": by_reason, "unreconciled_amount": round(total, 2)}


@task
def load(transformed: dict, recon: dict, validation: dict, **context):
    """Write everything to DuckDB in ONE transaction (all or nothing, safe to re-run)."""
    run_id = context["run_id"]
    work = _work_dir(run_id)
    frames = {
        "etl_fact_orders": pd.read_pickle(transformed["paths"]["fact"]),
        "etl_dim_customer": pd.read_pickle(transformed["paths"]["customer"]),
        "etl_dim_product": pd.read_pickle(transformed["paths"]["product"]),
        "etl_dim_store": pd.read_pickle(transformed["paths"]["store"]),
        "etl_payment_reconciliation": pd.read_pickle(recon["path"]),
    }
    loaded_at = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)
    quarantine_rows = pd.read_pickle(work / "quarantine_rows.pkl")

    con = _connect()
    try:
        con.execute("BEGIN TRANSACTION")
        for name, df in frames.items():
            df = df.copy()
            df["_run_id"] = run_id
            df["_loaded_at"] = loaded_at
            con.register("_incoming", df)
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _incoming")
            con.unregister("_incoming")
        con.execute(
            """CREATE TABLE IF NOT EXISTS etl_quarantine_rows (
                   source_file VARCHAR, row_key VARCHAR, source_row BIGINT, failure_reason VARCHAR,
                   failed_checks VARCHAR, run_id VARCHAR)"""
        )
        con.execute("DELETE FROM etl_quarantine_rows WHERE run_id = ?", [run_id])
        con.register("_q", quarantine_rows)
        con.execute("INSERT INTO etl_quarantine_rows SELECT * FROM _q")
        con.unregister("_q")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    rows = {name: int(len(df)) for name, df in frames.items()}
    rows["etl_quarantine_rows"] = int(len(quarantine_rows))
    log.info("load: %s", rows)
    return {"rows": rows, "rows_loaded": rows["etl_fact_orders"]}


@task
def report(load_result: dict, **context):
    """Build the two report tables and print them in the task log."""
    con = _connect()
    try:
        con.execute(
            """CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
               SELECT order_date, zone, store_id, store_name,
                      round(sum(net_revenue), 2) AS net_revenue,
                      count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders
               FROM etl_fact_orders
               GROUP BY order_date, zone, store_id, store_name
               ORDER BY order_date, zone, store_id"""
        )
        con.execute(
            """CREATE OR REPLACE TABLE etl_category_margin AS
               SELECT coalesce(category, 'Unknown') AS category,
                      round(sum(net_revenue), 2) AS revenue,
                      round(sum(gross_margin), 2) AS gross_margin,
                      round(100.0 * sum(gross_margin) / nullif(sum(net_revenue) FILTER (WHERE gross_margin IS NOT NULL), 0), 1) AS margin_pct,
                      count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
                      count(*) FILTER (WHERE order_status = 'DELIVERED' AND gross_margin IS NULL) AS orders_without_cost
               FROM etl_fact_orders
               GROUP BY 1
               ORDER BY revenue DESC"""
        )
        zone = con.execute(
            """SELECT zone, round(sum(net_revenue), 2) AS net_revenue, count(DISTINCT store_id) AS stores
               FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC"""
        ).df()
        cat = con.execute("SELECT * FROM etl_category_margin").df()
    finally:
        con.close()
    log.info("NET REVENUE BY ZONE (all days)\n%s", zone.to_string(index=False))
    log.info("REVENUE AND GROSS MARGIN BY CATEGORY\n%s", cat.to_string(index=False))
    return {"zones": int(len(zone)), "categories": int(len(cat))}


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
    validation, loaded, rep = pull("validate"), pull("load"), pull("report")
    status = "SUCCESS" if (rep and loaded and all(extracts)) else "FAILED"
    dag_run = context.get("dag_run")
    start = getattr(dag_run, "start_date", None) or pendulum.now("Asia/Kolkata")
    start = pd.Timestamp(start).tz_convert("Asia/Kolkata").tz_localize(None)
    end = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)
    row = pd.DataFrame([{
        "run_id": run_id, "dag_id": DAG_ID, "start_time": start, "end_time": end,
        "rows_extracted": int(rows_extracted),
        "rows_loaded": int(loaded["rows_loaded"]) if loaded else 0,
        "rows_quarantined": int(validation["rows_quarantined"]) if validation else 0,
        "status": status,
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
    description="NammaMart ETL: validate in pandas, quarantine bad rows, load a clean star schema to DuckDB",
    schedule="0 9 * * 1-6",                                   # 9:00 AM IST, Monday to Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,                                            # we want today's numbers, not a flood of old runs
    max_active_runs=1,                                        # DuckDB allows one writer at a time
    default_args={
        "owner": OWNER,
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=15),
    },
    params={"load_increment": Param(False, type="boolean",
                                    description="Also load orders_*_increment.csv on top of orders.csv")},
    tags=["nammamart", "etl", OWNER],
    doc_md=__doc__,
)
def nammamart_etl():
    extracts = [extract.override(task_id=f"extract_{t}")(t) for t in TABLES]   # 5 tasks run in parallel
    validation = validate(extracts)
    transformed = transform(validation)
    recon = reconcile(transformed)
    loaded = load(transformed, recon, validation)
    reported = report(loaded)
    audit_task = audit()
    [*extracts, validation, transformed, recon, loaded, reported] >> audit_task


nammamart_etl()
