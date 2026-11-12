"""
Shared helpers for the NammaMart ETL and ELT DAGs.

Everything here is plain Python (no Airflow imports) so the same code can be run
from a DAG task, from `scripts/run_local.py`, or from a unit test.
"""
import os
import time
import urllib.request
import json
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

OWNER = "mohanrajs"  # <your_name> in file names: dags/nammamart_etl_mohanrajs.py, warehouse_mohanrajs.duckdb ...

INCLUDE_DIR = Path(os.environ.get("NAMMAMART_INCLUDE_DIR", Path(__file__).resolve().parent))
DATA_DIR = Path(os.environ.get("NAMMAMART_DATA_DIR", INCLUDE_DIR / "data" / "nammamart"))
WAREHOUSE_DB = Path(os.environ.get("NAMMAMART_WAREHOUSE", INCLUDE_DIR / f"warehouse_{OWNER}.duckdb"))
QUARANTINE_DIR = Path(os.environ.get("NAMMAMART_QUARANTINE_DIR", INCLUDE_DIR / "quarantine" / OWNER))
WORK_DIR = Path(os.environ.get("NAMMAMART_WORK_DIR", INCLUDE_DIR / "work" / OWNER))
SQL_DIR = INCLUDE_DIR / "sql" / "elt"

# The five daily/master files and the next-day incremental feed.
FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_FILE = "orders_2026-10-01_increment.csv"

# Schema contract: a missing column must fail the run loudly (observability experiment 6).
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
EXPECTED_COLUMNS["orders_increment"] = EXPECTED_COLUMNS["orders"]

# "Now" for timeliness checks. The September export is a fixed historical batch, so the default is the
# morning after the increment file. Set Airflow Variable nammamart_as_of = "now" in production.
DEFAULT_AS_OF = "2026-10-02 09:00:00"


def resolve_as_of(value=None):
    value = value or DEFAULT_AS_OF
    if str(value).lower() == "now":
        return pd.Timestamp.now().floor("s")
    return pd.Timestamp(value)


def file_path(name, data_dir=None):
    fname = INCREMENT_FILE if name == "orders_increment" else FILES[name]
    return Path(data_dir or DATA_DIR) / fname


def read_csv_raw(name, data_dir=None):
    """Read a CSV with every column as text and no NA inference: what you see is what was received."""
    path = file_path(name, data_dir)
    if not path.exists():
        raise FileNotFoundError(f"Source file missing: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    check_schema(name, df.columns.tolist())
    df.insert(0, "_row_num", range(2, len(df) + 2))  # line number in the source file (header = line 1)
    return df


def check_schema(name, columns):
    expected = EXPECTED_COLUMNS[name]
    missing = [c for c in expected if c not in columns]
    extra = [c for c in columns if c not in expected]
    if missing:
        raise ValueError(f"Schema drift in {name}: missing columns {missing}; unexpected columns {extra}")


def connect(read_only=False):
    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    # DuckDB allows one writer process. The ETL and ELT DAGs share this file and both start at 09:00, so wait
    # for the other DAG's task to release the lock instead of failing the task.
    for attempt in range(60):
        try:
            return duckdb.connect(str(WAREHOUSE_DB), read_only=read_only)
        except duckdb.IOException:
            if attempt == 59:
                raise
            time.sleep(1)


DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP
)"""
PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP, rows_extracted BIGINT,
    rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
)"""


def write_dq_results(con, run_id, dag_id, results):
    """results: list of dicts with check_name, dimension, severity, rows_checked, rows_failed."""
    con.execute(DQ_RESULTS_DDL)
    # Idempotent: re-running the same run_id replaces its rows rather than duplicating them.
    con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    now = datetime.now()
    for r in results:
        status = "PASS" if r["rows_failed"] == 0 else ("FAIL" if r["severity"] == "Critical" else "WARN")
        con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)",
                    [run_id, dag_id, r["check_name"], r["dimension"], r["severity"],
                     int(r["rows_checked"]), int(r["rows_failed"]), status, now])


def write_pipeline_run(con, row):
    con.execute(PIPELINE_RUNS_DDL)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [row["run_id"], row["dag_id"]])
    con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
                [row["run_id"], row["dag_id"], row["start_time"], row["end_time"],
                 row["rows_extracted"], row["rows_loaded"], row["rows_quarantined"], row["status"]])


def previous_orders_volume(con, dag_id, run_id):
    """Peak number of orders rows seen by earlier runs of this DAG (from dq_results), or None."""
    try:
        r = con.execute("SELECT max(rows_checked) FROM dq_results WHERE dag_id = ? AND run_id <> ? "
                        "AND check_name = 'orders_order_id_not_null'", [dag_id, run_id]).fetchone()
    except duckdb.CatalogException:
        return None
    return r[0] if r else None


def volume_check(rows_now, rows_before, dag_id, label="orders"):
    """Warning-level check: more than 90% fewer rows than the previous successful run."""
    failed = 1 if (rows_before and rows_now < 0.1 * rows_before) else 0
    return {"check_name": f"{label}_volume_vs_previous_run", "dimension": "Completeness", "severity": "Warning",
            "rows_checked": 1, "rows_failed": failed}


def slack_alert(context):
    """on_failure_callback: posts to the webhook stored in the Airflow Variable SLACK_WEBHOOK_URL, if set."""
    try:
        from airflow.sdk import Variable
    except ImportError:  # Airflow 2
        from airflow.models import Variable
    try:
        url = Variable.get("SLACK_WEBHOOK_URL", default=None)
    except Exception:
        url = None
    ti = context.get("task_instance")
    msg = f":red_circle: NammaMart pipeline failure: dag={ti.dag_id} task={ti.task_id} run={ti.run_id}"
    print(msg)
    if not url:
        return
    req = urllib.request.Request(url, data=json.dumps({"text": msg}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:  # an alert failure must never mask the original failure
        print(f"Slack alert failed: {e}")
