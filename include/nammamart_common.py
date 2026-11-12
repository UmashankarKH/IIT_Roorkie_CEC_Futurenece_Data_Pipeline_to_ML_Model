"""Shared helpers for the NammaMart ETL and ELT DAGs: paths, DuckDB access, masking, audit tables."""
from __future__ import annotations

import os
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

OWNER = "sbonagiri"
IST = ZoneInfo("Asia/Kolkata")

INCLUDE_DIR = Path(__file__).resolve().parent
SQL_DIR = INCLUDE_DIR / "sql"
WAREHOUSE_DB = Path(os.environ.get("NAMMAMART_WAREHOUSE", INCLUDE_DIR / f"warehouse_{OWNER}.duckdb"))
DEFAULT_DATA_DIR = Path(os.environ.get("NAMMAMART_DATA_DIR", INCLUDE_DIR / "data" / "nammamart"))
QUARANTINE_DIR = Path(os.environ.get("NAMMAMART_QUARANTINE_DIR", INCLUDE_DIR / "quarantine" / OWNER))
STAGE_DIR = Path(os.environ.get("NAMMAMART_STAGE_DIR", INCLUDE_DIR / "staging" / OWNER))

BASE_FILES = ["orders", "payments", "customers", "products", "stores"]
INCREMENT_FILE = "orders_2026-10-01_increment"
# Batch label orders the files: a later batch may overwrite an earlier one, never the reverse.
BATCH_OF = {"orders": "2026-09-30", INCREMENT_FILE: "2026-10-01"}

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
EXPECTED_COLUMNS[INCREMENT_FILE] = EXPECTED_COLUMNS["orders"]

VALID_STATUS = ("DELIVERED", "CANCELLED", "RETURNED")
VALID_PAYMENT_MODE = ("UPI", "Card", "Cash on Delivery", "Wallet")
VALID_PAYMENT_STATUS = ("SUCCESS", "REFUNDED", "FAILED")
PAYMENT_MODE_MAP = {"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
                    "cod": "Cash on Delivery", "wallet": "Wallet"}
EXPECTED_PAYMENT_STATUS = {"DELIVERED": "SUCCESS", "RETURNED": "REFUNDED"}

DDL_AUDIT = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
);
"""


def today_ist(as_of: str | None = None) -> date:
    """Reference date for timeliness checks; as_of overrides it for tests and experiments."""
    as_of = as_of or os.environ.get("NAMMAMART_AS_OF")
    return date.fromisoformat(as_of) if as_of else datetime.now(IST).date()


def safe_run_id(run_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in run_id)


def data_dir(override: str | None = None) -> Path:
    return Path(override) if override else DEFAULT_DATA_DIR


def csv_path(name: str, override: str | None = None) -> Path:
    return data_dir(override) / f"{name}.csv"


def connect(retries: int = 30, wait_s: float = 2.0) -> duckdb.DuckDBPyConnection:
    """DuckDB allows one writer process; retry so the ETL and ELT DAGs can share the file at 9 AM."""
    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(retries):
        try:
            con = duckdb.connect(str(WAREHOUSE_DB))
            con.execute(DDL_AUDIT)
            return con
        except duckdb.IOException:
            if attempt == retries - 1:
                raise
            time.sleep(wait_s)
    raise RuntimeError("unreachable")


def check_schema(name: str, override: str | None = None) -> int:
    """Fail loudly if a file is missing or its header differs from the contract; returns the row count."""
    path = csv_path(name, override)
    if not path.exists():
        raise FileNotFoundError(f"Source file missing: {path}")
    with path.open(encoding="utf-8") as fh:
        header = [c.strip() for c in fh.readline().rstrip("\n").split(",")]
        rows = sum(1 for _ in fh)
    expected = EXPECTED_COLUMNS[name]
    missing, extra = [c for c in expected if c not in header], [c for c in header if c not in expected]
    if missing or extra or header != expected:
        raise ValueError(f"Schema change in {path.name}: missing columns {missing}, unexpected columns {extra}, "
                         f"column order {'changed' if not (missing or extra) else 'n/a'}")
    return rows


# ---- PII masking -----------------------------------------------------------------------------------------

def mask_email(email) -> str | None:
    """priya.hegde@example.com -> pr***@example.com"""
    if email is None or str(email).strip() == "" or "@" not in str(email):
        return None
    local, _, domain = str(email).strip().partition("@")
    return f"{local[:2]}***@{domain}"


def mask_phone(phone) -> str | None:
    """9774349021 -> 97******21"""
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if len(digits) != 10:
        return None
    return f"{digits[:2]}{'*' * 6}{digits[-2:]}"


# ---- audit writers ---------------------------------------------------------------------------------------

def write_dq_results(con, run_id: str, dag_id: str, results: list[dict]) -> None:
    now = datetime.now(IST).replace(tzinfo=None)
    rows = [(run_id, dag_id, r["check_name"], r["dimension"], r["severity"], int(r["rows_checked"]),
             int(r["rows_failed"]), r["status"], now) for r in results]
    if rows:
        con.executemany("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)", rows)


def previous_rows_checked(con, dag_id: str, check_name: str, run_id: str) -> int | None:
    row = con.execute(
        "SELECT rows_checked FROM dq_results WHERE dag_id=? AND check_name=? AND run_id<>? "
        "ORDER BY checked_at DESC LIMIT 1", [dag_id, check_name, run_id]).fetchone()
    return row[0] if row else None


def volume_result(con, dag_id: str, run_id: str, dataset: str, rows_now: int, drop_threshold: float = 0.9) -> dict:
    """Warning when row volume fell by more than 90% versus the previous run."""
    name = f"{dataset}.volume_vs_previous_run"
    prev = previous_rows_checked(con, dag_id, name, run_id)
    failed = 1 if prev and rows_now < prev * (1 - drop_threshold) else 0
    return {"check_name": name, "dimension": "Completeness", "severity": "Warning",
            "rows_checked": rows_now, "rows_failed": failed, "status": "WARN" if failed else "PASS"}


def dag_run_status(ti, final_task: str) -> str:
    """SUCCESS only if no task of this run failed; falls back to 'did the final task produce output'."""
    try:
        states = ti.get_task_states(dag_id=ti.dag_id, run_ids=[ti.run_id])[ti.run_id]
        bad = [t for t, s in states.items() if getattr(s, "value", s) in ("failed", "upstream_failed")
               and t != ti.task_id]
        return "FAILED" if bad else "SUCCESS"
    except Exception:
        return "SUCCESS" if ti.xcom_pull(task_ids=final_task, default=None) is not None else "FAILED"


def write_run_audit(con, run_id: str, dag_id: str, start_time, rows_extracted, rows_loaded, rows_quarantined,
                    status: str) -> None:
    end = datetime.now(IST).replace(tzinfo=None)
    start = start_time.astimezone(IST).replace(tzinfo=None) if hasattr(start_time, "astimezone") else start_time
    con.execute("DELETE FROM pipeline_runs WHERE run_id=? AND dag_id=?", [run_id, dag_id])
    con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
                [run_id, dag_id, start, end, rows_extracted, rows_loaded, rows_quarantined, status])
