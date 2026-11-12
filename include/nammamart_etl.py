"""
NammaMart ETL — pipeline logic (Part A)
=======================================

Extract -> Validate -> Transform -> Load -> Reconcile -> Report -> Audit

All the work lives here as plain functions, so it can be tested without
Airflow. The DAG (dags/nammamart_etl_anaz.py) only wires these steps together.

Design choices (explained in SUBMISSION.md, Part A):
  * Data quality rules come from include/dq_rules.py, the same rules the ELT
    pipeline uses. Critical failures are quarantined; Warnings load and are
    recorded in a dq_warnings column.
  * Tasks hand data to each other through small pickle files in
    include/tmp/<learner>/<run_id>/, never through XCom (XCom is for metadata).
  * Only one task at a time writes to DuckDB. DuckDB allows a single writer per
    file, so connect() waits and retries if the ELT DAG holds the lock.
  * Idempotent load: dimensions are fully replaced each run; the fact table is
    keyed on order_id and loaded per source file (delete that file's rows,
    then upsert). A newer source file (the increment) always wins over an older
    one, so re-running September can never undo a later RETURNED correction.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

try:  # inside Airflow / from the project root
    from include import observability
    from include.dq_rules import FILES, apply_flags, build_context, dq_results, read_raw, standardise
except ImportError:  # when run from inside include/
    import observability
    from dq_rules import FILES, apply_flags, build_context, dq_results, read_raw, standardise

LEARNER = "anaz"
INCLUDE = Path(__file__).resolve().parent
DATA_DIR = INCLUDE / "data" / "nammamart"
WAREHOUSE = INCLUDE / f"warehouse_{LEARNER}.duckdb"
QUARANTINE_DIR = INCLUDE / "quarantine" / LEARNER
WORK_DIR = INCLUDE / "tmp" / LEARNER

MASTER_FILES = ["orders", "payments", "customers", "products", "stores"]
INCREMENT = "orders_increment"
ORDER_FILES = ["orders", INCREMENT]

EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
               "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
               "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                  "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                 "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date",
               "manager_name", "capacity_orders_per_day"],
}
EXPECTED_COLUMNS[INCREMENT] = EXPECTED_COLUMNS["orders"]

AMOUNT_TOLERANCE = 0.01
BOOL_MAP = {"y": True, "yes": True, "true": True, "t": True, "1": True,
            "n": False, "no": False, "false": False, "f": False, "0": False}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def safe(run_id: str) -> str:
    """run_ids contain ':' and '+'; make them safe for file names."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def run_dir(run_id: str, work: Path = WORK_DIR) -> Path:
    p = Path(work) / safe(run_id)
    p.mkdir(parents=True, exist_ok=True)
    return p


def source_date(name: str, as_of, order_ts: pd.Series | None = None) -> pd.Timestamp:
    """Business date a file describes: the date in its name; otherwise the date of its latest
    valid order; otherwise the day before the run date. It never depends on as_of when the
    data can answer, so running with a different as_of cannot make an old file look newer."""
    m = re.search(r"\d{4}-\d{2}-\d{2}", FILES[name])
    if m:
        return pd.Timestamp(m.group(0))
    if order_ts is not None and order_ts.notna().any():
        return order_ts.max().normalize()
    return pd.Timestamp(as_of) - pd.Timedelta(days=1)


def connect(path: Path | str = WAREHOUSE, timeout_s: int = 300):
    """Open DuckDB, waiting if another process (e.g. the ELT DAG) holds the write lock."""
    import duckdb

    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return duckdb.connect(str(path))
        except duckdb.IOException as e:
            if "lock" not in str(e).lower() or time.monotonic() > deadline:
                raise
            print(f"Warehouse is locked by another process; retrying in 5 s ({e})")
            time.sleep(5)


def mask_email(v):
    """priya.s@example.com -> pr***@example.com ; malformed -> ***"""
    if v is None or pd.isna(v):
        return None
    v = str(v)
    if "@" not in v:
        return "***"
    local, domain = v.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(digits):
    """9876543221 -> 98******21 (works on the normalised digits)."""
    if digits is None or pd.isna(digits) or not str(digits):
        return None
    d = str(digits)
    return "*" * len(d) if len(d) < 6 else d[:2] + "*" * (len(d) - 4) + d[-2:]


def mask_name(v):
    """Priya Sharma -> P*** S***"""
    if v is None or pd.isna(v):
        return None
    return " ".join(part[0] + "***" for part in str(v).split())


def _bool(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower().map(BOOL_MAP).astype("boolean")


def _print_table(title: str, df: pd.DataFrame, max_rows: int = 60) -> None:
    with pd.option_context("display.width", 200, "display.max_columns", 30):
        print(f"\n=== {title} ===\n{df.head(max_rows).to_string(index=False)}")
        if len(df) > max_rows:
            print(f"... {len(df) - max_rows} more rows")


# ---------------------------------------------------------------------------
# 1. Extract
# ---------------------------------------------------------------------------
def extract(name: str, run_id: str, data_dir: Path = DATA_DIR, work: Path = WORK_DIR) -> dict:
    """Read one CSV exactly as received (all text) and check its schema."""
    path = Path(data_dir) / FILES[name]
    if not path.exists():
        raise FileNotFoundError(f"Source file missing: {path}")
    raw = read_raw(path)
    cols = [c for c in raw.columns if c != "_row"]
    missing = [c for c in EXPECTED_COLUMNS[name] if c not in cols]
    extra = [c for c in cols if c not in EXPECTED_COLUMNS[name]]
    if missing:  # fail loudly: never load a file whose meaning has changed
        raise ValueError(f"Schema change in {FILES[name]}: missing columns {missing}; "
                         f"unexpected columns {extra}")
    if extra:
        print(f"WARNING: {FILES[name]} has unexpected columns {extra}; they are ignored")
    out = run_dir(run_id, work) / f"{name}_raw.pkl"
    raw.to_pickle(out)
    print(f"Extracted {len(raw):,} rows from {FILES[name]}")
    return {"name": name, "source_file": FILES[name], "path": str(out), "rows": len(raw)}


# ---------------------------------------------------------------------------
# 2. Validate (+ quarantine)
# ---------------------------------------------------------------------------
def validate(name: str, raw_infos: dict, as_of: str, run_id: str, dag_id: str,
             quarantine_dir: Path = QUARANTINE_DIR, work: Path = WORK_DIR) -> dict | None:
    """Apply the shared DQ rules to one file; split it into passed and quarantined rows."""
    if raw_infos.get(name) is None:
        print(f"{name}: not extracted in this run; nothing to validate")
        return None
    std = {n: standardise(n, pd.read_pickle(raw_infos[n]["path"])) for n in MASTER_FILES}
    ctx = build_context(std, as_of)
    raw = pd.read_pickle(raw_infos[name]["path"])
    s = std[name] if name in std else standardise(name, raw)

    flagged, masks, dataset = apply_flags(name, s, ctx)
    failed = (flagged["dq_status"] == "FAIL").to_numpy()
    d = run_dir(run_id, work)

    # Quarantine: the row exactly as received, plus why it was rejected
    qfile = None
    if failed.any():
        quarantine_dir = Path(quarantine_dir)
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        q = raw.loc[failed].copy()
        q.insert(0, "failure_reason", flagged.loc[failed, "failure_reason"].to_numpy())
        stem = FILES[name].removesuffix(".csv")
        qfile = quarantine_dir / f"{stem}_failed_{safe(run_id)}.csv"
        q.to_csv(qfile, index=False)

    flagged.to_pickle(d / f"{name}_flagged.pkl")
    flagged.loc[~failed].to_pickle(d / f"{name}_passed.pkl")
    dq = dq_results(name, s, masks, dataset, run_id, dag_id)
    dq.to_pickle(d / f"{name}_dq.pkl")

    n_warn = int((flagged["dq_status"] == "WARN").sum())
    print(f"{FILES[name]}: {len(raw):,} rows -> {int((~failed).sum()):,} passed "
          f"({n_warn:,} with warnings), {int(failed.sum()):,} quarantined")
    if qfile:
        print(f"Quarantine file: {qfile}")
    problems = dq[dq["rows_failed"] > 0][["check_name", "severity", "rows_failed", "status"]]
    if len(problems):
        _print_table(f"Checks with failures in {FILES[name]}", problems)
    return {"name": name, "source_file": FILES[name], "rows_in": len(raw),
            "rows_passed": int((~failed).sum()), "rows_quarantined": int(failed.sum()),
            "rows_warned": n_warn, "quarantine_file": str(qfile) if qfile else None,
            "flagged": str(d / f"{name}_flagged.pkl"), "passed": str(d / f"{name}_passed.pkl"),
            "dq": str(d / f"{name}_dq.pkl")}


# ---------------------------------------------------------------------------
# 3. Transform: repairs, masking, dimensions, fact
# ---------------------------------------------------------------------------
def build_dim_customer(c: pd.DataFrame, as_of, run_id: str) -> pd.DataFrame:
    next_day = pd.Timestamp(as_of) + pd.Timedelta(days=1)
    return pd.DataFrame({
        "customer_id": c["customer_id"],
        "customer_name_masked": c["full_name"].map(mask_name),
        "email_masked": c["email"].map(mask_email),
        "phone_masked": c["phone_norm"].map(mask_phone),
        "city": c["city"],
        "area": c["area"],
        "pincode": c["pincode"],
        "signup_date": c["signup_date"].where(c["signup_date"] < next_day),  # REPAIR: future -> NULL
        "loyalty_tier": c["loyalty_tier"].fillna("UNKNOWN"),                  # REPAIR
        "is_active": _bool(c["is_active"]),
        "dq_warnings": c["warning_reason"],
        "_run_id": run_id,
    })


def build_dim_product(p: pd.DataFrame, run_id: str) -> pd.DataFrame:
    mrp_ok = (p["mrp"] > 0).fillna(False)
    cost_ok = ((p["cost_price"] > 0) & ~(mrp_ok & (p["cost_price"] > p["mrp"]))).fillna(False)
    return pd.DataFrame({
        "product_id": p["product_id"],
        "product_name": p["product_name"],
        "category": p["category"].fillna("UNKNOWN"),          # REPAIR
        "sub_category": p["sub_category"],
        "brand": p["brand"],
        "mrp": p["mrp"].where(mrp_ok),                        # REPAIR: <= 0 -> NULL
        "cost_price": p["cost_price"].where(cost_ok),         # REPAIR: <= 0 or > mrp -> NULL
        "unit": p["unit"],
        "is_perishable": _bool(p["is_perishable"]),
        "launch_date": p["launch_date"],
        "dq_warnings": p["warning_reason"],
        "_run_id": run_id,
    })


def build_dim_store(s: pd.DataFrame, run_id: str) -> pd.DataFrame:
    cap = s["capacity_orders_per_day"]
    return pd.DataFrame({  # manager_name (personal data) is not needed for reporting: dropped
        "store_id": s["store_id"],
        "store_name": s["store_name"],
        "zone": s["zone"].fillna("UNKNOWN"),                  # REPAIR
        "area": s["area"],
        "pincode": s["pincode"],
        "open_date": s["open_date"],
        "capacity_orders_per_day": cap.where((cap > 0).fillna(False)),  # REPAIR: <= 0 -> NULL
        "dq_warnings": s["warning_reason"],
        "_run_id": run_id,
    })


def build_fact(o: pd.DataFrame, dim_c: pd.DataFrame, dim_p: pd.DataFrame, dim_s: pd.DataFrame,
               name: str, as_of, run_id: str) -> pd.DataFrame:
    """Join clean orders to the dimensions and derive the reporting measures."""
    n = len(o)
    # validate='many_to_one' raises if a dimension still has duplicate keys:
    # a duplicated customer would otherwise double-count every one of their orders.
    j = (o.merge(dim_c[["customer_id"]], on="customer_id", how="left", validate="many_to_one", indicator="_c")
          .merge(dim_p[["product_id", "cost_price"]], on="product_id", how="left", validate="many_to_one")
          .merge(dim_s[["store_id", "zone"]], on="store_id", how="left", validate="many_to_one"))
    if len(j) != n:
        raise RuntimeError(f"Join changed the row count ({n} -> {len(j)}): duplicate dimension keys")
    if (j["_c"] != "both").any() or j["zone"].isna().any():
        raise RuntimeError("Some clean orders did not match a dimension row; check the FK rules")

    delivered = j["order_status"].eq("DELIVERED").fillna(False).astype(bool)
    cost_amount = (j["quantity"] * j["cost_price"]).where(delivered)
    net_revenue = j["order_amount"].where(delivered, 0.0)
    return pd.DataFrame({
        "order_id": j["order_id"],
        "order_date": j["order_ts"].dt.normalize(),
        "order_ts": j["order_ts"],
        "customer_id": j["customer_id"],
        "product_id": j["product_id"],
        "store_id": j["store_id"],
        "quantity": j["quantity"].astype("Int64"),
        "unit_price": j["unit_price"],
        "discount_pct": j["discount_pct"].fillna(0),
        "order_amount": j["order_amount"],
        "net_revenue": net_revenue,                                   # DELIVERED only, after discount
        "cost_amount": cost_amount,                                   # NULL when cost_price unknown
        "gross_margin": (net_revenue - cost_amount).where(delivered),
        "payment_mode": j["payment_mode"],
        "order_status": j["order_status"],
        "channel": j["channel"],
        "delivery_minutes": j["delivery_minutes"],
        "within_15_min": (j["delivery_minutes"] <= 15).where(delivered).astype("boolean"),
        "dq_warnings": j["warning_reason"],
        "_source_file": FILES[name],
        "_source_date": source_date(name, as_of, j["order_ts"]),
        "_run_id": run_id,
        "_loaded_at": pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s"),
    })


def transform(validated: dict, as_of: str, run_id: str, work: Path = WORK_DIR) -> dict:
    d = run_dir(run_id, work)
    passed = {n: pd.read_pickle(v["passed"]) for n, v in validated.items() if v}
    dim_c = build_dim_customer(passed["customers"], as_of, run_id)
    dim_p = build_dim_product(passed["products"], run_id)
    dim_s = build_dim_store(passed["stores"], run_id)
    out = {"run_id": run_id, "facts": []}
    for key, df in [("dim_customer", dim_c), ("dim_product", dim_p), ("dim_store", dim_s)]:
        df.to_pickle(d / f"{key}.pkl")
        out[key] = {"path": str(d / f"{key}.pkl"), "rows": len(df)}
    for name in ORDER_FILES:  # base file first, then the increment
        if name in passed:
            fact = build_fact(passed[name], dim_c, dim_p, dim_s, name, as_of, run_id)
            fact.to_pickle(d / f"fact_{name}.pkl")
            out["facts"].append({"name": name, "source_file": FILES[name],
                                 "path": str(d / f"fact_{name}.pkl"), "rows": len(fact)})
    print(f"Dimensions: {len(dim_c)} customers, {len(dim_p)} products, {len(dim_s)} stores")
    for f in out["facts"]:
        print(f"Fact rows from {f['source_file']}: {f['rows']:,}")
    return out


# ---------------------------------------------------------------------------
# 4. Load (idempotent)
# ---------------------------------------------------------------------------
FACT_DDL = """
CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id VARCHAR PRIMARY KEY,
    order_date DATE, order_ts TIMESTAMP,
    customer_id VARCHAR, product_id VARCHAR, store_id VARCHAR,
    quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    net_revenue DOUBLE, cost_amount DOUBLE, gross_margin DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR,
    delivery_minutes DOUBLE, within_15_min BOOLEAN, dq_warnings VARCHAR,
    _source_file VARCHAR, _source_date DATE, _run_id VARCHAR, _loaded_at TIMESTAMP
)"""
DQ_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP
)"""
RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
)"""
FACT_COLUMNS = ["order_id", "order_date", "order_ts", "customer_id", "product_id", "store_id",
                "quantity", "unit_price", "discount_pct", "order_amount", "net_revenue",
                "cost_amount", "gross_margin", "payment_mode", "order_status", "channel",
                "delivery_minutes", "within_15_min", "dq_warnings", "_source_file",
                "_source_date", "_run_id", "_loaded_at"]


def load(transformed: dict, validated: dict, dag_id: str, warehouse: Path = WAREHOUSE) -> dict:
    run_id = transformed["run_id"]
    con = connect(warehouse)
    try:
        # Dimensions: full replace (always the latest master data)
        for key, date_cols in [("dim_customer", ["signup_date"]), ("dim_product", ["launch_date"]),
                               ("dim_store", ["open_date"])]:
            df = pd.read_pickle(transformed[key]["path"])
            con.register("incoming", df)
            casts = ", ".join(f"CAST({c} AS DATE) AS {c}" for c in date_cols)
            con.execute(f"CREATE OR REPLACE TABLE etl_{key} AS "
                        f"SELECT * REPLACE ({casts}) FROM incoming")
            con.unregister("incoming")

        # Fact: per source file, delete that file's rows then upsert; newer files win
        con.execute(FACT_DDL)
        before = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
        per_file = []
        set_cols = ", ".join(f"{c} = EXCLUDED.{c}" for c in FACT_COLUMNS if c != "order_id")
        for f in transformed["facts"]:
            df = pd.read_pickle(f["path"])
            con.register("incoming", df[FACT_COLUMNS])
            # Classify incoming rows against what the warehouse holds now (for the run log)
            existing = con.execute(
                "SELECT e._source_file = i._source_file AS same_file, "
                "CAST(i._source_date AS DATE) >= e._source_date AS incoming_newer "
                "FROM etl_fact_orders e JOIN incoming i USING (order_id)").df()
            con.execute("DELETE FROM etl_fact_orders WHERE _source_file = ?", [f["source_file"]])
            con.execute(f"""
                INSERT INTO etl_fact_orders
                SELECT * REPLACE (CAST(order_date AS DATE) AS order_date,
                                  CAST(_source_date AS DATE) AS _source_date) FROM incoming
                ON CONFLICT (order_id) DO UPDATE SET {set_cols}
                WHERE EXCLUDED._source_date >= etl_fact_orders._source_date""")
            con.unregister("incoming")
            same = existing["same_file"].astype(bool) if len(existing) else pd.Series(dtype=bool)
            newer = existing["incoming_newer"].astype(bool) if len(existing) else pd.Series(dtype=bool)
            per_file.append({
                "source_file": f["source_file"], "rows": len(df),
                "inserted_new": len(df) - len(existing),         # order_id not in the warehouse before
                "rewritten_same_file": int(same.sum()),          # re-run of the same file: no change
                "updated_by_newer_file": int((~same & newer).sum()),   # e.g. DELIVERED -> RETURNED
                "kept_newer_version": int((~same & ~newer).sum()),     # older file can't undo a correction
            })
        after = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]

        # dq_results: replace this run's rows (safe if a task is cleared and re-run)
        con.execute(DQ_DDL)
        con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        for v in validated.values():
            if v:
                con.register("dq", pd.read_pickle(v["dq"]))
                con.execute("INSERT INTO dq_results SELECT run_id, dag_id, check_name, dimension, "
                            "severity, rows_checked, rows_failed, status, checked_at FROM dq")
                con.unregister("dq")
    finally:
        con.close()

    _print_table("Fact load per source file", pd.DataFrame(per_file))
    print(f"etl_fact_orders: {before:,} rows before -> {after:,} rows after")
    return {"run_id": run_id, "fact_rows_before": int(before), "fact_rows_after": int(after),
            "rows_loaded": int(sum(p["rows"] for p in per_file)), "per_file": per_file}


# ---------------------------------------------------------------------------
# 5. Reconcile orders with payments
# ---------------------------------------------------------------------------
def build_reconciliation(fact: pd.DataFrame, payments: pd.DataFrame, source_order_ids: set,
                         run_id: str) -> pd.DataFrame:
    """One row per order/payment mismatch, with a reason. `payments` = standardised payments."""
    pay = payments[payments["payment_id"].notna()].drop_duplicates("payment_id", keep="first")
    pay = pay.assign(payment_mode=pay["payment_mode"].fillna("UNKNOWN"))
    succ = pay[pay["payment_status"].eq("SUCCESS").fillna(False).astype(bool)]
    s_agg = succ.groupby("order_id").agg(amount_paid=("amount_paid", "sum"),
                                         n_success=("payment_id", "count"),
                                         success_ids=("payment_id", lambda x: ", ".join(x)))
    all_agg = pay.groupby("order_id").agg(
        payment_ids=("payment_id", lambda x: ", ".join(x)),
        statuses=("payment_status", lambda x: ", ".join(sorted(set(x.dropna())))))

    f = (fact[["order_id", "order_status", "order_amount", "payment_mode"]]
         .merge(s_agg, left_on="order_id", right_index=True, how="left")
         .merge(all_agg, left_on="order_id", right_index=True, how="left"))
    f["amount_paid"] = f["amount_paid"].fillna(0.0)
    f["n_success"] = f["n_success"].fillna(0).astype(int)
    st = f["order_status"]
    has_any = f["payment_ids"].notna()
    has_refund = f["statuses"].fillna("").str.contains("REFUNDED")

    rows = []

    def add(mask, reason, difference):
        part = f.loc[mask].copy()
        part["reason"] = reason
        part["difference"] = difference.loc[mask].round(2)
        rows.append(part)

    dlv = st.eq("DELIVERED")
    add(dlv & ~has_any, "NO_PAYMENT_RECORD", -f["order_amount"])
    add(dlv & has_any & (f["n_success"] == 0), "PAYMENT_FAILED", -f["order_amount"])
    add(dlv & (f["n_success"] > 1), "MULTIPLE_SUCCESS", f["amount_paid"] - f["order_amount"])
    add(dlv & (f["n_success"] == 1) & ((f["amount_paid"] - f["order_amount"]).abs() > AMOUNT_TOLERANCE),
        "AMOUNT_MISMATCH", f["amount_paid"] - f["order_amount"])
    add(st.eq("CANCELLED") & (f["n_success"] > 0), "PAYMENT_ON_CANCELLED", f["amount_paid"])
    add(st.eq("RETURNED") & (f["n_success"] > 0) & ~has_refund, "REFUND_MISSING", f["amount_paid"])
    recon = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

    # Payments that point at no reportable order
    loose = pay[~pay["order_id"].isin(set(fact["order_id"]))].copy()
    loose["reason"] = np.where(loose["order_id"].isin(source_order_ids),
                               "PAYMENT_FOR_REJECTED_ORDER", "ORPHAN_PAYMENT")
    loose_rows = pd.DataFrame({
        "order_id": loose["order_id"], "order_status": None, "order_amount": np.nan,
        "payment_mode": loose["payment_mode"], "amount_paid": loose["amount_paid"],
        "n_success": loose["payment_status"].eq("SUCCESS").fillna(False).astype(int),
        "success_ids": None, "payment_ids": loose["payment_id"], "statuses": loose["payment_status"],
        "reason": loose["reason"], "difference": loose["amount_paid"].round(2)})

    out = pd.concat([recon, loose_rows], ignore_index=True)
    out = out[["order_id", "reason", "order_status", "order_amount", "amount_paid", "difference",
               "payment_mode", "payment_ids", "statuses"]]
    out["_run_id"] = run_id
    return out.sort_values(["reason", "order_id"]).reset_index(drop=True)


def reconcile(validated: dict, run_id: str, warehouse: Path = WAREHOUSE) -> dict:
    payments = pd.read_pickle(validated["payments"]["flagged"])
    source_ids = set()
    for n in ORDER_FILES:
        if validated.get(n):
            source_ids |= set(pd.read_pickle(validated[n]["flagged"])["order_id"].dropna())
    con = connect(warehouse)
    try:
        fact = con.execute("SELECT order_id, order_status, order_amount, payment_mode "
                           "FROM etl_fact_orders").df()
        recon = build_reconciliation(fact, payments, source_ids, run_id)
        con.register("recon", recon)
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon")
        con.unregister("recon")
    finally:
        con.close()
    summary = (recon.groupby("reason")
               .agg(rows=("reason", "size"), amount=("difference", lambda x: round(x.abs().sum(), 2)))
               .reset_index())
    _print_table("Payment reconciliation (rows and absolute amount by reason)", summary)
    print(f"Total unreconciled amount: {recon['difference'].abs().sum():,.2f}")
    return {"rows": len(recon), "unreconciled_amount": round(float(recon["difference"].abs().sum()), 2)}


# ---------------------------------------------------------------------------
# 6. Report
# ---------------------------------------------------------------------------
REPORT_SQL = {
    "etl_daily_zone_revenue": """
        SELECT f.order_date, coalesce(s.zone, 'UNKNOWN') AS zone, f.store_id, s.store_name,
               count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS net_revenue
        FROM etl_fact_orders f LEFT JOIN etl_dim_store s USING (store_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY ALL ORDER BY f.order_date, zone, f.store_id""",
    "etl_category_margin": """
        SELECT coalesce(p.category, 'UNKNOWN') AS category,
               count(*) AS delivered_orders,
               round(sum(f.net_revenue), 2) AS revenue,
               round(sum(f.cost_amount), 2) AS cost,
               round(sum(f.gross_margin), 2) AS gross_margin,
               round(100 * sum(f.gross_margin)
                     / nullif(sum(f.net_revenue) FILTER (WHERE f.gross_margin IS NOT NULL), 0), 1)
                 AS margin_pct,
               count(*) FILTER (WHERE f.cost_amount IS NULL) AS orders_without_cost
        FROM etl_fact_orders f LEFT JOIN etl_dim_product p USING (product_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY ALL ORDER BY revenue DESC""",
}


def report(warehouse: Path = WAREHOUSE) -> dict:
    con = connect(warehouse)
    try:
        for table, sql in REPORT_SQL.items():
            con.execute(f"CREATE OR REPLACE TABLE {table} AS {sql}")
        zone = con.execute("""SELECT zone, sum(delivered_orders) AS delivered_orders,
                                     round(sum(net_revenue), 2) AS net_revenue
                              FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC""").df()
        daily = con.execute("""SELECT order_date, round(sum(net_revenue), 2) AS net_revenue
                               FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY 1""").df()
        cat = con.execute("SELECT * FROM etl_category_margin").df()
        total = con.execute("SELECT round(sum(net_revenue), 2) FROM etl_fact_orders").fetchone()[0]
    finally:
        con.close()
    _print_table("Net revenue by zone (all days)", zone)
    _print_table("Net revenue by day", daily, max_rows=40)
    _print_table("Revenue and gross margin by category", cat)
    print(f"\nTotal net revenue (delivered orders): {total:,.2f}")
    return {"total_net_revenue": float(total or 0)}


# ---------------------------------------------------------------------------
# 6b. Run-level observability checks (volume, store distribution, margin)
# ---------------------------------------------------------------------------
def observe(loaded: dict, run_id: str, dag_id: str, warehouse: Path = WAREHOUSE) -> dict:
    rid = run_id.replace("'", "''")
    con = connect(warehouse)
    try:
        return observability.run_checks(
            con, run_id, dag_id, rows_loaded=int(loaded["rows_loaded"]),
            orders_sql=("SELECT store_id, order_status, net_revenue, gross_margin "
                        f"FROM etl_fact_orders WHERE _run_id = '{rid}'"),
            stores_sql="SELECT store_id FROM etl_dim_store")
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 7. Run audit
# ---------------------------------------------------------------------------
def audit(run_id: str, dag_id: str, start_time, extracted: list, validated: list,
          loaded: dict | None, finished_ok: bool, warehouse: Path = WAREHOUSE,
          status: str | None = None) -> dict:
    """status: 'success', 'failed', or 'anomaly' (pipeline finished but a critical
    run-level check says the numbers are suspect). Defaults from finished_ok."""
    row = {
        "run_id": run_id, "dag_id": dag_id,
        "start_time": pd.Timestamp(start_time).tz_convert("UTC").tz_localize(None)
        if pd.Timestamp(start_time).tzinfo else pd.Timestamp(start_time),
        "end_time": pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None).floor("s"),
        "rows_extracted": int(sum(e["rows"] for e in extracted if e)),
        "rows_loaded": int(loaded["rows_loaded"]) if loaded else 0,
        "rows_quarantined": int(sum(v["rows_quarantined"] for v in validated if v)),
        "status": status or ("success" if finished_ok else "failed"),
    }
    con = connect(warehouse)
    try:
        con.execute(RUNS_DDL)
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        con.register("r", pd.DataFrame([row]))
        con.execute("INSERT INTO pipeline_runs SELECT run_id, dag_id, start_time, end_time, rows_extracted, "
                    "rows_loaded, rows_quarantined, status FROM r")
        con.unregister("r")
    finally:
        con.close()
    _print_table("pipeline_runs row", pd.DataFrame([row]))
    # XCom values must be JSON-serialisable: return timestamps as ISO strings
    return {k: (v.isoformat() if isinstance(v, pd.Timestamp) else v) for k, v in row.items()}
