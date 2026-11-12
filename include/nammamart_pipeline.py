"""Shared NammaMart pipeline utilities. Never edit source CSV files."""
from __future__ import annotations
import csv, json, re
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA_CANDIDATES = [ROOT / "include" / "data" / "nammamart", ROOT / "assignment" / "nammamart_dataset"]
FILES = {
    "orders": "orders.csv", "payments": "payments.csv", "customers": "customers.csv",
    "products": "products.csv", "stores": "stores.csv"
}
INCREMENT = "orders_2026-10-01_increment.csv"
REQUIRED_COLUMNS = {
    "orders.csv": {"order_id", "customer_id", "product_id", "store_id", "order_ts", "quantity", "unit_price", "discount_pct", "order_amount", "order_status", "delivery_minutes"},
    "payments.csv": {"payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status"},
    "customers.csv": {"customer_id", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"},
    "products.csv": {"product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"},
    "stores.csv": {"store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"},
    INCREMENT: {"order_id", "customer_id", "product_id", "store_id", "order_ts", "quantity", "unit_price", "discount_pct", "order_amount", "order_status", "delivery_minutes"},
}
WAREHOUSE = ROOT / "include" / "warehouse_surbhi.duckdb"
def data_dir():
    for p in DATA_CANDIDATES:
        if p.exists() and (p / "orders.csv").exists(): return p
    raise FileNotFoundError("Dataset not found. Copy assignment/nammamart_dataset into include/data/nammamart.")
def connect():
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(WAREHOUSE))

def append_new_raw_rows(con, table, frame):
    """Append source rows once; allow changed versions to remain in raw history."""
    metadata = {"_loaded_at", "_source_file", "_run_id"}
    source_columns = [column for column in frame.columns if column not in metadata]
    if not source_columns:
        raise ValueError("Raw input has no source columns")
    def quote(identifier):
        return '"' + identifier.replace('"', '""') + '"'
    match = " AND ".join(
        "existing." + quote(column) + " IS NOT DISTINCT FROM incoming." + quote(column)
        for column in source_columns
    )
    con.execute(
        "INSERT INTO " + table + " BY NAME SELECT DISTINCT incoming.* FROM raw_input AS incoming "
        "WHERE NOT EXISTS (SELECT 1 FROM " + table + " AS existing WHERE " + match + ")"
    )
def check_required_columns(frame, required, source_name):
    """Raise a clear error when a source export loses required columns."""
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"Schema check failed for {source_name}: missing required columns {missing}")


def write_observability_report(run_id, checks):
    """Persist observable warnings/checks as a CSV artifact."""
    ensure_dirs()
    report = pd.DataFrame(checks)
    report["run_id"] = run_id
    report["checked_at_utc"] = datetime.now(timezone.utc).isoformat()
    report.to_csv(ROOT / "include" / "reports" / f"observability_{run_id.replace(':', '_')}.csv", index=False)
    return report


def read_csv(name, dtype=None):
    return pd.read_csv(data_dir() / name, dtype=dtype, keep_default_na=True)
def mask_email(v):
    if pd.isna(v) or not str(v).strip(): return None
    s=str(v).strip()
    if "@" not in s: return "***"
    left, right=s.split("@",1)
    return (left[:2] + "***@" + right) if len(left)>2 else (left[:1]+"***@"+right)
def mask_phone(v):
    if pd.isna(v): return None
    digits=re.sub(r"\D","",str(v))
    return (digits[:2] + "*"*max(0,len(digits)-4) + digits[-2:]) if len(digits)>=4 else "***"
def ensure_dirs():
    for p in [ROOT/"include"/"quarantine"/"surbhi", ROOT/"include"/"reports", ROOT/"include"/"sql"]:
        p.mkdir(parents=True, exist_ok=True)
def dq_orders(df, customers, products, stores, run_id):
    """Return row-level issues. Critical failures are quarantined."""
    checks=[]
    def add(mask, col, problem, dimension, severity="Critical"):
        for idx in df.index[mask.fillna(False)]:
            checks.append({"file":"orders.csv","row_key":str(df.at[idx,"order_id"]) if "order_id" in df else str(idx),
              "column":col,"problem":problem,"quality_dimension":dimension,"severity":severity,
              "run_id":run_id})
    add(df["order_id"].isna(), "order_id", "Missing order identifier", "Completeness")
    add(df["customer_id"].isna(), "customer_id", "Missing customer key", "Completeness")
    add(df["quantity"].isna() | (pd.to_numeric(df["quantity"],errors="coerce")<=0), "quantity", "Quantity must be positive", "Accuracy")
    add(~df["order_status"].astype(str).str.strip().str.upper().isin(["DELIVERED","CANCELLED","RETURNED"]), "order_status", "Unknown order status", "Validity")
    add(pd.to_datetime(df["order_ts"],errors="coerce").isna(), "order_ts", "Invalid timestamp", "Validity")
    add(pd.to_numeric(df["discount_pct"],errors="coerce").isna() | ~pd.to_numeric(df["discount_pct"],errors="coerce").between(0,100), "discount_pct", "Discount must be between 0 and 100", "Validity")
    calc=(pd.to_numeric(df["quantity"],errors="coerce")*pd.to_numeric(df["unit_price"],errors="coerce")*(1-pd.to_numeric(df["discount_pct"],errors="coerce")/100)).round(2)
    amt=pd.to_numeric(df["order_amount"],errors="coerce")
    add((calc-amt).abs()>0.02, "order_amount", "Amount does not match quantity × unit price × discount", "Accuracy")
    add(df["order_id"].duplicated(keep="first"), "order_id", "Duplicate order identifier", "Uniqueness")
    add(~df["customer_id"].isin(customers["customer_id"]), "customer_id", "Customer key not found in customer master", "Consistency")
    add(~df["product_id"].isin(products["product_id"]), "product_id", "Product key not found in product master", "Consistency")
    add(~df["store_id"].isin(stores["store_id"]), "store_id", "Store key not found in store master", "Consistency")
    delivered=df["order_status"].astype(str).str.strip().str.upper().eq("DELIVERED")
    add(delivered & pd.to_numeric(df["delivery_minutes"],errors="coerce").isna(), "delivery_minutes", "Delivered order has no delivery duration", "Completeness")
    add(delivered & (pd.to_numeric(df["delivery_minutes"],errors="coerce")>=120), "delivery_minutes", "Delivery duration is implausibly high (>=120 minutes)", "Accuracy")
    ts=pd.to_datetime(df["order_ts"],errors="coerce")
    add(ts > pd.Timestamp.now(tz=None), "order_ts", "Order timestamp is in the future", "Timeliness")
    issues=pd.DataFrame(checks)
    failed_keys=set(issues.loc[issues.severity.eq("Critical"),"row_key"].astype(str)) if not issues.empty else set()
    failed=df["order_id"].astype(str).isin(failed_keys)
    return df.loc[~failed].copy(), df.loc[failed].copy(), issues
def profile_all(run_id):
    """Profile all six source files and write both issue and column-profile artifacts."""
    ensure_dirs()
    d = data_dir()
    records = []
    profile_rows = []
    category_columns = {"order_status", "payment_mode", "loyalty_tier", "zone", "category"}
    for fn in [*FILES.values(), INCREMENT]:
        frame = pd.read_csv(d / fn, low_memory=False)
        for col in frame.columns:
            nulls = int(frame[col].isna().sum())
            distinct = int(frame[col].nunique(dropna=True))
            numeric = pd.to_numeric(frame[col], errors="coerce")
            date_like = col.endswith("_ts") or col.endswith("_date") or col in {"signup_date","open_date","launch_date"}
            if numeric.notna().sum() == frame[col].notna().sum() and numeric.notna().any():
                min_value, max_value = str(numeric.min()), str(numeric.max())
            elif date_like:
                parsed = pd.to_datetime(frame[col], errors="coerce").dropna()
                min_value = str(parsed.min()) if not parsed.empty else None
                max_value = str(parsed.max()) if not parsed.empty else None
            else:
                min_value = max_value = None
            category_values = None
            if col in category_columns:
                category_values = " | ".join(frame[col].dropna().astype(str).value_counts().head(30).index.tolist())
            profile_rows.append({
                "file": fn, "row_count": len(frame), "column": col,
                "dtype_inferred": str(frame[col].dtype), "null_count": nulls,
                "distinct_count": distinct, "min_value": min_value,
                "max_value": max_value, "common_category_values": category_values,
                "run_id": run_id
            })
            if nulls:
                records.append({"file":fn,"row_key":"*","column":col,"problem":f"{nulls} null values",
                    "quality_dimension":"Completeness","severity":"Warning","run_id":run_id})
        key = "order_id" if "order_id" in frame else ("payment_id" if "payment_id" in frame else ("customer_id" if "customer_id" in frame else ("product_id" if "product_id" in frame else ("store_id" if "store_id" in frame else None))))
        if key and frame[key].duplicated().any():
            records.append({"file":fn,"row_key":"*","column":key,
                "problem":f"{int(frame[key].duplicated().sum())} duplicate keys",
                "quality_dimension":"Uniqueness","severity":"Critical","run_id":run_id})
    pd.DataFrame(profile_rows).to_csv(ROOT / "include" / "reports" / f"profile_summary_{run_id.replace(':','_')}.csv", index=False)
    return pd.DataFrame(records)

def record_dq(con, run_id, dag_id, issues, rows_checked):
    con.execute("""CREATE TABLE IF NOT EXISTS dq_results(run_id VARCHAR,dag_id VARCHAR,check_name VARCHAR,dimension VARCHAR,severity VARCHAR,rows_checked BIGINT,rows_failed BIGINT,status VARCHAR,checked_at TIMESTAMP)""")
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    if issues.empty:
        con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)",[run_id,dag_id,"orders_critical_rules","Multiple","Critical",rows_checked,0,"PASS",now])
    else:
        for dim, group in issues.groupby("quality_dimension"):
            con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)",[run_id,dag_id,f"{dim.lower()}_checks",dim,group["severity"].iloc[0],rows_checked,len(group),"FAIL" if group["severity"].eq("Critical").any() else "WARN",now])
def record_run(con, run_id, dag_id, start, extracted, loaded, quarantined, status):
    con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs(run_id VARCHAR,dag_id VARCHAR,start_time TIMESTAMP,end_time TIMESTAMP,rows_extracted BIGINT,rows_loaded BIGINT,rows_quarantined BIGINT,status VARCHAR)""")
    con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",[run_id,dag_id,start,datetime.now(timezone.utc).replace(tzinfo=None),extracted,loaded,quarantined,status])

def _record_quality_checks(con, run_id, dag_id, checks):
    """Persist one auditable result per quality rule for every pipeline run."""
    con.execute("""CREATE TABLE IF NOT EXISTS dq_results(
        run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
        severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
        checked_at TIMESTAMP)""")
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for item in checks:
        failed = int(item["rows_failed"])
        severity = item.get("severity", "Critical")
        status = "FAIL" if failed and severity == "Critical" else ("WARN" if failed else "PASS")
        con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?)", [
            run_id, dag_id, item["check_name"], item["dimension"], severity,
            int(item["rows_checked"]), failed, status, checked_at
        ])


def _count_bad(frame, mask):
    return int(mask.fillna(True).sum()) if hasattr(mask, "fillna") else int(mask.sum())


def record_etl_quality_scorecard(con, run_id, frames):
    """Python-side checks across all five source files and the increment feed."""
    orders, payments = frames["orders"], frames["payments"]
    customers, products, stores = frames["customers"], frames["products"], frames["stores"]
    checks = []
    def add(name, dim, frame, mask, severity="Critical"):
        checks.append({"check_name": name, "dimension": dim, "severity": severity,
                       "rows_checked": len(frame), "rows_failed": _count_bad(frame, mask)})
    add("orders_required_fields", "Completeness", orders,
        orders[["order_id","customer_id","product_id","store_id","order_ts","quantity","order_amount"]].isna().any(axis=1))
    add("payments_required_fields", "Completeness", payments,
        payments[["payment_id","order_id","payment_ts","amount_paid","payment_status"]].isna().any(axis=1))
    add("customers_required_fields", "Completeness", customers,
        customers[["customer_id","city","signup_date"]].isna().any(axis=1))
    add("products_required_fields", "Completeness", products,
        products[["product_id","product_name","category","mrp","cost_price"]].isna().any(axis=1))
    add("stores_required_fields", "Completeness", stores,
        stores[["store_id","store_name","zone","capacity_orders_per_day"]].isna().any(axis=1))
    add("orders_unique_order_id", "Uniqueness", orders, orders["order_id"].duplicated(keep=False))
    add("payments_unique_payment_id", "Uniqueness", payments, payments["payment_id"].duplicated(keep=False))
    add("customers_unique_customer_id", "Uniqueness", customers, customers["customer_id"].duplicated(keep=False))
    add("products_unique_product_id", "Uniqueness", products, products["product_id"].duplicated(keep=False))
    add("stores_unique_store_id", "Uniqueness", stores, stores["store_id"].duplicated(keep=False))
    status_ok = orders["order_status"].astype(str).str.strip().str.upper().isin(["DELIVERED","CANCELLED","RETURNED"])
    add("orders_allowed_status", "Validity", orders, ~status_ok)
    add("payments_allowed_mode", "Validity", payments,
        ~payments["payment_mode"].astype(str).str.strip().str.upper().isin(["UPI","CARD","CASH ON DELIVERY","WALLET"]))
    add("customer_email_format", "Validity", customers,
        ~customers["email"].astype(str).str.match(r"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", na=False), "Warning")
    add("product_price_positive", "Accuracy", products,
        (pd.to_numeric(products["mrp"], errors="coerce") <= 0) |
        (pd.to_numeric(products["cost_price"], errors="coerce") <= 0))
    calc = (pd.to_numeric(orders["quantity"], errors="coerce") *
            pd.to_numeric(orders["unit_price"], errors="coerce") *
            (1-pd.to_numeric(orders["discount_pct"], errors="coerce")/100)).round(2)
    amount = pd.to_numeric(orders["order_amount"], errors="coerce")
    add("orders_amount_formula", "Accuracy", orders, (calc-amount).abs() > 0.02)
    add("orders_master_keys", "Consistency", orders,
        ~orders["customer_id"].isin(customers["customer_id"]) |
        ~orders["product_id"].isin(products["product_id"]) |
        ~orders["store_id"].isin(stores["store_id"]))
    add("payments_order_key", "Consistency", payments, ~payments["order_id"].isin(orders["order_id"]))
    order_ts = pd.to_datetime(orders["order_ts"], errors="coerce")
    add("orders_future_timestamp", "Timeliness", orders, order_ts > pd.Timestamp.now(), "Critical")
    add("orders_stale_timestamp", "Timeliness", orders,
        order_ts.notna() & (order_ts < (pd.Timestamp.now() - pd.Timedelta(days=30))), "Warning")
    increment = frames["increment"]
    add("increment_required_fields", "Completeness", increment,
        increment[["order_id","customer_id","product_id","store_id","order_ts","quantity","order_amount"]].isna().any(axis=1))
    add("increment_unique_order_id", "Uniqueness", increment, increment["order_id"].duplicated(keep=False))
    add("increment_allowed_status", "Validity", increment,
        ~increment["order_status"].astype(str).str.strip().str.upper().isin(["DELIVERED","CANCELLED","RETURNED"]))
    inc_calc = (pd.to_numeric(increment["quantity"], errors="coerce") *
                pd.to_numeric(increment["unit_price"], errors="coerce") *
                (1-pd.to_numeric(increment["discount_pct"], errors="coerce")/100)).round(2)
    inc_amount = pd.to_numeric(increment["order_amount"], errors="coerce")
    add("increment_amount_formula", "Accuracy", increment, (inc_calc-inc_amount).abs() > 0.02)
    add("increment_master_keys", "Consistency", increment,
        ~increment["customer_id"].isin(customers["customer_id"]) |
        ~increment["product_id"].isin(products["product_id"]) |
        ~increment["store_id"].isin(stores["store_id"]))
    inc_ts = pd.to_datetime(increment["order_ts"], errors="coerce")
    add("increment_future_timestamp", "Timeliness", increment, inc_ts > pd.Timestamp.now(), "Critical")
    add("increment_stale_timestamp", "Timeliness", increment,
        inc_ts.notna() & (inc_ts < (pd.Timestamp.now() - pd.Timedelta(days=30))), "Warning")
    _record_quality_checks(con, run_id, "nammamart_etl_surbhi", checks)


def record_elt_quality_scorecard(con, run_id):
    """SQL-side scorecard: rules execute against raw/staging DuckDB tables."""
    checks = [
        ("orders_required_fields","Completeness","SELECT count(*) FROM raw_orders WHERE coalesce(trim(order_id),'')='' OR coalesce(trim(customer_id),'')='' OR coalesce(trim(product_id),'')='' OR coalesce(trim(store_id),'')='' OR coalesce(trim(order_ts),'')='' OR coalesce(trim(quantity),'')='' OR coalesce(trim(order_amount),'')=''","Critical"),
        ("payments_required_fields","Completeness","SELECT count(*) FROM raw_payments WHERE coalesce(trim(payment_id),'')='' OR coalesce(trim(order_id),'')='' OR coalesce(trim(payment_ts),'')='' OR coalesce(trim(amount_paid),'')=''","Critical"),
        ("customers_required_fields","Completeness","SELECT count(*) FROM raw_customers WHERE coalesce(trim(customer_id),'')='' OR coalesce(trim(city),'')='' OR coalesce(trim(signup_date),'')=''","Critical"),
        ("products_required_fields","Completeness","SELECT count(*) FROM raw_products WHERE coalesce(trim(product_id),'')='' OR coalesce(trim(product_name),'')='' OR coalesce(trim(category),'')=''","Critical"),
        ("stores_required_fields","Completeness","SELECT count(*) FROM raw_stores WHERE coalesce(trim(store_id),'')='' OR coalesce(trim(store_name),'')='' OR coalesce(trim(zone),'')=''","Critical"),
        ("orders_unique_order_id","Uniqueness","SELECT count(*) FROM (SELECT order_id FROM raw_orders GROUP BY order_id HAVING count(*)>1)","Critical"),
        ("payments_unique_payment_id","Uniqueness","SELECT count(*) FROM (SELECT payment_id FROM raw_payments GROUP BY payment_id HAVING count(*)>1)","Critical"),
        ("customers_unique_customer_id","Uniqueness","SELECT count(*) FROM (SELECT customer_id FROM raw_customers GROUP BY customer_id HAVING count(*)>1)","Critical"),
        ("products_unique_product_id","Uniqueness","SELECT count(*) FROM (SELECT product_id FROM raw_products GROUP BY product_id HAVING count(*)>1)","Critical"),
        ("stores_unique_store_id","Uniqueness","SELECT count(*) FROM (SELECT store_id FROM raw_stores GROUP BY store_id HAVING count(*)>1)","Critical"),
        ("orders_allowed_status","Validity","SELECT count(*) FROM raw_orders WHERE upper(trim(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED')","Critical"),
        ("payments_allowed_mode","Validity","SELECT count(*) FROM raw_payments WHERE upper(trim(payment_mode)) NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET')","Critical"),
        ("product_price_positive","Accuracy","SELECT count(*) FROM raw_products WHERE try_cast(mrp AS DOUBLE)<=0 OR try_cast(cost_price AS DOUBLE)<=0 OR try_cast(mrp AS DOUBLE) IS NULL OR try_cast(cost_price AS DOUBLE) IS NULL","Critical"),
        ("orders_amount_formula","Accuracy","SELECT count(*) FROM raw_orders WHERE abs(try_cast(order_amount AS DOUBLE)-try_cast(quantity AS DOUBLE)*try_cast(unit_price AS DOUBLE)*(1-try_cast(discount_pct AS DOUBLE)/100))>0.02","Critical"),
        ("orders_master_keys","Consistency","SELECT count(*) FROM raw_orders o WHERE NOT EXISTS (SELECT 1 FROM raw_customers c WHERE c.customer_id=o.customer_id) OR NOT EXISTS (SELECT 1 FROM raw_products p WHERE p.product_id=o.product_id) OR NOT EXISTS (SELECT 1 FROM raw_stores s WHERE s.store_id=o.store_id)","Critical"),
        ("payments_order_key","Consistency","SELECT count(*) FROM raw_payments p WHERE NOT EXISTS (SELECT 1 FROM raw_orders o WHERE o.order_id=p.order_id)","Critical"),
        ("orders_future_timestamp","Timeliness","SELECT count(*) FROM raw_orders WHERE try_cast(order_ts AS TIMESTAMP)>current_timestamp","Critical"),
        ("orders_stale_timestamp","Timeliness","SELECT CASE WHEN max(try_cast(order_ts AS TIMESTAMP)) < current_timestamp-INTERVAL 30 DAY THEN count(*) ELSE 0 END FROM raw_orders WHERE try_cast(order_ts AS TIMESTAMP)<=current_timestamp","Warning"),
        ("increment_required_fields","Completeness","SELECT count(*) FROM raw_orders_increment WHERE coalesce(trim(order_id),'')='' OR coalesce(trim(customer_id),'')='' OR coalesce(trim(product_id),'')='' OR coalesce(trim(store_id),'')='' OR coalesce(trim(order_ts),'')='' OR coalesce(trim(quantity),'')='' OR coalesce(trim(order_amount),'')=''","Critical"),
        ("increment_unique_order_id","Uniqueness","SELECT count(*) FROM (SELECT order_id FROM raw_orders_increment GROUP BY order_id HAVING count(*)>1)","Critical"),
        ("increment_allowed_status","Validity","SELECT count(*) FROM raw_orders_increment WHERE upper(trim(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED')","Critical"),
        ("increment_amount_formula","Accuracy","SELECT count(*) FROM raw_orders_increment WHERE abs(try_cast(order_amount AS DOUBLE)-try_cast(quantity AS DOUBLE)*try_cast(unit_price AS DOUBLE)*(1-try_cast(discount_pct AS DOUBLE)/100))>0.02","Critical"),
        ("increment_master_keys","Consistency","SELECT count(*) FROM raw_orders_increment o WHERE NOT EXISTS (SELECT 1 FROM raw_customers c WHERE c.customer_id=o.customer_id) OR NOT EXISTS (SELECT 1 FROM raw_products p WHERE p.product_id=o.product_id) OR NOT EXISTS (SELECT 1 FROM raw_stores s WHERE s.store_id=o.store_id)","Critical"),
        ("increment_future_timestamp","Timeliness","SELECT count(*) FROM raw_orders_increment WHERE try_cast(order_ts AS TIMESTAMP)>current_timestamp","Critical"),
        ("increment_stale_timestamp","Timeliness","SELECT CASE WHEN max(try_cast(order_ts AS TIMESTAMP)) < current_timestamp-INTERVAL 30 DAY THEN count(*) ELSE 0 END FROM raw_orders_increment WHERE try_cast(order_ts AS TIMESTAMP)<=current_timestamp","Warning"),
    ]
    # Report the population actually inspected by each rule, not the sum of all raw tables.
    def source_table_for_check(name):
        if name.startswith("increment_"):
            return "raw_orders_increment"
        if name.startswith("payments_") or name == "payments_order_key":
            return "raw_payments"
        if name.startswith("customers_"):
            return "raw_customers"
        if name.startswith("products_") or name == "product_price_positive":
            return "raw_products"
        if name.startswith("stores_"):
            return "raw_stores"
        return "raw_orders"

    scorecard = []
    for name, dim, sql, severity in checks:
        failed = int(con.execute(sql).fetchone()[0])
        table = source_table_for_check(name)
        checked_count = int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        scorecard.append({"check_name":name,"dimension":dim,"severity":severity,
                          "rows_checked":checked_count,"rows_failed":failed})
    _record_quality_checks(con, run_id, "nammamart_elt_surbhi", scorecard)



def ensure_run_audit(run_id, dag_id, status="failed"):
    """Fallback audit for runs whose pipeline task failed before writing its own audit row."""
    con = connect()
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs(
            run_id VARCHAR,dag_id VARCHAR,start_time TIMESTAMP,end_time TIMESTAMP,
            rows_extracted BIGINT,rows_loaded BIGINT,rows_quarantined BIGINT,status VARCHAR)""")
        exists = con.execute("SELECT count(*) FROM pipeline_runs WHERE run_id=? AND dag_id=?", [run_id, dag_id]).fetchone()[0]
        if not exists:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
                        [run_id, dag_id, now, now, 0, 0, 0, status])
    finally:
        con.close()


def execute_sql_file(con, filename):
    """Execute a checked-in SQL transformation from include/sql."""
    sql_path = ROOT / "include" / "sql" / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL transformation file not found: {sql_path}")
    sql_text = sql_path.read_text(encoding="utf-8")
    # These checked-in assignment SQL files use semicolons only as statement delimiters.
    for statement in sql_text.split(";"):
        if statement.strip():
            con.execute(statement)


def quarantine_source_frames(run_id, source_frames):
    """Write row-level rejects for non-order source files without altering inputs."""
    ensure_dirs()
    total = 0
    specs = {
        "payments.csv": (["payment_id","order_id","payment_ts","amount_paid","payment_status"], "payment_id"),
        "customers.csv": (["customer_id","city","signup_date"], "customer_id"),
        "products.csv": (["product_id","product_name","category","mrp","cost_price"], "product_id"),
        "stores.csv": (["store_id","store_name","zone","capacity_orders_per_day"], "store_id"),
        INCREMENT: (["order_id","customer_id","product_id","store_id","order_ts","quantity","order_amount"], "order_id"),
    }
    for filename, frame in source_frames.items():
        if filename not in specs or frame.empty:
            continue
        required, key = specs[filename]
        available = [column for column in required if column in frame.columns]
        missing = frame[available].isna().any(axis=1) if available else pd.Series(False, index=frame.index)
        reasons = pd.Series("", index=frame.index, dtype="object")
        reasons.loc[missing] = "Missing required field(s)"
        duplicates = frame[key].duplicated(keep="first") if key in frame.columns else pd.Series(False, index=frame.index)
        reasons.loc[duplicates] = reasons.loc[duplicates].apply(lambda value: (value + "; " if value else "") + f"Duplicate {key}")
        if filename == "payments.csv":
            invalid_mode = ~frame["payment_mode"].astype(str).str.strip().str.upper().isin(["UPI","CARD","CASH ON DELIVERY","WALLET"])
            reasons.loc[invalid_mode] = reasons.loc[invalid_mode].apply(lambda value: (value + "; " if value else "") + "Invalid payment_mode")
            amount = pd.to_numeric(frame["amount_paid"], errors="coerce")
            invalid_amount = amount.isna() | (amount < 0)
            reasons.loc[invalid_amount] = reasons.loc[invalid_amount].apply(lambda value: (value + "; " if value else "") + "Invalid amount_paid")
        elif filename == "products.csv":
            mrp = pd.to_numeric(frame["mrp"], errors="coerce")
            cost = pd.to_numeric(frame["cost_price"], errors="coerce")
            invalid_price = mrp.isna() | cost.isna() | (mrp <= 0) | (cost <= 0) | (cost > mrp)
            reasons.loc[invalid_price] = reasons.loc[invalid_price].apply(lambda value: (value + "; " if value else "") + "Invalid product pricing")
        elif filename == "stores.csv":
            capacity = pd.to_numeric(frame["capacity_orders_per_day"], errors="coerce")
            invalid_capacity = capacity.isna() | (capacity <= 0)
            reasons.loc[invalid_capacity] = reasons.loc[invalid_capacity].apply(lambda value: (value + "; " if value else "") + "Invalid store capacity")
        bad = frame.loc[reasons.ne("")].copy()
        if not bad.empty:
            bad["failure_reason"] = reasons.loc[bad.index].values
            safe_name = filename.replace(".csv", "").replace("/", "_")
            bad.to_csv(ROOT / "include" / "quarantine" / "surbhi" / f"{safe_name}_failed_{run_id.replace(':','_')}.csv", index=False)
            total += len(bad)
    return total


def run_etl(run_id):
    ensure_dirs(); start=datetime.now(timezone.utc).replace(tzinfo=None); con=connect()
    try:
        base_orders=read_csv(FILES["orders"]); increment_orders=read_csv(INCREMENT)
        # Current-state ETL upsert: increment rows override earlier order versions.
        orders=pd.concat([base_orders, increment_orders], ignore_index=True).drop_duplicates("order_id", keep="last")
        payments=read_csv(FILES["payments"])
        customers_source=read_csv(FILES["customers"]); customers=customers_source.drop_duplicates("customer_id")
        products_source=read_csv(FILES["products"]); products=products_source.drop_duplicates("product_id")
        stores_source=read_csv(FILES["stores"]); stores=stores_source.drop_duplicates("store_id")
        other_quarantined=quarantine_source_frames(run_id, {
            "payments.csv": payments, "customers.csv": customers_source,
            "products.csv": products_source, "stores.csv": stores_source
        })
        issues=profile_all(run_id)
        clean,bad,order_issues=dq_orders(orders,customers,products,stores,run_id)
        issues=pd.concat([issues,order_issues],ignore_index=True)
        if not issues.empty: issues.to_csv(ROOT/"include"/"reports"/f"data_issues_{run_id.replace(':','_')}.csv",index=False)
        if not bad.empty:
            bad = bad.copy()
            # Preserve the actual validation messages for each quarantined order.
            # Multiple failed checks for the same order are combined into one reason.
            if not order_issues.empty:
                reason_by_key = (
                    order_issues.groupby("row_key")["problem"]
                    .agg(lambda problems: "; ".join(dict.fromkeys(str(p) for p in problems)))
                    .to_dict()
                )
            else:
                reason_by_key = {}
            bad["failure_reason"] = bad["order_id"].astype(str).map(reason_by_key)
            bad["failure_reason"] = bad["failure_reason"].fillna(
                "Rejected by data-quality validation; see data_issues report"
            )
            bad.to_csv(ROOT/"include"/"quarantine"/"surbhi"/f"orders_failed_{run_id.replace(':','_')}.csv",index=False)
        for name, frame in [("etl_dim_customer",customers),("etl_dim_product",products),("etl_dim_store",stores)]:
            frame=frame.copy()
            if name=="etl_dim_customer":
                frame["email"]=frame["email"].map(mask_email); frame["phone"]=frame["phone"].map(mask_phone)
            con.register("tmp_frame",frame); con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM tmp_frame"); con.unregister("tmp_frame")
        clean["order_status"]=clean["order_status"].astype(str).str.strip().str.upper()
        clean["payment_mode"]=clean["payment_mode"].astype(str).str.strip().str.upper().replace({"CASH ON DELIVERY":"Cash on Delivery"})
        clean["order_date"]=pd.to_datetime(clean["order_ts"],errors="coerce").dt.date
        clean["net_revenue"]=pd.to_numeric(clean["order_amount"],errors="coerce").where(clean["order_status"].eq("DELIVERED"),0)
        clean=clean.merge(products[["product_id","category","cost_price"]],on="product_id",how="left")
        clean["gross_margin"]=clean["net_revenue"]-pd.to_numeric(clean["quantity"],errors="coerce")*pd.to_numeric(clean["cost_price"],errors="coerce")
        clean["within_15_min"]=pd.to_numeric(clean["delivery_minutes"],errors="coerce").le(15)
        clean=clean.merge(stores[["store_id","store_name","zone"]],on="store_id",how="left")
        clean=clean.merge(customers[["customer_id","loyalty_tier","email","phone"]].assign(email=lambda x:x.email.map(mask_email),phone=lambda x:x.phone.map(mask_phone)),on="customer_id",how="left",suffixes=("","_masked"))
        con.register("tmp_frame",clean); con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM tmp_frame"); con.unregister("tmp_frame")
        con.execute("CREATE OR REPLACE TABLE etl_daily_zone_revenue AS SELECT order_date,zone,store_id,store_name,SUM(net_revenue) revenue FROM etl_fact_orders GROUP BY ALL")
        con.execute("CREATE OR REPLACE TABLE etl_category_margin AS SELECT category,SUM(net_revenue) revenue,SUM(gross_margin) gross_margin FROM etl_fact_orders GROUP BY category")
        con.execute("""CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            SELECT o.order_id,o.order_amount,p.amount_paid,p.payment_status,
            CASE WHEN p.order_id IS NULL THEN 'No payment found'
                 WHEN abs(try_cast(o.order_amount AS DOUBLE)-try_cast(p.amount_paid AS DOUBLE))>0.02 THEN 'Amount mismatch'
                 WHEN upper(p.payment_status)!='SUCCESS' THEN 'Payment not successful' END failure_reason
            FROM etl_fact_orders o LEFT JOIN payments p USING(order_id)
            WHERE p.order_id IS NULL OR abs(try_cast(o.order_amount AS DOUBLE)-try_cast(p.amount_paid AS DOUBLE))>0.02 OR upper(p.payment_status)!='SUCCESS'""")
        record_dq(con,run_id,"nammamart_etl_surbhi",issues,len(orders))
        record_etl_quality_scorecard(con, run_id, {
            "orders": base_orders, "payments": payments,
            "customers": customers_source, "products": products_source, "stores": stores_source,
            "increment": increment_orders
        })
        record_run(con,run_id,"nammamart_etl_surbhi",start,len(base_orders)+len(increment_orders)+len(payments)+len(customers_source)+len(products_source)+len(stores_source),len(clean),len(bad)+other_quarantined,"success")
    except Exception:
        record_run(con,run_id,"nammamart_etl_surbhi",start,0,0,0,"failed")
        raise
    finally: con.close()
def record_elt_observability(con, run_id, source_dir):
    """Record source volume, freshness, and store-level distribution checks."""
    checks = []
    con.execute("""CREATE TABLE IF NOT EXISTS observability_metrics(
        metric_name VARCHAR, metric_key VARCHAR, metric_value DOUBLE,
        run_id VARCHAR, recorded_at TIMESTAMP)""")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for filename in [*FILES.values(), INCREMENT]:
        path = source_dir / filename
        if not path.exists():
            checks.append({"check":"source_volume", "subject":filename, "status":"FAIL", "observed":None, "details":"Required source file is missing"})
            continue
        frame = pd.read_csv(path, low_memory=False)
        count = len(frame)
        previous = con.execute("SELECT metric_value FROM observability_metrics WHERE metric_name='source_rows' AND metric_key=? ORDER BY recorded_at DESC LIMIT 1", [filename]).fetchone()
        status, details = "PASS", "Source row count recorded"
        if previous and previous[0] > 0 and count < previous[0] * 0.10:
            status, details = "WARN", "Source row count dropped by more than 90% versus prior run"
        checks.append({"check":"source_volume", "subject":filename, "status":status, "observed":count, "details":details})
        con.execute("INSERT INTO observability_metrics VALUES (?,?,?,?,?)", ["source_rows", filename, float(count), run_id, now])
    orders = pd.read_csv(source_dir / FILES["orders"], low_memory=False)
    timestamps = pd.to_datetime(orders.get("order_ts"), errors="coerce", utc=True).dropna()
    if timestamps.empty:
        checks.append({"check":"freshness", "subject":"orders.csv", "status":"FAIL", "observed":None, "details":"No valid order timestamps found"})
    else:
        now_utc = pd.Timestamp.now(tz="UTC")
        future_count = int((timestamps > now_utc).sum())
        non_future = timestamps[timestamps <= now_utc]
        if non_future.empty:
            checks.append({"check":"freshness", "subject":"orders.csv", "status":"WARN", "observed":None, "details":"No non-future order timestamps available to assess freshness"})
        else:
            age_days = (now_utc - non_future.max()).total_seconds() / 86400
            checks.append({"check":"freshness", "subject":"orders.csv", "status":"WARN" if age_days > 30 else "PASS", "observed":round(age_days, 2), "details":"Age in days of newest non-future order timestamp; warning threshold is 30 days"})
        checks.append({"check":"future_timestamps", "subject":"orders.csv", "status":"WARN" if future_count else "PASS", "observed":future_count, "details":"Count of future order timestamps"})
    if {"store_id", "order_amount"}.issubset(orders.columns):
        orders["order_amount_numeric"] = pd.to_numeric(orders["order_amount"], errors="coerce")
        current = orders.dropna(subset=["store_id", "order_amount_numeric"]).groupby("store_id")["order_amount_numeric"].mean()
        for store_id, mean_amount in current.items():
            key = str(store_id)
            previous = con.execute("SELECT metric_value FROM observability_metrics WHERE metric_name='store_avg_order_amount' AND metric_key=? ORDER BY recorded_at DESC LIMIT 1", [key]).fetchone()
            status, details = "PASS", "Store average order amount recorded"
            if previous and previous[0] != 0 and abs(mean_amount - previous[0]) / abs(previous[0]) > 0.50:
                status, details = "WARN", "Store average order amount shifted by more than 50% versus prior run"
            checks.append({"check":"distribution_drift", "subject":key, "status":status, "observed":round(float(mean_amount), 2), "details":details})
            con.execute("INSERT INTO observability_metrics VALUES (?,?,?,?,?)", ["store_avg_order_amount", key, float(mean_amount), run_id, now])
    write_observability_report(run_id, checks)

def run_elt(run_id):
    ensure_dirs(); start=datetime.now(timezone.utc).replace(tzinfo=None); con=connect(); extracted=loaded=0
    try:
        d=data_dir()
        for logical, fn in FILES.items():
            raw=pd.read_csv(d/fn,dtype=str,keep_default_na=False)
            check_required_columns(raw, REQUIRED_COLUMNS[fn], fn)
            raw["_loaded_at"]=datetime.now(timezone.utc).isoformat(); raw["_source_file"]=fn; raw["_run_id"]=run_id
            extracted+=len(raw)
            con.register("raw_input",raw)
            table=f"raw_{logical}"
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM raw_input WHERE 1=0")
            append_new_raw_rows(con, table, raw)
            con.unregister("raw_input")
        # Raw increment is append-only; typed staging is upserted on order_id.
        inc=d/INCREMENT
        if inc.exists():
            raw=pd.read_csv(inc,dtype=str,keep_default_na=False); check_required_columns(raw, REQUIRED_COLUMNS[INCREMENT], INCREMENT); raw["_loaded_at"]=datetime.now(timezone.utc).isoformat(); raw["_source_file"]=INCREMENT; raw["_run_id"]=run_id
            extracted+=len(raw); con.register("raw_input",raw)
            con.execute("CREATE TABLE IF NOT EXISTS raw_orders_increment AS SELECT * FROM raw_input WHERE 1=0")
            append_new_raw_rows(con, "raw_orders_increment", raw); con.unregister("raw_input")
        # SQL-only staging and marts. Raw data is retained even when staging rejects it.
        execute_sql_file(con, "elt_staging_dimensions.sql")
        execute_sql_file(con, "elt_staging_orders.sql")
        # Merge increment: newest copy of an order wins in staging; raw remains append-only.
        if inc.exists():
            incdf=pd.read_csv(inc,dtype=str,keep_default_na=False); incdf["_loaded_at"]=datetime.now(timezone.utc).isoformat(); incdf["_source_file"]=INCREMENT; incdf["_run_id"]=run_id
            con.register("inc_input",incdf)
            con.execute("""
                CREATE OR REPLACE TEMP TABLE inc_clean AS
                SELECT *,
                    try_cast(order_ts AS TIMESTAMP) AS order_ts_cast,
                    try_cast(quantity AS INTEGER) AS quantity_cast,
                    try_cast(unit_price AS DOUBLE) AS unit_price_cast,
                    try_cast(discount_pct AS DOUBLE) AS discount_pct_cast,
                    try_cast(order_amount AS DOUBLE) AS order_amount_cast,
                    try_cast(delivery_minutes AS DOUBLE) AS delivery_minutes_cast,
                    upper(trim(order_status)) AS order_status_std,
                    CASE
                        WHEN order_id IS NULL OR trim(order_id) = ''
                            THEN 'Missing order_id'
                        WHEN try_cast(order_ts AS TIMESTAMP) IS NULL
                            THEN 'Invalid timestamp'
                        WHEN try_cast(order_ts AS TIMESTAMP) > current_timestamp
                            THEN 'Future timestamp'
                        WHEN try_cast(quantity AS INTEGER) IS NULL
                             OR try_cast(quantity AS INTEGER) <= 0
                            THEN 'Invalid quantity'
                        WHEN upper(trim(order_status)) NOT IN
                             ('DELIVERED', 'CANCELLED', 'RETURNED')
                            THEN 'Invalid order status'
                        WHEN try_cast(discount_pct AS DOUBLE) IS NULL
                             OR try_cast(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100
                            THEN 'Invalid discount'
                        WHEN try_cast(unit_price AS DOUBLE) IS NULL
                             OR try_cast(order_amount AS DOUBLE) IS NULL
                            THEN 'Invalid price or amount'
                        WHEN abs(
                            try_cast(order_amount AS DOUBLE) -
                            try_cast(quantity AS DOUBLE) *
                            try_cast(unit_price AS DOUBLE) *
                            (1 - try_cast(discount_pct AS DOUBLE) / 100)
                        ) > 0.02
                            THEN 'Amount mismatch'
                        WHEN NOT EXISTS (
                            SELECT 1 FROM stg_customers c
                            WHERE c.customer_id = inc_input.customer_id AND c.dq_status <> 'FAIL'
                        )
                            THEN 'Unknown customer'
                        WHEN NOT EXISTS (
                            SELECT 1 FROM stg_products p
                            WHERE p.product_id = inc_input.product_id AND p.dq_status <> 'FAIL'
                        )
                            THEN 'Unknown product'
                        WHEN NOT EXISTS (
                            SELECT 1 FROM stg_stores s
                            WHERE s.store_id = inc_input.store_id AND s.dq_status <> 'FAIL'
                        )
                            THEN 'Unknown store'
                        WHEN upper(trim(order_status)) = 'DELIVERED'
                             AND try_cast(delivery_minutes AS DOUBLE) IS NULL
                            THEN 'Missing delivery time'
                        WHEN upper(trim(order_status)) = 'DELIVERED'
                             AND try_cast(delivery_minutes AS DOUBLE) >= 120
                            THEN 'Delivery time too long'
                        ELSE NULL
                    END AS failure_reason,
                    CASE
                        WHEN order_id IS NULL OR trim(order_id) = ''
                          OR try_cast(order_ts AS TIMESTAMP) IS NULL
                          OR try_cast(order_ts AS TIMESTAMP) > current_timestamp
                          OR try_cast(quantity AS INTEGER) IS NULL
                          OR try_cast(quantity AS INTEGER) <= 0
                          OR upper(trim(order_status)) NOT IN
                             ('DELIVERED', 'CANCELLED', 'RETURNED')
                          OR try_cast(discount_pct AS DOUBLE) IS NULL
                          OR try_cast(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100
                          OR try_cast(unit_price AS DOUBLE) IS NULL
                          OR try_cast(order_amount AS DOUBLE) IS NULL
                          OR abs(
                              try_cast(order_amount AS DOUBLE) -
                              try_cast(quantity AS DOUBLE) *
                              try_cast(unit_price AS DOUBLE) *
                              (1 - try_cast(discount_pct AS DOUBLE) / 100)
                          ) > 0.02
                          OR NOT EXISTS (
                              SELECT 1 FROM stg_customers c
                              WHERE c.customer_id = inc_input.customer_id AND c.dq_status <> 'FAIL'
                          )
                          OR NOT EXISTS (
                              SELECT 1 FROM stg_products p
                              WHERE p.product_id = inc_input.product_id AND p.dq_status <> 'FAIL'
                          )
                          OR NOT EXISTS (
                              SELECT 1 FROM stg_stores s
                              WHERE s.store_id = inc_input.store_id AND s.dq_status <> 'FAIL'
                          )
                          OR (
                              upper(trim(order_status)) = 'DELIVERED'
                              AND (
                                  try_cast(delivery_minutes AS DOUBLE) IS NULL
                                  OR try_cast(delivery_minutes AS DOUBLE) >= 120
                              )
                          )
                        THEN 'FAIL'
                        ELSE 'PASS'
                    END AS dq_status
                FROM inc_input
            """)
            con.execute("CREATE OR REPLACE TEMP TABLE stg_orders_all AS SELECT * FROM stg_orders UNION ALL BY NAME SELECT * FROM inc_clean")
            con.execute("CREATE OR REPLACE TABLE stg_orders AS SELECT * FROM stg_orders_all QUALIFY row_number() OVER(PARTITION BY order_id ORDER BY _loaded_at DESC)=1")
            con.unregister("inc_input")
        execute_sql_file(con, "elt_marts.sql")
        # SQL scorecard: persist each rule separately for every run and dimension.
        record_elt_quality_scorecard(con, run_id)
        bad=con.execute("SELECT count(*) FROM stg_orders WHERE dq_status='FAIL'").fetchone()[0]
        loaded=con.execute("SELECT count(*) FROM stg_orders WHERE dq_status='PASS'").fetchone()[0]
        record_elt_observability(con, run_id, d)
        record_run(con,run_id,"nammamart_elt_surbhi",start,extracted,loaded,bad,"success")
    except Exception:
        record_run(con,run_id,"nammamart_elt_surbhi",start,extracted,loaded,0,"failed")
        raise
    finally: con.close()
