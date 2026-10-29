from __future__ import annotations
import os, re, hashlib
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"
WORK_DIR = PROJECT_ROOT / "include" / "work" / "nammamart"
DB_PATH = PROJECT_ROOT / "include" / "warehouse.duckdb"
SQL_DIR = PROJECT_ROOT / "include" / "sql"
FILES = {
    "customers": "customers.csv",
    "orders": "orders.csv",
    "increment": "orders_2026-10-01_increment.csv",
    "payments": "payments.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

def safe_run_id(run_id):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(run_id))[-80:]

def conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))

def read_csv(name, dtype=None):
    path = DATA_DIR / FILES[name]
    if not path.exists():
        raise FileNotFoundError(f"Required source file not found: {path}")
    return pd.read_csv(path, dtype=dtype, keep_default_na=True)

def mask_email(value):
    if pd.isna(value) or not str(value).strip(): return None
    value = str(value).strip()
    if "@" not in value: return "***"
    local, domain = value.split("@", 1)
    return (local[:1] + "***@" + domain.lower()) if local else "***@" + domain.lower()

def mask_phone(value):
    if pd.isna(value) or not str(value).strip(): return None
    digits = re.sub(r"\D", "", str(value))
    return ("*" * max(0, len(digits)-4) + digits[-4:]) if digits else None

def norm_text(series):
    return series.astype("string").str.strip()

def to_num(df, cols):
    for c in cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def extract_file(name, **context):
    # Extraction tasks validate that source files exist and return metadata only.
    path = DATA_DIR / FILES[name]
    if not path.exists():
        raise FileNotFoundError(f"Missing required source file: {path}")
    return {"name": name, "path": str(path), "bytes": path.stat().st_size}

def etl_validate(run_id):
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    rid = safe_run_id(run_id)
    customers = read_csv("customers")
    orders = pd.concat([read_csv("orders"), read_csv("increment")], ignore_index=True)
    payments = read_csv("payments")
    products = read_csv("products")
    stores = read_csv("stores")
    # Normalize whitespace and casing only in the cleaned layer.
    for df, cols in [
        (customers, ["customer_id","full_name","email","phone","city","area","loyalty_tier"]),
        (orders, ["order_id","customer_id","store_id","product_id","payment_mode","order_status","channel"]),
        (payments, ["payment_id","order_id","payment_mode","payment_status","gateway_ref"]),
        (products, ["product_id","product_name","category","sub_category","brand","unit"]),
        (stores, ["store_id","store_name","zone","area","manager_name"]),
    ]:
        for c in cols:
            if c in df.columns: df[c] = norm_text(df[c])
    customers["loyalty_tier"] = customers["loyalty_tier"].str.title()
    orders["order_status"] = orders["order_status"].str.upper()
    orders["payment_mode"] = orders["payment_mode"].str.strip().str.lower()
    mode_map = {"upi":"UPI","card":"CARD","wallet":"WALLET","cash on delivery":"CASH_ON_DELIVERY","cod":"CASH_ON_DELIVERY","cashh":"CASHH"}
    orders["payment_mode"] = orders["payment_mode"].map(lambda x: mode_map.get(str(x).lower(), str(x).upper()) if pd.notna(x) else x)
    payments["payment_mode"] = payments["payment_mode"].map(lambda x: mode_map.get(str(x).lower(), str(x).upper()) if pd.notna(x) else x)
    to_num(orders, ["quantity","unit_price","discount_pct","order_amount","delivery_minutes"])
    to_num(products, ["mrp","cost_price"])
    to_num(payments, ["amount_paid"])
    orders["order_ts_parsed"] = pd.to_datetime(orders["order_ts"], errors="coerce")
    orders["order_date"] = orders["order_ts_parsed"].dt.date.astype("string")
    # Remove only exact duplicate order records; do not silently resolve conflicting duplicate keys.
    orders = orders.drop_duplicates(subset=list(read_csv("orders").columns), keep="first").copy()
    cust_ids = set(customers["customer_id"].dropna().astype(str))
    prod_ids = set(products["product_id"].dropna().astype(str))
    store_ids = set(stores["store_id"].dropna().astype(str))
    critical = []
    for idx, row in orders.iterrows():
        reasons=[]
        if pd.isna(row.get("order_id")) or not str(row.get("order_id")).strip(): reasons.append("missing_order_id")
        if pd.isna(row.get("order_ts_parsed")): reasons.append("invalid_or_missing_order_ts")
        if pd.isna(row.get("customer_id")) or row.get("customer_id") not in cust_ids: reasons.append("missing_or_unknown_customer")
        if pd.isna(row.get("product_id")) or row.get("product_id") not in prod_ids: reasons.append("missing_or_unknown_product")
        if pd.isna(row.get("store_id")) or row.get("store_id") not in store_ids: reasons.append("missing_or_unknown_store")
        if pd.isna(row.get("quantity")) or row.get("quantity") <= 0: reasons.append("non_positive_or_invalid_quantity")
        if pd.isna(row.get("discount_pct")) or row.get("discount_pct") < 0 or row.get("discount_pct") > 100: reasons.append("invalid_discount_pct")
        if row.get("order_status") not in {"DELIVERED","CANCELLED","RETURNED","PENDING","SHIPPED"}: reasons.append("unexpected_order_status")
        critical.append(";".join(reasons))
    orders["failure_reason"] = critical
    failed = orders[orders["failure_reason"] != ""].copy()
    passed = orders[orders["failure_reason"] == ""].copy()
    # Use a stable run-specific quarantine file with required failure_reason.
    qdir = PROJECT_ROOT / "include" / "quarantine" / "indhu"
    qdir.mkdir(parents=True, exist_ok=True)
    if not failed.empty:
        failed.to_csv(qdir / f"orders_failed_{rid}.csv", index=False)
    # Data quality warnings (not automatic corrections) are recorded alongside critical errors.
    mismatch = orders["order_amount"].notna() & orders["quantity"].notna() & orders["unit_price"].notna() & orders["discount_pct"].notna()
    expected = orders["quantity"] * orders["unit_price"] * (1-orders["discount_pct"]/100)
    warnings = orders.loc[mismatch & ((orders["order_amount"]-expected).abs()>0.02), ["order_id"]].copy()
    warnings["failure_reason"] = "order_amount_formula_mismatch_review"
    warning_path = qdir / f"orders_warnings_{rid}.csv"
    if not warnings.empty: warnings.to_csv(warning_path, index=False)
    # Persist validated data and masters for the transform task.
    for name, df in [("orders_passed",passed),("customers",customers),("products",products),("stores",stores),("payments",payments)]:
        df.to_csv(WORK_DIR/f"{rid}_{name}.csv", index=False)
    return {"run_id":rid,"passed":len(passed),"failed":len(failed),"warnings":len(warnings),"work_dir":str(WORK_DIR)}

def etl_transform(run_id, validation_result=None):
    rid=safe_run_id(run_id)
    orders=pd.read_csv(WORK_DIR/f"{rid}_orders_passed.csv")
    customers=pd.read_csv(WORK_DIR/f"{rid}_customers.csv")
    products=pd.read_csv(WORK_DIR/f"{rid}_products.csv")
    stores=pd.read_csv(WORK_DIR/f"{rid}_stores.csv")
    payments=pd.read_csv(WORK_DIR/f"{rid}_payments.csv")
    # Deterministic dimension survivor rules. Product rows marked '(old)' lose to current rows.
    customers=customers.drop_duplicates("customer_id", keep="first")
    products["_old_flag"]=products["product_name"].astype(str).str.contains(r"\(old\)",case=False,regex=True)
    products=products.sort_values(["_old_flag","product_id"]).drop_duplicates("product_id",keep="first").drop(columns=["_old_flag"])
    stores=stores.drop_duplicates("store_id",keep="first")
    customers["masked_email"]=customers["email"].map(mask_email) if "email" in customers else None
    customers["masked_phone"]=customers["phone"].map(mask_phone) if "phone" in customers else None
    dim_customer=customers[[c for c in ["customer_id","masked_email","masked_phone","city","area","pincode","signup_date","loyalty_tier","is_active"] if c in customers]].copy()
    dim_product=products[[c for c in ["product_id","product_name","category","sub_category","brand","mrp","cost_price","unit","is_perishable","launch_date"] if c in products]].copy()
    dim_store=stores[[c for c in ["store_id","store_name","zone","area","pincode","open_date","capacity_orders_per_day"] if c in stores]].copy()
    # Joins are many-to-one after the documented deterministic dimension deduplication.
    # Resolve conflicting order versions before calculating financial metrics.
    # Business rule: RETURNED takes precedence over DELIVERED for the same order_id.
    if "order_id" in orders.columns and "order_status" in orders.columns:
        orders = orders.copy()

    orders["_status_priority"] = (
        orders["order_status"].astype(str).str.strip().str.upper()
        .map({"RETURNED": 2, "DELIVERED": 1})
        .fillna(0)
    )

    orders = (
        orders.sort_values("_status_priority", ascending=False)
        .drop_duplicates(subset=["order_id"], keep="first")
        .drop(columns=["_status_priority"])
    )
    fact=orders.merge(products[["product_id","cost_price"]].drop_duplicates("product_id"),on="product_id",how="left",suffixes=("","_master"))
    fact=fact.merge(stores[["store_id","zone","store_name"]].drop_duplicates("store_id"),on="store_id",how="left")
    fact["order_ts"]=pd.to_datetime(fact["order_ts"],errors="coerce")
    fact["order_date"]=fact["order_ts"].dt.date.astype("string")
    fact["net_revenue"]=0.0
    delivered=fact["order_status"].astype(str).str.upper().eq("DELIVERED")
    fact.loc[delivered,"net_revenue"]=(fact.loc[delivered,"quantity"]*fact.loc[delivered,"unit_price"]*(1-fact.loc[delivered,"discount_pct"]/100)).round(2)
    fact["gross_margin"]=0.0
    fact.loc[delivered,"gross_margin"]=(fact.loc[delivered,"net_revenue"]-fact.loc[delivered,"quantity"]*fact.loc[delivered,"cost_price"]).round(2)
    fact["within_15_min"]=fact["delivery_minutes"].le(15) & delivered
    fact_orders=fact[[c for c in ["order_id","order_date","order_ts","customer_id","product_id","store_id","zone","store_name","quantity","unit_price","discount_pct","order_amount","payment_mode","order_status","channel","delivery_minutes","net_revenue","gross_margin","within_15_min"] if c in fact]].copy()
    # Payment reconciliation by order; never expose raw gateway references in reporting table.
    payments["amount_paid"]=pd.to_numeric(payments["amount_paid"],errors="coerce")
    pay=payments.groupby("order_id",dropna=False,as_index=False)["amount_paid"].sum().rename(columns={"amount_paid":"total_paid"})
    expected=fact_orders[["order_id","net_revenue"]].groupby("order_id",as_index=False)["net_revenue"].sum().rename(columns={"net_revenue":"expected_amount"})
    recon=expected.merge(pay,on="order_id",how="outer")
    recon["expected_amount"]=recon["expected_amount"].fillna(0)
    recon["total_paid"]=recon["total_paid"].fillna(0)
    recon["unreconciled_amount"]=(recon["expected_amount"]-recon["total_paid"]).round(2)
    recon["reconciliation_status"]=recon["unreconciled_amount"].abs().le(0.02).map({True:"MATCHED",False:"UNRECONCILED"})
    # Reports
    daily=fact_orders[fact_orders["order_status"].astype(str).str.upper().eq("DELIVERED")].groupby(["order_date","zone","store_id","store_name"],dropna=False,as_index=False).agg(net_revenue=("net_revenue","sum"),orders=("order_id","nunique"))
    category=fact.merge(products[["product_id","category"]].drop_duplicates("product_id"),on="product_id",how="left",suffixes=("","_p"))
    catcol="category_p" if "category_p" in category.columns else "category"
    cat=category.groupby(catcol,dropna=False,as_index=False).agg(net_revenue=("net_revenue","sum"),gross_margin=("gross_margin","sum")).rename(columns={catcol:"category"})
    outputs={"fact_orders":fact_orders,"dim_customer":dim_customer,"dim_product":dim_product,"dim_store":dim_store,"recon":recon,"daily":daily,"category":cat}
    for key, df in outputs.items():
        df.to_csv(WORK_DIR/f"{rid}_{key}.csv", index=False)
    return {"run_id":rid,"outputs":{k:len(v) for k,v in outputs.items()},"rows_quarantined":int((validation_result or {}).get("failed",0)),"rows_warnings":int((validation_result or {}).get("warnings",0)),"rows_extracted":int((validation_result or {}).get("passed",0)) + int((validation_result or {}).get("failed",0))}


def ensure_audit_tables(con):
    con.execute("""CREATE TABLE IF NOT EXISTS dq_results(
        run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
        severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP)""")
    con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs(
        run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
        rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR)""")

def record_dq_result(con, run_id, dag_id, check_name, dimension, severity, checked, failed):
    status = "PASS" if int(failed) == 0 else ("WARN" if severity.upper() == "WARNING" else "FAIL")
    con.execute(
        "INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
        [str(run_id), dag_id, check_name, dimension, severity, int(checked), int(failed), status]
    )

def write_elt_quality_checks(con, run_id):
    """Persist one auditable check per required data-quality dimension."""
    ensure_audit_tables(con)
    total = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
    # Completeness: key fields required for an order record.
    completeness = con.execute("""SELECT count(*) FROM stg_orders
        WHERE nullif(trim(order_id), '') IS NULL OR nullif(trim(customer_id), '') IS NULL
           OR nullif(trim(product_id), '') IS NULL OR nullif(trim(store_id), '') IS NULL""").fetchone()[0]
    uniqueness = con.execute("""SELECT count(*) FROM (SELECT order_id FROM stg_orders
        GROUP BY order_id HAVING count(*) > 1)""").fetchone()[0]
    validity = con.execute("SELECT count(*) FROM stg_orders WHERE dq_status='FAIL'").fetchone()[0]
    accuracy = con.execute("""SELECT count(*) FROM stg_orders
        WHERE quantity IS NOT NULL AND unit_price IS NOT NULL AND discount_pct IS NOT NULL
          AND order_amount IS NOT NULL
          AND abs(order_amount - quantity * unit_price * (1 - discount_pct / 100.0)) > 0.02""").fetchone()[0]
    consistency = con.execute("""SELECT count(*) FROM stg_orders
        WHERE failure_reason LIKE '%missing_or_unknown_customer%'
           OR failure_reason LIKE '%missing_or_unknown_product%'
           OR failure_reason LIKE '%missing_or_unknown_store%'""").fetchone()[0]
    # Timeliness measures whether raw source records were loaded in the last 24 hours.
    raw_total = con.execute("SELECT count(*) FROM raw_orders").fetchone()[0]
    raw_stale = con.execute("""SELECT count(*) FROM raw_orders
        WHERE try_cast(_loaded_at AS TIMESTAMPTZ) IS NULL
           OR try_cast(_loaded_at AS TIMESTAMPTZ) < current_timestamp - INTERVAL '24 hours'""").fetchone()[0]
    checks = [
        ("required_order_fields_present", "completeness", "CRITICAL", total, completeness),
        ("order_id_unique_in_staging", "uniqueness", "CRITICAL", total, uniqueness),
        ("order_business_rules", "validity", "CRITICAL", total, validity),
        ("order_amount_formula_review", "accuracy", "WARNING", total, accuracy),
        ("order_master_references_resolve", "consistency", "CRITICAL", total, consistency),
        ("raw_orders_loaded_within_24h", "timeliness", "WARNING", raw_total, raw_stale),
    ]
    for name, dimension, severity, checked, failed in checks:
        record_dq_result(con, run_id, "nammamart_elt_indhu", name, dimension, severity, checked, failed)
    return {name: int(failed) for name, _, _, _, failed in checks}


def etl_load(transform_result, run_id):
    rid=safe_run_id(run_id)
    con=conn()
    try:
        tables={"fact_orders":"etl_fact_orders","dim_customer":"etl_dim_customer","dim_product":"etl_dim_product","dim_store":"etl_dim_store","recon":"etl_payment_reconciliation","daily":"etl_daily_zone_revenue","category":"etl_category_margin"}
        for key,table in tables.items():
            df=pd.read_csv(WORK_DIR/f"{rid}_{key}.csv")
            con.register("incoming_df",df)
            con.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM incoming_df')
            con.unregister("incoming_df")
        ensure_audit_tables(con)
        fact_rows = int(transform_result.get("outputs", {}).get("fact_orders", 0))
        extracted = int(transform_result.get("rows_extracted", 0))
        quarantined = int(transform_result.get("rows_quarantined", 0))
        completeness = con.execute("SELECT count(*) FROM etl_fact_orders WHERE order_id IS NULL OR customer_id IS NULL OR product_id IS NULL OR store_id IS NULL").fetchone()[0]
        uniqueness = con.execute("SELECT count(*) FROM (SELECT order_id FROM etl_fact_orders GROUP BY order_id HAVING count(*) > 1)").fetchone()[0]
        future_or_missing_ts = con.execute("SELECT count(*) FROM etl_fact_orders WHERE try_cast(order_ts AS TIMESTAMP) IS NULL OR try_cast(order_ts AS TIMESTAMP) > current_timestamp").fetchone()[0]
        consistency = con.execute("""SELECT count(*) FROM etl_fact_orders f
            LEFT JOIN etl_dim_customer c USING(customer_id) LEFT JOIN etl_dim_product p USING(product_id)
            LEFT JOIN etl_dim_store s USING(store_id)
            WHERE c.customer_id IS NULL OR p.product_id IS NULL OR s.store_id IS NULL""").fetchone()[0]
        warning_count = int(transform_result.get("rows_warnings", 0))
        checks = [
            ("required_fact_fields_present", "completeness", "CRITICAL", fact_rows, completeness),
            ("fact_order_id_unique", "uniqueness", "CRITICAL", fact_rows, uniqueness),
            ("source_order_validation", "validity", "CRITICAL", extracted, quarantined),
            ("order_amount_formula_review", "accuracy", "WARNING", extracted, warning_count),
            ("fact_master_references_resolve", "consistency", "CRITICAL", fact_rows, consistency),
            ("order_timestamps_not_future_or_missing", "timeliness", "WARNING", fact_rows, future_or_missing_ts),
        ]
        for name, dimension, severity, checked, failed in checks:
            record_dq_result(con, run_id, "nammamart_etl_indhu", name, dimension, severity, checked, failed)
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, current_timestamp, current_timestamp, ?, ?, ?, 'SUCCESS')",
                    [str(run_id), "nammamart_etl_indhu", extracted, fact_rows, quarantined])
    finally: con.close()
    return {k:len(v) for k,v in tables.items()}

def elt_load_raw(run_id):
    con=conn()
    loaded=0
    try:
        # Raw columns are VARCHAR; source values are not normalized or cleaned.
        mapping=[("customers","raw_customers",["customers"]),("orders","raw_orders",["orders","increment"]),("payments","raw_payments",["payments"]),("products","raw_products",["products"]),("stores","raw_stores",["stores"])]
        for key,table,names in mapping:
            for name in names:
                path=DATA_DIR/FILES[name]
                if not path.exists(): raise FileNotFoundError(path)
                df=pd.read_csv(path,dtype=str,keep_default_na=False)
                df["_loaded_at"]=datetime.now(timezone.utc).isoformat()
                df["_source_file"]=FILES[name]
                df["_run_id"]=str(run_id)
                # Idempotent reprocessing of the same source snapshot; corrections from increment remain distinct.
                con.execute(f'DELETE FROM "{table}" WHERE _source_file = ?', [FILES[name]]) if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [table]).fetchone()[0] else None
                if not con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [table]).fetchone()[0]:
                    cols = list(df.columns)
                    schema=", ".join('"'+c.replace('"','""')+'" VARCHAR' for c in cols)
                    con.execute(f'CREATE TABLE "{table}" ({schema})')
                # Recreate missing columns safely if source schema evolves; all source columns remain text.
                existing=[r[1] for r in con.execute(f'PRAGMA table_info("{table}")').fetchall()]
                for c in df.columns:
                    if c not in existing: con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" VARCHAR')
                con.register("raw_df",df)
                colsql=", ".join('"'+c+'"' for c in df.columns)
                con.execute(f'INSERT INTO "{table}" ({colsql}) SELECT {colsql} FROM raw_df')
                con.unregister("raw_df")
                loaded+=len(df)
        return loaded
    finally: con.close()

def elt_run_sql(run_id=None):
    con=conn()
    try:
        for filename in ["stg_dimensions.sql","stg_orders.sql","mart_daily_store_kpis.sql","mart_category_margin.sql","mart_delivery_sla.sql","mart_payment_reconciliation.sql","mart_top_customers.sql"]:
            sql_path=SQL_DIR/filename
            if not sql_path.exists(): raise FileNotFoundError(f"Missing SQL transformation: {sql_path}")
            con.execute(sql_path.read_text(encoding="utf-8"))
        write_elt_quality_checks(con, run_id or "manual_unknown")
        return {r[0]:r[1] for r in con.execute("SELECT table_name, estimated_size FROM duckdb_tables() WHERE table_name LIKE 'mart_%'").fetchall()}
    finally: con.close()
