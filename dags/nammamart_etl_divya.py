"""
dags/nammamart_etl_divya.py
---------------------------
NammaMart ETL pipeline: Extract -> Validate -> Quarantine -> Transform -> Load
-> Incremental merge -> Reconcile -> Report -> Run audit.

  extract_orders ─────┐
  extract_payments ───┤
  extract_customers ──┼─► validate_* ─► quarantine ─► transform ─► load_warehouse ─► load_increment
  extract_products ───┤                                                                  │
  extract_stores ─────┤                                    reconcile_payments ◄──────────┘
  extract_increment ──┘                                          │
                                                          build_reports ─► run_audit (all_done)

Only rows that pass every Critical check reach etl_fact_orders (finance tables).
All transformation happens in pandas before the warehouse; DuckDB is the target.
Schedule: 09:00 IST, Monday-Saturday.
"""

import sys

sys.path.insert(0, "/usr/local/airflow/include")

import pandas as pd
from airflow.sdk import dag, task, get_current_context

from nammamart_divya import dq
from nammamart_divya.config import (
    DATA_DIR, INCREMENT_DIR, QUARANTINE_DIR, SLA_MINUTES, SOURCE_FILES, VOLUME_DROP_THRESHOLD,
    STALE_AFTER_DAYS,
)
from nammamart_divya.masking import mask_email, mask_phone
from nammamart_divya.runtime import (
    DEFAULT_ARGS, SCHEDULE_CRON, START_DATE, business_cutoff, notify_failure,
    read_source_csv, run_audit, work_dir,
)
from nammamart_divya.warehouse import (
    connect, now_utc, previous_order_volume, write_dq_results, write_pipeline_run,
)

DAG_ID = "nammamart_etl_divya"


def _ctx():
    ctx = get_current_context()
    return ctx["ti"].run_id, work_dir(DAG_ID, ctx["ti"].run_id)


def _save(df, name):
    _, wd = _ctx()
    path = wd / f"{name}.pkl"
    df.to_pickle(path)
    return str(path)


def _passed(df):
    return df[df["dq_status"] == "PASSED"]


@dag(
    dag_id=DAG_ID,
    description="NammaMart ETL: validate in Python, load only clean rows to a DuckDB star schema",
    schedule=SCHEDULE_CRON,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": notify_failure},
    tags=["nammamart", "etl", "divya"],
)
def nammamart_etl_divya():

    # ── EXTRACT: one parallel task per file ──────────────────────────────────
    def make_extract(file_key):
        @task(task_id=f"extract_{file_key}")
        def _extract():
            df = read_source_csv(DATA_DIR / SOURCE_FILES[file_key], file_key)
            print(f"[extract] {SOURCE_FILES[file_key]}: {len(df)} rows, columns {list(df.columns)}")
            return {"path": _save(df, f"raw_{file_key}"), "rows": len(df)}
        return _extract()

    raw = {key: make_extract(key) for key in SOURCE_FILES}

    @task
    def extract_increment():
        """Next-day feeds dropped into include/data/nammamart/increments/."""
        frames = []
        for path in sorted(INCREMENT_DIR.glob("orders_*_increment.csv")):
            df = read_source_csv(path, "orders")
            df["_source_file"] = path.name
            frames.append(df)
            print(f"[extract] increment {path.name}: {len(df)} rows")
        if not frames:
            print("[extract] no increment files found")
            return {"path": None, "rows": 0}
        df = pd.concat(frames, ignore_index=True)
        return {"path": _save(df, "raw_increment"), "rows": len(df)}

    # ── VALIDATE: split every file into passed / failed ──────────────────────
    def _record(df, file_key, name, source_file=None, extra_counts=None):
        run_id, _ = _ctx()
        counts = dq.row_check_counts(df, file_key)
        counts.update(extra_counts or {})
        with connect() as con:
            write_dq_results(con, run_id, DAG_ID, counts, source_file)
        failed = int((df["dq_status"] == "FAILED").sum())
        print(f"[validate] {name}: {len(df)} rows, {len(df) - failed} passed, {failed} failed")
        for check, (checked, nfail) in counts.items():
            if nfail:
                print(f"    {check}: {nfail} of {checked} rows")
        return {"path": _save(df, f"valid_{name}"), "rows": len(df),
                "passed": len(df) - failed, "failed": failed}

    @task
    def validate_stores(src):
        return _record(dq.validate_stores(pd.read_pickle(src["path"])), "stores", "stores")

    @task
    def validate_products(src):
        return _record(dq.validate_products(pd.read_pickle(src["path"]), business_cutoff()),
                       "products", "products")

    @task
    def validate_customers(src):
        return _record(dq.validate_customers(pd.read_pickle(src["path"]), business_cutoff()),
                       "customers", "customers")

    def _order_refs(stores, products, customers):
        st = pd.read_pickle(stores["path"])
        pr = _passed(pd.read_pickle(products["path"]))
        cu = pd.read_pickle(customers["path"])
        return set(_passed(st)["store_id"]), dict(zip(pr["product_id"], pr["mrp"])), set(cu["customer_id"].dropna())

    @task
    def validate_orders(src, stores, products, customers):
        run_id, _ = _ctx()
        cutoff = business_cutoff()
        df = dq.validate_orders(pd.read_pickle(src["path"]), cutoff, *_order_refs(stores, products, customers))

        # Dataset-level checks: volume vs previous run, freshness of the newest order.
        with connect() as con:
            previous = previous_order_volume(con, DAG_ID, run_id)
        dropped = previous is not None and len(df) < VOLUME_DROP_THRESHOLD * previous
        latest = df["order_ts"][df["order_ts"] < pd.Timestamp(cutoff)].max()
        stale = pd.isna(latest) or latest < pd.Timestamp(now_utc()) - pd.Timedelta(days=STALE_AFTER_DAYS)
        print(f"[validate] volume: {len(df)} rows (previous run {previous}); latest order_ts {latest}")
        if dropped:
            print(f"[WARNING] order volume dropped by more than 90% ({previous} -> {len(df)})")
        if stale:
            print(f"[WARNING] data is stale: latest order_ts {latest} is older than {STALE_AFTER_DAYS} day")
        extra = {"ord_volume_drop": (1, int(dropped)), "ord_freshness": (1, int(stale))}
        return _record(df, "orders", "orders", extra_counts=extra)

    @task
    def validate_increment(src, stores, products, customers):
        if not src["path"]:
            return {"path": None, "rows": 0, "passed": 0, "failed": 0}
        raw_inc = pd.read_pickle(src["path"])
        df = dq.validate_orders(raw_inc.drop(columns="_source_file"), business_cutoff(),
                                *_order_refs(stores, products, customers))
        df["_source_file"] = raw_inc["_source_file"].values
        return _record(df, "orders", "increment", source_file=",".join(df["_source_file"].unique()))

    @task
    def validate_payments(src, orders, increment):
        order_ids = set(pd.read_pickle(orders["path"])["order_id"])
        if increment["path"]:
            order_ids |= set(pd.read_pickle(increment["path"])["order_id"])
        df = dq.validate_payments(pd.read_pickle(src["path"]), business_cutoff(), order_ids)
        return _record(df, "payments", "payments")

    # ── QUARANTINE: failed rows + reason, never silently dropped ─────────────
    @task
    def quarantine(orders, increment, payments, customers, products, stores):
        validated = {"orders": orders, "increment": increment, "payments": payments,
                     "customers": customers, "products": products, "stores": stores}
        run_id, _ = _ctx()
        safe_run = work_dir(DAG_ID, run_id).name
        out_dir = QUARANTINE_DIR / "etl"
        out_dir.mkdir(parents=True, exist_ok=True)
        frames, total = [], 0
        for name, meta in validated.items():
            if not meta["path"]:
                continue
            df = pd.read_pickle(meta["path"])
            failed = df[df["dq_status"] == "FAILED"]
            if failed.empty:
                continue
            raw_cols = [c for c in df.columns if not c.startswith("f_") and c not in ("dq_warnings", "dq_status")]
            failed = failed[raw_cols]
            path = out_dir / f"{name}_failed_{safe_run}.csv"
            failed.to_csv(path, index=False)
            print(f"[quarantine] {len(failed)} {name} rows -> {path}")
            key_col = failed.columns[0]
            frames.append(pd.DataFrame({
                "run_id": run_id, "source": name, "row_key": failed[key_col].astype(str),
                "failure_reason": failed["failure_reason"],
                "row_json": failed.drop(columns="failure_reason").astype(str).to_json(orient="records", lines=True).splitlines(),
                "quarantined_at": now_utc(),
            }))
            total += len(failed)
        with connect() as con:
            con.execute("""CREATE TABLE IF NOT EXISTS etl_quarantine (run_id VARCHAR, source VARCHAR,
                           row_key VARCHAR, failure_reason VARCHAR, row_json VARCHAR, quarantined_at TIMESTAMP)""")
            con.execute("DELETE FROM etl_quarantine WHERE run_id = ?", [run_id])
            if frames:
                q = pd.concat(frames, ignore_index=True)
                con.register("q", q)
                con.execute("INSERT INTO etl_quarantine SELECT * FROM q")
        return {"rows_quarantined": total}

    # ── TRANSFORM: standardise, dedupe, mask PII, join, derive ───────────────
    @task
    def transform(orders, increment, customers, products, stores):
        cu = _passed(pd.read_pickle(customers["path"]))
        dim_customer = pd.DataFrame({
            "customer_id": cu["customer_id"],
            "customer_name_masked": cu["full_name"].map(
                lambda n: None if n is None else (n.split()[0] + (f" {n.split()[-1][0]}." if len(n.split()) > 1 else ""))),
            "email_masked": cu["email"].map(mask_email),
            "phone_masked": cu["phone"].map(mask_phone),
            "city": cu["city"], "area": cu["area"], "pincode": cu["pincode"],
            "signup_date": pd.to_datetime(cu["signup_date"]).dt.date,
            "loyalty_tier": cu["loyalty_tier"], "is_active": cu["is_active"],
        })
        pr = _passed(pd.read_pickle(products["path"]))
        dim_product = pr[["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                          "cost_price", "unit", "is_perishable", "launch_date"]].copy()
        dim_product["launch_date"] = dim_product["launch_date"].dt.date
        st = _passed(pd.read_pickle(stores["path"]))
        dim_store = st[["store_id", "store_name", "zone", "area", "pincode", "open_date",
                        "manager_name", "capacity_orders_per_day"]].copy()
        dim_store["open_date"] = dim_store["open_date"].dt.date

        def build_fact(df, source_files):
            df = _passed(df)
            fact = df[["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
                       "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
                       "channel", "delivery_minutes"]].copy()
            fact = fact.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="left")
            fact = fact.merge(dim_store[["store_id"]], on="store_id", how="inner")       # FK already validated
            fact = fact.merge(dim_customer[["customer_id"]], on="customer_id", how="inner")
            delivered = fact["order_status"].eq("DELIVERED")
            fact["order_date"] = fact["order_ts"].dt.date
            fact["quantity"] = fact["quantity"].astype(int)
            fact["net_revenue"] = fact["order_amount"].where(delivered, 0.0).round(2)
            fact["gross_margin"] = (fact["order_amount"] - fact["quantity"] * fact["cost_price"]).where(delivered, 0.0).round(2)
            fact["within_15_min"] = (fact["delivery_minutes"] <= SLA_MINUTES).where(delivered, None)
            fact["source_file"] = source_files
            if len(fact) != len(df):
                raise ValueError(f"Join dropped {len(df) - len(fact)} validated orders")
            return fact

        fact = build_fact(pd.read_pickle(orders["path"]), "orders.csv")
        inc_fact = None
        if increment["path"]:
            inc = pd.read_pickle(increment["path"])
            inc_fact = build_fact(inc.drop(columns="_source_file"), "")
            inc_fact["source_file"] = _passed(inc)["_source_file"].values
        print(f"[transform] dim_customer {len(dim_customer)}, dim_product {len(dim_product)}, "
              f"dim_store {len(dim_store)}, fact {len(fact)}, increment fact {0 if inc_fact is None else len(inc_fact)}")
        print("[transform] masked sample:\n" + dim_customer.head(3).to_string())
        return {
            "dim_customer": _save(dim_customer, "dim_customer"),
            "dim_product": _save(dim_product, "dim_product"),
            "dim_store": _save(dim_store, "dim_store"),
            "fact": _save(fact, "fact"),
            "inc_fact": _save(inc_fact, "inc_fact") if inc_fact is not None else None,
        }

    # ── LOAD: idempotent full refresh of the September snapshot ──────────────
    @task
    def load_warehouse(t):
        run_id, _ = _ctx()
        loaded_at = now_utc()
        with connect() as con:
            con.execute("BEGIN TRANSACTION")
            for table, key in [("etl_dim_customer", "dim_customer"), ("etl_dim_product", "dim_product"),
                               ("etl_dim_store", "dim_store")]:
                df = pd.read_pickle(t[key])
                con.register("src", df)
                con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM src")
                con.unregister("src")
                print(f"[load] {table}: {len(df)} rows")
            fact = pd.read_pickle(t["fact"])
            fact["_run_id"], fact["_loaded_at"] = run_id, loaded_at
            con.register("src", fact)
            # Replace, never append: re-running the DAG cannot double-count revenue.
            con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM src")
            con.execute("ALTER TABLE etl_fact_orders ADD PRIMARY KEY (order_id)")
            con.execute("COMMIT")
            n = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
        print(f"[load] etl_fact_orders: {n} rows")
        return {"fact_rows": n}

    # ── INCREMENTAL: upsert next-day orders and late status changes ──────────
    @task
    def load_increment(t, loaded):
        run_id, _ = _ctx()
        if not t["inc_fact"]:
            print("[increment] nothing to merge")
            return {"inserted": 0, "updated": 0, "fact_rows": loaded["fact_rows"]}
        inc = pd.read_pickle(t["inc_fact"])
        inc["_run_id"], inc["_loaded_at"] = run_id, now_utc()
        with connect() as con:
            con.register("inc", inc)
            updated = con.execute(
                "SELECT count(*) FROM inc WHERE order_id IN (SELECT order_id FROM etl_fact_orders)").fetchone()[0]
            changes = con.execute("""
                SELECT i.order_id, f.order_status AS old_status, i.order_status AS new_status
                FROM inc i JOIN etl_fact_orders f USING (order_id)
                WHERE f.order_status <> i.order_status""").df()
            con.execute("""
                MERGE INTO etl_fact_orders AS t
                USING inc AS s ON t.order_id = s.order_id
                WHEN MATCHED THEN UPDATE
                WHEN NOT MATCHED THEN INSERT
            """)
            n = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
        print(f"[increment] {len(inc)} rows merged: {len(inc) - updated} inserted, {updated} updated; fact now {n} rows")
        print("[increment] status changes:\n" + changes.to_string())
        return {"inserted": len(inc) - updated, "updated": int(updated), "fact_rows": n}

    # ── RECONCILE: every order/payment mismatch with a reason ────────────────
    @task
    def reconcile_payments(payments, merged):
        pay = _passed(pd.read_pickle(payments["path"]))
        with connect() as con:
            fact = con.execute("SELECT order_id, order_status, order_amount, payment_mode FROM etl_fact_orders").df()
        m = fact.merge(
            pay[["payment_id", "order_id", "payment_status", "amount_paid", "payment_mode"]],
            on="order_id", how="outer", suffixes=("_order", "_gateway"), indicator=True,
        )
        expected = m["order_amount"].where(m["order_status"].eq("DELIVERED"), 0.0).fillna(0.0)
        received = m["amount_paid"].where(m["payment_status"].eq("SUCCESS"), 0.0).fillna(0.0)
        m["expected_amount"] = expected.round(2)
        m["received_amount"] = received.round(2)
        m["difference"] = (expected - received).round(2)

        def reasons(r):
            out = []
            if r["_merge"] == "right_only":
                out.append("payment for an order not in finance table (quarantined or unknown)")
            elif r["_merge"] == "left_only":
                if r["order_status"] == "DELIVERED":
                    out.append("DELIVERED order has no payment")
                elif r["order_status"] == "RETURNED":
                    out.append("RETURNED order has no payment/refund record")
            else:
                st, ps = r["order_status"], r["payment_status"]
                if st == "DELIVERED" and ps == "FAILED":
                    out.append("DELIVERED order but payment FAILED")
                if st == "DELIVERED" and ps == "REFUNDED":
                    out.append("DELIVERED order but payment REFUNDED")
                if st == "RETURNED" and ps == "SUCCESS":
                    out.append("RETURNED order not yet refunded")
                if st == "CANCELLED" and ps == "SUCCESS":
                    out.append("CANCELLED order was charged")
                if st == "DELIVERED" and ps == "SUCCESS" and abs(r["difference"]) > 0.01:
                    out.append("amount_paid differs from order_amount")
                if r["payment_mode_order"] != r["payment_mode_gateway"]:
                    out.append("payment_mode differs between order and gateway")
            return "; ".join(out) or None

        m["reason"] = m.apply(reasons, axis=1)
        recon = m[m["reason"].notna()][[
            "order_id", "payment_id", "order_status", "payment_status", "payment_mode_order",
            "payment_mode_gateway", "order_amount", "amount_paid", "expected_amount",
            "received_amount", "difference", "reason"]].sort_values("order_id")
        with connect() as con:
            con.register("recon", recon)
            con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon")
        print(f"[reconcile] {len(recon)} mismatches; unreconciled amount = {recon['difference'].abs().sum():,.2f}")
        print(recon["reason"].value_counts().to_string())
        return {"mismatches": len(recon)}

    # ── REPORT ───────────────────────────────────────────────────────────────
    @task
    def build_reports(_recon):
        with connect() as con:
            fact = con.execute("SELECT * FROM etl_fact_orders").df()
            stores = con.execute("SELECT store_id, store_name, zone FROM etl_dim_store").df()
            products = con.execute("SELECT product_id, category FROM etl_dim_product").df()
            d = fact[fact["order_status"] == "DELIVERED"].merge(stores, on="store_id").merge(products, on="product_id")
            zone = (d.groupby(["order_date", "zone", "store_id", "store_name"], as_index=False)
                     .agg(delivered_orders=("order_id", "count"), net_revenue=("net_revenue", "sum"),
                          gross_margin=("gross_margin", "sum")))
            cat = (d.groupby("category", as_index=False)
                    .agg(delivered_orders=("order_id", "count"), quantity=("quantity", "sum"),
                         revenue=("net_revenue", "sum"), gross_margin=("gross_margin", "sum")))
            cat["cost"] = (cat["revenue"] - cat["gross_margin"]).round(2)
            cat["margin_pct"] = (100 * cat["gross_margin"] / cat["revenue"]).round(2)
            for frame in (zone, cat):
                for col in ("net_revenue", "revenue", "gross_margin"):
                    if col in frame:
                        frame[col] = frame[col].round(2)
            con.register("zone_df", zone)
            con.register("cat_df", cat)
            con.execute("CREATE OR REPLACE TABLE etl_daily_zone_revenue AS SELECT * FROM zone_df ORDER BY order_date, zone, store_id")
            con.execute("CREATE OR REPLACE TABLE etl_category_margin AS SELECT * FROM cat_df ORDER BY revenue DESC")
        by_zone = zone.groupby("zone")["net_revenue"].sum().round(2)
        print("[report] Net revenue by zone (delivered, after discount):\n" + by_zone.to_string())
        print(f"[report] Total net revenue: {zone['net_revenue'].sum():,.2f}")
        print("[report] Category margin:\n" + cat.to_string(index=False))
        return {"net_revenue": round(float(zone["net_revenue"].sum()), 2)}

    # ── RUN AUDIT: always runs, one row per run in pipeline_runs ─────────────
    @task(trigger_rule="all_done", retries=0)
    def audit():
        ti = get_current_context()["ti"]
        extract_ids = [f"extract_{k}" for k in SOURCE_FILES] + ["extract_increment"]
        extracted = sum((ti.xcom_pull(task_ids=t) or {}).get("rows", 0) for t in extract_ids)
        merged = ti.xcom_pull(task_ids="load_increment") or ti.xcom_pull(task_ids="load_warehouse") or {}
        quarantined = (ti.xcom_pull(task_ids="quarantine") or {}).get("rows_quarantined")

        def write(row):
            with connect() as con:
                write_pipeline_run(con, row)

        run_audit(DAG_ID, {"rows_extracted": extracted, "rows_loaded": merged.get("fact_rows"),
                           "rows_quarantined": quarantined}, write)

    inc_raw = extract_increment()
    v_stores = validate_stores(raw["stores"])
    v_products = validate_products(raw["products"])
    v_customers = validate_customers(raw["customers"])
    v_orders = validate_orders(raw["orders"], v_stores, v_products, v_customers)
    v_inc = validate_increment(inc_raw, v_stores, v_products, v_customers)
    v_payments = validate_payments(raw["payments"], v_orders, v_inc)
    q = quarantine(orders=v_orders, increment=v_inc, payments=v_payments,
                   customers=v_customers, products=v_products, stores=v_stores)
    t = transform(v_orders, v_inc, v_customers, v_products, v_stores)
    q >> t
    loaded = load_warehouse(t)
    merged = load_increment(t, loaded)
    recon = reconcile_payments(v_payments, merged)
    report = build_reports(recon)
    report >> audit()


nammamart_etl_divya()
