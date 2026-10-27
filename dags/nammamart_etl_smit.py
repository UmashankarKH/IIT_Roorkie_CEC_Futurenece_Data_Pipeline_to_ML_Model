"""
dags/nammamart_etl_smit.py
--------------------------
NammaMart ETL pipeline: Extract -> Validate -> Transform -> Load (pandas + DuckDB)

Finance's rule: a bad row must never reach the CFO's tables. So every row is
checked in Python BEFORE the warehouse, and only rows that pass every critical
check are loaded. Failed rows go to a quarantine CSV with a failure_reason.

Flow:
  extract_stores ─────► validate_stores ────┐
  extract_products ───► validate_products ──┤
  extract_customers ──► validate_customers ─┼──► validate_orders ──► validate_payments
  extract_orders ───────────────────────────┤                                │
  extract_orders_increment ─────────────────┘                                ▼
  extract_payments ──────────────────────────────────────────────────► quarantine
                                                                              │
          transform ──► load ──► reconcile ──► report ──► dq_trend_chart ──► run_audit (all_done)

Tables written to include/warehouse_smit.duckdb:
  etl_fact_orders, etl_dim_customer, etl_dim_product, etl_dim_store,
  etl_payment_reconciliation, etl_daily_zone_revenue, etl_category_margin,
  etl_store_capacity, etl_quarantine, etl_loaded_files, dq_results, pipeline_runs
Plus include/reports/dq_trend_nammamart_etl_smit.html (bonus: quality trend chart).

Increment files: a scheduled run loads every orders_<date>_increment.csv dated
before today that hasn't been loaded yet (tracked in etl_loaded_files).
For a manual run, trigger with config {"increment_file": "orders_2026-10-01_increment.csv"}.
"""

import os
import re
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk import Param, dag, task

from include.nammamart_smit.config import (
    BASE_BUSINESS_DATE, QUARANTINE_DIR, SLA_MINUTES, as_of_for, business_date_for,
)
from include.nammamart_smit.files import from_json, pick_increment_files, read_source, to_json
from include.nammamart_smit.masking import mask_email, mask_name, mask_phone
from include.nammamart_smit import quality
from include.nammamart_smit.warehouse import (
    get_connection, last_row_count, save_dq_results, save_pipeline_run,
)

DAG_ID = "nammamart_etl_smit"

default_args = {
    "owner": "smit",
    "retries": 2,                                # a file landing a few minutes late shouldn't fail the run
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),  # a hung task must not block the 9:30 stand-up
}


def safe_run_id(run_id):
    # run_id has ':' and '+' in it, which are bad in file names
    return re.sub(r"[^A-Za-z0-9_-]", "_", run_id)


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                      # 9:00 AM, Monday to Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),  # cron is read in IST, not UTC
    catchup=False,                                               # don't backfill missed days, see SUBMISSION.md
    max_active_runs=1,                                           # two runs must never write the warehouse together
    default_args=default_args,
    params={"increment_file": Param(None, type=["null", "string"],
                                    description="Optional: orders_<date>_increment.csv to load in this run")},
    tags=["nammamart", "etl", "smit"],
)
def nammamart_etl_smit():

    # ── EXTRACT (run in parallel) ─────────────────────────────────────────────

    @task
    def extract_orders():
        return to_json(read_source("orders.csv", "orders"))

    @task
    def extract_orders_increment(params=None, dag_run=None):
        con = get_connection()
        con.execute("CREATE TABLE IF NOT EXISTS etl_loaded_files (file_name VARCHAR, run_id VARCHAR, loaded_at TIMESTAMP)")
        already_loaded = {row[0] for row in con.execute("SELECT file_name FROM etl_loaded_files").fetchall()}
        con.close()

        files = pick_increment_files(params.get("increment_file"), dag_run.run_after, already_loaded)
        return [{"file": f, "data": to_json(read_source(f, "orders"))} for f in files]

    @task
    def extract_payments():
        return to_json(read_source("payments.csv", "payments"))

    @task
    def extract_customers():
        return to_json(read_source("customers.csv", "customers"))

    @task
    def extract_products():
        return to_json(read_source("products.csv", "products"))

    @task
    def extract_stores():
        return to_json(read_source("stores.csv", "stores"))

    # ── VALIDATE: split every file into passed / failed rows ──────────────────

    @task
    def validate_stores(stores_json):
        df, results = quality.validate_stores(from_json(stores_json), as_of_for(BASE_BUSINESS_DATE))
        print(f"[Validate] stores: {(df['dq_status'] == 'PASSED').sum()} passed, {(df['dq_status'] == 'FAILED').sum()} failed")
        return {"data": to_json(df), "results": results}

    @task
    def validate_products(products_json):
        df, results = quality.validate_products(from_json(products_json), as_of_for(BASE_BUSINESS_DATE))
        print(f"[Validate] products: {(df['dq_status'] == 'PASSED').sum()} passed, {(df['dq_status'] == 'FAILED').sum()} failed")
        return {"data": to_json(df), "results": results}

    @task
    def validate_customers(customers_json):
        df, results = quality.validate_customers(from_json(customers_json), as_of_for(BASE_BUSINESS_DATE))
        print(f"[Validate] customers: {(df['dq_status'] == 'PASSED').sum()} passed, {(df['dq_status'] == 'FAILED').sum()} failed")
        return {"data": to_json(df), "results": results}

    @task
    def validate_orders(orders_json, increment, stores_v, products_v, customers_v):
        stores = from_json(stores_v["data"])
        products = from_json(products_v["data"])
        customers = from_json(customers_v["data"])

        # Orders may only point at dimension rows that passed their own checks
        good_stores = set(stores.loc[stores["dq_status"] == "PASSED", "store_id"])
        good_products = products[products["dq_status"] == "PASSED"]
        good_customers = set(customers.loc[customers["dq_status"] == "PASSED", "customer_id"])
        product_mrp = good_products.set_index("product_id")["mrp"].astype(float)

        base, results = quality.validate_orders(
            from_json(orders_json), as_of_for(BASE_BUSINESS_DATE), "orders.csv",
            good_customers, set(good_products["product_id"]), good_stores, product_mrp,
        )
        base["_source_file"] = "orders.csv"
        print(f"[Validate] orders.csv: {(base['dq_status'] == 'PASSED').sum()} passed, {(base['dq_status'] == 'FAILED').sum()} failed")

        batches = [base]
        for batch in increment:
            inc_file = batch["file"]
            inc, inc_results = quality.validate_orders(
                from_json(batch["data"]), as_of_for(business_date_for(inc_file)), inc_file,
                good_customers, set(good_products["product_id"]), good_stores, product_mrp,
            )
            inc["_source_file"] = inc_file
            results += inc_results
            batches.append(inc)
            print(f"[Validate] {inc_file}: {(inc['dq_status'] == 'PASSED').sum()} passed, {(inc['dq_status'] == 'FAILED').sum()} failed")

        return {"data": to_json(pd.concat(batches, ignore_index=True)), "results": results}

    @task
    def validate_payments(payments_json, orders_json, increment):
        # A payment is only "orphan" if the order system never sent that order_id at all
        known_orders = set(from_json(orders_json)["order_id"])
        for batch in increment:
            known_orders |= set(from_json(batch["data"])["order_id"])

        df, results = quality.validate_payments(from_json(payments_json), as_of_for(BASE_BUSINESS_DATE), known_orders)
        print(f"[Validate] payments: {(df['dq_status'] == 'PASSED').sum()} passed, {(df['dq_status'] == 'FAILED').sum()} failed")
        return {"data": to_json(df), "results": results}

    # ── QUARANTINE: failed rows to CSV + every check result to dq_results ─────

    @task
    def quarantine(stores_v, products_v, customers_v, orders_v, payments_v, run_id=None):
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        orders = from_json(orders_v["data"])

        files = {
            "stores.csv": (from_json(stores_v["data"]), "store_id"),
            "products.csv": (from_json(products_v["data"]), "product_id"),
            "customers.csv": (from_json(customers_v["data"]), "customer_id"),
            "payments.csv": (from_json(payments_v["data"]), "payment_id"),
        }
        for file_name in orders["_source_file"].unique():
            files[file_name] = (orders[orders["_source_file"] == file_name], "order_id")

        all_results = (stores_v["results"] + products_v["results"] + customers_v["results"]
                       + orders_v["results"] + payments_v["results"])

        con = get_connection()

        # File-level checks: freshness (orders + payments) and volume (every file)
        for file_name, (df, key) in files.items():
            previous = last_row_count(con, DAG_ID, file_name, run_id)
            all_results.append(quality.volume_check(file_name, len(df), previous))
        # Freshness looks at the newest believable timestamp (future-dated rows are already
        # caught by the *_not_future checks, they shouldn't make stale data look fresh)
        for file_name, (df, key) in files.items():
            if file_name.startswith("orders"):
                as_of = as_of_for(business_date_for(file_name))
                ts = quality.to_timestamp(df["order_ts"])
                all_results.append(quality.freshness_check(file_name, ts[ts <= as_of].max(), as_of))
        as_of = as_of_for(BASE_BUSINESS_DATE)
        ts = quality.to_timestamp(files["payments.csv"][0]["payment_ts"])
        all_results.append(quality.freshness_check("payments.csv", ts[ts <= as_of].max(), as_of))

        # Bonus: daily clean orders per store vs the store's capacity
        all_results.append(quality.store_capacity_check(orders, files["stores.csv"][0]))

        save_dq_results(con, run_id, DAG_ID, all_results)

        # Failed rows: one CSV per file, plus a table so "rows rejected today" is one query
        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_quarantine (
                run_id VARCHAR, source_file VARCHAR, row_key VARCHAR,
                failure_reason VARCHAR, raw_record VARCHAR, quarantined_at TIMESTAMP
            )
        """)
        con.execute("DELETE FROM etl_quarantine WHERE run_id = ?", [run_id])

        total_failed = 0
        for file_name, (df, key) in files.items():
            failed = df[df["dq_status"] == "FAILED"].drop(columns=["dq_status", "warnings"])
            failed = failed.drop(columns=[c for c in failed.columns if c.endswith("_std") or c == "_source_file"])
            if file_name == "customers.csv":
                # Ops reads quarantine files, so customer contact details are masked here too.
                # The row exactly as received is still in the CRM export (and raw_customers in ELT).
                failed["full_name"] = failed["full_name"].map(mask_name)
                failed["email"] = failed["email"].map(mask_email)
                failed["phone"] = failed["phone"].map(mask_phone)
            if len(failed) == 0:
                print(f"[Quarantine] {file_name}: no failed rows")
                continue

            stem = file_name.replace(".csv", "")
            path = f"{QUARANTINE_DIR}/{stem}_failed_{safe_run_id(run_id)}.csv"
            failed.to_csv(path, index=False)
            print(f"[Quarantine] {file_name}: {len(failed)} rows -> {path}")

            source_columns = [c for c in failed.columns if c != "failure_reason"]
            rows = pd.DataFrame({
                "run_id": run_id,
                "source_file": file_name,
                "row_key": failed[key].values,
                "failure_reason": failed["failure_reason"].values,
                "raw_record": failed[source_columns].to_json(orient="records", lines=True).splitlines(),
                "quarantined_at": pd.Timestamp.now("UTC").tz_localize(None),
            })
            con.execute("INSERT INTO etl_quarantine SELECT * FROM rows")
            total_failed += len(failed)

        con.close()
        print(f"[Quarantine] {total_failed} rows quarantined in total")
        return {"rows_quarantined": total_failed}

    # ── TRANSFORM: standardise, mask PII, join, derive measures ───────────────

    @task
    def transform(orders_v, customers_v, products_v, stores_v, run_id=None):
        orders = from_json(orders_v["data"])
        customers = from_json(customers_v["data"])
        products = from_json(products_v["data"])
        stores = from_json(stores_v["data"])

        # Orders that had no passing copy in this run. If the warehouse holds a
        # version of them from the same file, load() takes it out again.
        passed_ids = set(orders.loc[orders["dq_status"] == "PASSED", "order_id"])
        rejected = orders.loc[~orders["order_id"].isin(passed_ids), ["order_id", "_source_file"]].drop_duplicates()
        rejected["_batch_date"] = rejected["_source_file"].map(business_date_for)
        increment_files = [f for f in orders["_source_file"].unique() if f != "orders.csv"]

        # Only rows that passed every critical check. Duplicates were already
        # sent to quarantine by the uniqueness checks, so each key is now unique.
        orders = orders[orders["dq_status"] == "PASSED"].copy()
        customers = customers[customers["dq_status"] == "PASSED"].copy()
        products = products[products["dq_status"] == "PASSED"].copy()
        stores = stores[stores["dq_status"] == "PASSED"].copy()

        # Customer dimension: mask PII, fix casing
        dim_customer = pd.DataFrame({
            "customer_id": customers["customer_id"],
            "customer_name": customers["full_name"].map(mask_name),
            "email_masked": customers["email"].map(mask_email),
            "phone_masked": customers["phone"].map(mask_phone),
            "city": customers["city"].mask(customers["city"] == "", None),
            "area": customers["area"],
            "pincode": customers["pincode"],
            "signup_date": pd.to_datetime(customers["signup_date"]).dt.date,
            "loyalty_tier": customers["loyalty_tier"].str.strip().str.title(),
            "is_active": customers["is_active"] == "Y",
        })

        dim_product = pd.DataFrame({
            "product_id": products["product_id"],
            "product_name": products["product_name"],
            "category": products["category"],
            "sub_category": products["sub_category"],
            "brand": products["brand"],
            "mrp": products["mrp"].astype(float),
            "cost_price": products["cost_price"].astype(float),
            "unit": products["unit"],
            "is_perishable": products["is_perishable"] == "True",
            "launch_date": pd.to_datetime(products["launch_date"]).dt.date,
        })

        dim_store = pd.DataFrame({
            "store_id": stores["store_id"],
            "store_name": stores["store_name"],
            "zone": stores["zone"].str.strip().replace("", "Unknown"),
            "area": stores["area"],
            "pincode": stores["pincode"],
            "open_date": pd.to_datetime(stores["open_date"]).dt.date,
            "manager_name": stores["manager_name"],
            "capacity_orders_per_day": stores["capacity_orders_per_day"].astype(int),
        })

        # Fact: typed columns + derived measures
        fact = pd.DataFrame({
            "order_id": orders["order_id"],
            "order_ts": pd.to_datetime(orders["order_ts"]),
            "customer_id": orders["customer_id"],
            "store_id": orders["store_id"],
            "product_id": orders["product_id"],
            "quantity": orders["quantity"].astype(int),
            "unit_price": orders["unit_price"].astype(float),
            "discount_pct": orders["discount_pct"].astype(float),
            "order_amount": orders["order_amount"].astype(float),
            "payment_mode": orders["payment_mode_std"],
            "order_status": orders["order_status_std"],
            "channel": orders["channel"],
            "delivery_minutes": pd.to_numeric(orders["delivery_minutes"], errors="coerce"),
            "_source_file": orders["_source_file"],
        })
        fact["order_date"] = fact["order_ts"].dt.date

        # Join to the product dimension for cost (store/customer joins are guaranteed by the checks)
        fact = fact.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="left")

        delivered = fact["order_status"] == "DELIVERED"
        fact["net_revenue"] = fact["order_amount"].where(delivered, 0.0)
        fact["cost_amount"] = (fact["quantity"] * fact["cost_price"]).where(delivered, 0.0).round(2)
        fact["gross_margin"] = (fact["net_revenue"] - fact["cost_amount"]).round(2)
        fact["within_15_min"] = (fact["delivery_minutes"] <= SLA_MINUTES).where(delivered, None)
        fact["_batch_date"] = fact["_source_file"].map(business_date_for)
        fact["_run_id"] = run_id
        fact = fact.drop(columns=["cost_price"])

        print(f"[Transform] fact rows: {len(fact)} | customers: {len(dim_customer)} | "
              f"products: {len(dim_product)} | stores: {len(dim_store)}")

        return {
            "fact": fact.to_json(orient="records", date_format="iso"),
            "dim_customer": dim_customer.to_json(orient="records", date_format="iso"),
            "dim_product": dim_product.to_json(orient="records", date_format="iso"),
            "dim_store": dim_store.to_json(orient="records", date_format="iso"),
            "rejected": rejected[["order_id", "_batch_date"]].to_json(orient="records"),
            "increment_files": increment_files,
        }

    # ── LOAD: star schema in DuckDB, idempotent ───────────────────────────────

    @task
    def load(transformed, run_id=None):
        fact = from_json(transformed["fact"])
        dim_customer = from_json(transformed["dim_customer"])
        dim_product = from_json(transformed["dim_product"])
        dim_store = from_json(transformed["dim_store"])

        con = get_connection()

        # Dimensions are full snapshots from the masters, so replacing them is idempotent
        con.execute("""
            CREATE OR REPLACE TABLE etl_dim_customer AS
            SELECT customer_id, customer_name, email_masked, phone_masked, city, area, pincode,
                   CAST(signup_date AS DATE) AS signup_date, loyalty_tier, CAST(is_active AS BOOLEAN) AS is_active
            FROM dim_customer
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_dim_product AS
            SELECT product_id, product_name, category, sub_category, brand,
                   CAST(mrp AS DOUBLE) AS mrp, CAST(cost_price AS DOUBLE) AS cost_price, unit,
                   CAST(is_perishable AS BOOLEAN) AS is_perishable, CAST(launch_date AS DATE) AS launch_date
            FROM dim_product
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_dim_store AS
            SELECT store_id, store_name, zone, area, pincode, CAST(open_date AS DATE) AS open_date,
                   manager_name, CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day
            FROM dim_store
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS etl_fact_orders (
                order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, order_date DATE,
                customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
                quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
                payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes DOUBLE,
                net_revenue DOUBLE, cost_amount DOUBLE, gross_margin DOUBLE, within_15_min BOOLEAN,
                _source_file VARCHAR, _batch_date DATE, _run_id VARCHAR, _loaded_at TIMESTAMP
            )
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE incoming AS
            SELECT order_id, CAST(order_ts AS TIMESTAMP) AS order_ts, CAST(order_date AS DATE) AS order_date,
                   customer_id, store_id, product_id, CAST(quantity AS INTEGER) AS quantity,
                   CAST(unit_price AS DOUBLE) AS unit_price, CAST(discount_pct AS DOUBLE) AS discount_pct,
                   CAST(order_amount AS DOUBLE) AS order_amount, payment_mode, order_status, channel,
                   CAST(delivery_minutes AS DOUBLE) AS delivery_minutes, CAST(net_revenue AS DOUBLE) AS net_revenue,
                   CAST(cost_amount AS DOUBLE) AS cost_amount, CAST(gross_margin AS DOUBLE) AS gross_margin,
                   CAST(within_15_min AS BOOLEAN) AS within_15_min, _source_file,
                   CAST(_batch_date AS DATE) AS _batch_date, _run_id, now()::TIMESTAMP AS _loaded_at
            FROM fact
            -- the September file and the increment can both carry the same order: newest file wins
            QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY CAST(_batch_date AS DATE) DESC) = 1
        """)

        # How many rows are new, how many change an existing order (e.g. DELIVERED -> RETURNED)
        inserted = con.execute("""
            SELECT count(*) FROM incoming i
            WHERE i.order_id NOT IN (SELECT order_id FROM etl_fact_orders)
        """).fetchone()[0]
        updated = con.execute("""
            SELECT count(*) FROM incoming i JOIN etl_fact_orders f USING (order_id)
            WHERE i._batch_date >= f._batch_date
              AND (i.order_status <> f.order_status OR i.order_amount <> f.order_amount
                   OR i.delivery_minutes IS DISTINCT FROM f.delivery_minutes)
        """).fetchone()[0]

        # Upsert on order_id (INSERT OR REPLACE uses the primary key). Rows are only
        # written if they come from the same or a newer file than what's stored, so
        # re-loading the September file can never undo a correction from 1 October.
        con.execute("""
            INSERT OR REPLACE INTO etl_fact_orders
            SELECT i.order_id, i.order_ts, i.order_date, i.customer_id, i.store_id, i.product_id,
                   i.quantity, i.unit_price, i.discount_pct, i.order_amount, i.payment_mode, i.order_status,
                   i.channel, i.delivery_minutes, i.net_revenue, i.cost_amount, i.gross_margin, i.within_15_min,
                   i._source_file, i._batch_date, i._run_id, i._loaded_at
            FROM incoming i
            LEFT JOIN etl_fact_orders f USING (order_id)
            WHERE f.order_id IS NULL OR i._batch_date >= f._batch_date
        """)

        # A file can be re-sent with an order that now fails (e.g. a date that is now
        # in the future). The old version from that same file must not stay in the
        # finance table. A failed correction from a NEWER file doesn't remove anything:
        # the last good version stands.
        rejected = from_json(transformed["rejected"])
        removed = 0
        if len(rejected):
            removed = con.execute("""
                SELECT count(*) FROM etl_fact_orders f JOIN rejected r
                  ON f.order_id = r.order_id AND f._batch_date = CAST(r._batch_date AS DATE)
            """).fetchone()[0]
            con.execute("""
                DELETE FROM etl_fact_orders f USING rejected r
                WHERE f.order_id = r.order_id AND f._batch_date = CAST(r._batch_date AS DATE)
            """)

        # Remember which increment files are loaded, so scheduled runs don't redo them
        con.execute("CREATE TABLE IF NOT EXISTS etl_loaded_files (file_name VARCHAR, run_id VARCHAR, loaded_at TIMESTAMP)")
        for file_name in transformed["increment_files"]:
            con.execute("DELETE FROM etl_loaded_files WHERE file_name = ?", [file_name])
            con.execute("INSERT INTO etl_loaded_files VALUES (?, ?, now())", [file_name, run_id])

        total = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
        incoming = con.execute("SELECT count(*) FROM incoming").fetchone()[0]
        con.close()

        print(f"[Load] incoming {incoming} | inserted {inserted} | updated {updated} | "
              f"unchanged {incoming - inserted - updated} | removed {removed} | fact total {total}")
        return {"rows_loaded": incoming, "inserted": inserted, "updated": updated,
                "removed": removed, "fact_total": total}

    # ── RECONCILE: clean orders vs payment gateway ────────────────────────────

    @task
    def reconcile(load_result, payments_v):
        con = get_connection()
        orders = con.execute("""
            SELECT order_id, order_status, order_amount, payment_mode FROM etl_fact_orders
        """).fetchdf()

        # Every payment the gateway sent, one copy per payment_id (exact duplicates removed)
        payments = from_json(payments_v["data"])
        payments = payments.drop_duplicates("payment_id")
        payments = pd.DataFrame({
            "payment_id": payments["payment_id"],
            "order_id": payments["order_id"],
            "payment_status": payments["payment_status_std"],
            "gateway_payment_mode": payments["payment_mode_std"],
            "amount_paid": pd.to_numeric(payments["amount_paid"], errors="coerce"),
        })

        both = orders.merge(payments, on="order_id", how="outer")
        has_order = both["order_status"].notna()
        has_payment = both["payment_id"].notna()
        status = both["order_status"]
        pay = both["payment_status"]
        diff = (both["order_amount"] - both["amount_paid"]).abs()

        # One reason per row, checked in this order (first match wins)
        rules = [
            (~has_order, "payment_without_finance_order", both["amount_paid"]),
            ((status == "DELIVERED") & ~has_payment, "delivered_order_missing_payment", both["order_amount"]),
            ((status == "DELIVERED") & (pay == "FAILED"), "payment_failed_for_delivered_order", both["order_amount"]),
            ((status == "DELIVERED") & (pay == "REFUNDED"), "delivered_order_refunded", both["order_amount"]),
            ((status == "CANCELLED") & (pay == "SUCCESS"), "cancelled_order_has_payment", both["amount_paid"]),
            ((status == "RETURNED") & (pay == "SUCCESS"), "returned_order_not_refunded", both["amount_paid"]),
            ((status == "DELIVERED") & (pay == "SUCCESS") & (diff > 0.01), "amount_mismatch", diff),
            (has_order & has_payment & (both["payment_mode"] != both["gateway_payment_mode"]), "payment_mode_mismatch", 0.0),
        ]
        both["mismatch_reason"] = None
        both["unreconciled_amount"] = 0.0
        for condition, reason, amount in rules:
            pick = condition.fillna(False) & both["mismatch_reason"].isna()
            both.loc[pick, "mismatch_reason"] = reason
            both.loc[pick, "unreconciled_amount"] = (amount[pick] if isinstance(amount, pd.Series) else amount)

        mismatches = both[both["mismatch_reason"].notna()].copy()
        mismatches["unreconciled_amount"] = mismatches["unreconciled_amount"].abs().round(2)
        mismatches = mismatches[["order_id", "payment_id", "order_status", "payment_status", "payment_mode",
                                 "gateway_payment_mode", "order_amount", "amount_paid",
                                 "mismatch_reason", "unreconciled_amount"]]

        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM mismatches")
        summary = con.execute("""
            SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount
            FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC
        """).fetchdf()
        con.close()

        print(f"[Reconcile] {len(mismatches)} mismatches\n{summary.to_string(index=False)}")
        return {"mismatches": len(mismatches)}

    # ── REPORT: business-ready tables, printed in the log ─────────────────────

    @task
    def report(reconcile_result):
        con = get_connection()
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT f.order_date, s.zone, s.store_id, s.store_name,
                   count(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
                   round(sum(f.net_revenue), 2) AS net_revenue
            FROM etl_fact_orders f
            JOIN etl_dim_store s USING (store_id)
            GROUP BY ALL
            ORDER BY f.order_date, s.zone, s.store_id
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category,
                   count(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
                   round(sum(f.net_revenue), 2) AS net_revenue,
                   round(sum(f.cost_amount), 2) AS cost_amount,
                   round(sum(f.gross_margin), 2) AS gross_margin,
                   round(100 * sum(f.gross_margin) / nullif(sum(f.net_revenue), 0), 2) AS margin_pct
            FROM etl_fact_orders f
            JOIN etl_dim_product p USING (product_id)
            GROUP BY ALL
            ORDER BY net_revenue DESC
        """)
        # Bonus: how close each store runs to its daily capacity
        con.execute("""
            CREATE OR REPLACE TABLE etl_store_capacity AS
            WITH daily AS (
                SELECT store_id, order_date, count(*) AS orders
                FROM etl_fact_orders GROUP BY ALL
            )
            SELECT s.store_id, s.store_name, s.zone, s.capacity_orders_per_day,
                   count(*) AS days_with_orders,
                   round(avg(d.orders), 1) AS avg_daily_orders,
                   max(d.orders) AS max_daily_orders,
                   count(*) FILTER (WHERE d.orders > s.capacity_orders_per_day) AS days_over_capacity,
                   round(100.0 * max(d.orders) / nullif(s.capacity_orders_per_day, 0), 1) AS peak_utilisation_pct
            FROM daily d JOIN etl_dim_store s USING (store_id)
            GROUP BY 1, 2, 3, 4
            ORDER BY days_over_capacity DESC, peak_utilisation_pct DESC
        """)

        zone = con.execute("""
            SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
            FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY 3 DESC
        """).fetchdf()
        category = con.execute("SELECT * FROM etl_category_margin").fetchdf()
        total = con.execute("SELECT round(sum(net_revenue), 2) FROM etl_fact_orders").fetchone()[0]
        con.close()

        print(f"[Report] Net revenue by zone:\n{zone.to_string(index=False)}")
        print(f"[Report] Category margin:\n{category.to_string(index=False)}")
        print(f"[Report] Total net revenue (delivered orders): {total}")
        return {"net_revenue": total}

    # ── BONUS: data quality trend chart from dq_results ───────────────────────

    @task(trigger_rule="all_done", retries=0)
    def dq_trend_chart():
        from include.nammamart_smit.dq_trend import build_chart
        con = get_connection()
        path = build_chart(con, DAG_ID)
        con.close()
        return path

    # ── RUN AUDIT: always runs, writes one row to pipeline_runs ───────────────

    # retries=0: the audit reports a failed run once, it doesn't retry the failure
    @task(trigger_rule="all_done", retries=0)
    def run_audit(ti=None, dag_run=None, run_id=None):
        from airflow.sdk import Variable
        from include.slack_notifier import send_slack_alert

        steps = ["extract_orders", "extract_orders_increment", "extract_payments", "extract_customers",
                 "extract_products", "extract_stores", "validate_stores", "validate_products",
                 "validate_customers", "validate_orders", "validate_payments", "quarantine",
                 "transform", "load", "reconcile", "report"]
        outputs = {step: ti.xcom_pull(task_ids=step) for step in steps}
        not_finished = [step for step, value in outputs.items() if value is None]

        def count_rows(value):
            if value is None:
                return 0
            if isinstance(value, list):   # increment: a list of files
                return sum(len(from_json(batch["data"])) for batch in value)
            return len(from_json(value))

        rows_extracted = sum(count_rows(outputs[s]) for s in steps if s.startswith("extract"))
        rows_loaded = (outputs["load"] or {}).get("rows_loaded", 0)
        rows_quarantined = (outputs["quarantine"] or {}).get("rows_quarantined", 0)
        status = "success" if not not_finished else "failed"

        con = get_connection()
        save_pipeline_run(con, {
            "run_id": run_id, "dag_id": DAG_ID,
            "start_time": dag_run.start_date, "end_time": pendulum.now("UTC"),
            "rows_extracted": rows_extracted, "rows_loaded": rows_loaded,
            "rows_quarantined": rows_quarantined, "status": status,
            "failed_tasks": ", ".join(not_finished) or None,
        })
        con.close()
        print(f"[Audit] {status} | extracted {rows_extracted} | loaded {rows_loaded} | "
              f"quarantined {rows_quarantined} | not finished: {not_finished}")

        if status == "failed":
            message = (f":red_circle: {DAG_ID} run {run_id} FAILED.\n"
                       f"Tasks that did not finish: {', '.join(not_finished)}.\n"
                       f"Check the Airflow task logs before the 9:30 stand-up.")
            send_slack_alert(message, webhook_url=Variable.get("SLACK_WEBHOOK_URL", default=None))
            raise RuntimeError(f"[Audit] Run failed, tasks not finished: {not_finished}")

    # ── DAG WIRING ────────────────────────────────────────────────────────────

    orders = extract_orders()
    increment = extract_orders_increment()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()

    stores_v = validate_stores(stores)
    products_v = validate_products(products)
    customers_v = validate_customers(customers)
    orders_v = validate_orders(orders, increment, stores_v, products_v, customers_v)
    payments_v = validate_payments(payments, orders, increment)

    quarantined = quarantine(stores_v, products_v, customers_v, orders_v, payments_v)
    transformed = transform(orders_v, customers_v, products_v, stores_v)
    quarantined >> transformed

    loaded = load(transformed)
    reconciled = reconcile(loaded, payments_v)
    reported = report(reconciled)
    reported >> dq_trend_chart() >> run_audit()


nammamart_etl_smit()
