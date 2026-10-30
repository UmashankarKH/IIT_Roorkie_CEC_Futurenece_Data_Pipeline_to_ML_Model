"""NammaMart ETL pipeline: validate in Python, then load finance-ready tables."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule


INCLUDE_PATH = Path(__file__).resolve().parents[1] / "include"
sys.path.insert(0, str(INCLUDE_PATH))

from nammamart_common import (  # noqa: E402
    DATA_DIR,
    QUARANTINE_DIR,
    WAREHOUSE_DB,
    append_dq_results,
    ensure_audit_tables,
    frame_from_json,
    frame_to_json,
    mask_customer_frame,
    read_source,
    safe_run_id,
    standardize_payment_mode,
    validate_frame,
)


DAG_ID = "nammamart_etl_Rajan_Gupta"

DEFAULT_ARGS = {
    "owner": "nammamart-fde",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=2),
}


def _source_result(filename: str) -> dict:
    path = DATA_DIR / filename
    frame = read_source(path)
    return {"path": str(path), "filename": filename, "rows": len(frame)}


def _all_order_sources() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base = read_source(DATA_DIR / "orders.csv")
    base["_source_file"] = "orders.csv"
    base["_source_row_number"] = range(2, len(base) + 2)
    increment_path = DATA_DIR / "orders_2026-10-01_increment.csv"
    increment = read_source(increment_path) if increment_path.exists() else pd.DataFrame(columns=base.columns)
    if len(increment):
        increment["_source_file"] = increment_path.name
        increment["_source_row_number"] = range(2, len(increment) + 2)
    all_orders = pd.concat([base, increment], ignore_index=True, sort=False)
    all_orders["order_id"] = all_orders.get("order_id", "").astype(str).str.strip()
    all_orders = all_orders.drop_duplicates("order_id", keep="last")
    return base, increment, all_orders


@dag(
    dag_id=DAG_ID,
    description="NammaMart validated ETL into an idempotent DuckDB star schema",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "assignment", "etl"],
)
def nammamart_etl_Rajan_Gupta():
    @task
    def extract_orders():
        result = _source_result("orders.csv")
        increment = DATA_DIR / "orders_2026-10-01_increment.csv"
        if increment.exists():
            result["increment"] = _source_result(increment.name)
        return result

    @task
    def extract_payments():
        return _source_result("payments.csv")

    @task
    def extract_customers():
        return _source_result("customers.csv")

    @task
    def extract_products():
        return _source_result("products.csv")

    @task
    def extract_stores():
        return _source_result("stores.csv")

    @task
    def validate(
        order_source: dict,
        payment_source: dict,
        customer_source: dict,
        product_source: dict,
        store_source: dict,
    ) -> dict:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        run_id = context["run_id"]
        now = pendulum.now("UTC")

        customer_raw = read_source(customer_source["path"])
        product_raw = read_source(product_source["path"])
        store_raw = read_source(store_source["path"])
        payment_raw = read_source(payment_source["path"])
        base_raw = read_source(order_source["path"])
        base_raw["_source_file"] = "orders.csv"
        base_raw["_source_row_number"] = range(2, len(base_raw) + 2)
        increment_raw = pd.DataFrame(columns=base_raw.columns)
        if order_source.get("increment"):
            increment_raw = read_source(order_source["increment"]["path"])
            increment_raw["_source_file"] = order_source["increment"]["filename"]
            increment_raw["_source_row_number"] = range(2, len(increment_raw) + 2)

        customer_pass, customer_fail, customer_checks = validate_frame(
            "customers", "customers.csv", customer_raw, checked_at=now
        )
        product_pass, product_fail, product_checks = validate_frame(
            "products", "products.csv", product_raw, checked_at=now
        )
        store_pass, store_fail, store_checks = validate_frame(
            "stores", "stores.csv", store_raw, checked_at=now
        )

        all_order_refs = pd.concat([base_raw, increment_raw], ignore_index=True, sort=False)
        all_order_refs["order_id"] = all_order_refs.get("order_id", "").astype(str).str.strip()
        all_order_refs = all_order_refs.drop_duplicates("order_id", keep="last")
        payment_pass, payment_fail, payment_checks = validate_frame(
            "payments", "payments.csv", payment_raw, orders=all_order_refs, checked_at=now
        )

        orders_passed = []
        failures: dict[str, str] = {}
        checks = customer_checks + product_checks + store_checks + payment_checks
        order_rows = order_source["rows"] + (order_source.get("increment") or {}).get("rows", 0)
        extracted_rows = (
            order_source["rows"]
            + (order_source.get("increment") or {}).get("rows", 0)
            + payment_source["rows"]
            + customer_source["rows"]
            + product_source["rows"]
            + store_source["rows"]
        )

        for name, filename, raw in (
            ("orders", "orders.csv", base_raw),
            ("orders_increment", "orders_2026-10-01_increment.csv", increment_raw),
        ):
            if name == "orders_increment" and not order_source.get("increment"):
                continue
            passed, failed, order_checks = validate_frame(
                name,
                filename,
                raw,
                customers=customer_pass,
                products=product_pass,
                stores=store_pass,
                payments=payment_raw,
                valid_payments=payment_pass,
                checked_at=now,
            )
            passed["_source_file"] = filename
            orders_passed.append(passed)
            checks.extend(order_checks)
            if not failed.empty:
                failures[filename] = failed

        if not payment_fail.empty:
            failures["payments.csv"] = payment_fail
        if not customer_fail.empty:
            failures["customers.csv"] = mask_customer_frame(customer_fail)
        if not product_fail.empty:
            failures["products.csv"] = product_fail
        if not store_fail.empty:
            safe_store_fail = store_fail.drop(columns=["manager_name"], errors="ignore")
            failures["stores.csv"] = safe_store_fail

        passed_frames = {
            "orders": pd.concat(orders_passed, ignore_index=True, sort=False),
            "payments": payment_pass,
            "customers": mask_customer_frame(customer_pass),
            "products": product_pass,
            "stores": store_pass.drop(columns=["manager_name"], errors="ignore"),
        }

        run_dir = DATA_DIR / ".runtime" / safe_run_id(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        passed_paths = {}
        for key, frame in passed_frames.items():
            path = run_dir / f"{key}_passed.json"
            path.write_text(frame_to_json(frame), encoding="utf-8")
            passed_paths[key] = str(path)
        failed_paths = {}
        for filename, frame in failures.items():
            path = run_dir / f"{Path(filename).stem}_failed.json"
            path.write_text(frame_to_json(frame), encoding="utf-8")
            failed_paths[filename] = str(path)

        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            ensure_audit_tables(con)
            previous_volume = con.execute(
                """
                SELECT rows_checked
                FROM dq_results
                WHERE dag_id = ? AND check_name = 'etl_orders_source_volume_not_down_90_percent'
                  AND run_id <> ?
                ORDER BY checked_at DESC
                LIMIT 1
                """,
                [DAG_ID, run_id],
            ).fetchone()
            volume_failed = int(
                previous_volume is not None
                and previous_volume[0] > 0
                and order_rows < previous_volume[0] * 0.10
            )
            checks.append(
                {
                    "check_name": "etl_orders_source_volume_not_down_90_percent",
                    "dimension": "Volume",
                    "severity": "Warning",
                    "rows_checked": order_rows,
                    "rows_failed": volume_failed,
                }
            )
            append_dq_results(con, run_id, DAG_ID, checks)
        finally:
            con.close()

        quarantined_rows = sum(len(frame) for frame in failures.values())
        return {
            "passed": passed_paths,
            "failed": failed_paths,
            "counts": {
                "rows_extracted": int(extracted_rows),
                "rows_quarantined": int(quarantined_rows),
                "rows_passed_orders": sum(len(frame) for frame in orders_passed),
            },
            "run_id": run_id,
        }

    @task
    def quarantine(validation_result: dict) -> int:
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        run_key = safe_run_id(validation_result["run_id"])
        rows_written = 0
        quarantine_records = []
        key_columns = {
            "orders": "order_id",
            "payments": "payment_id",
            "customers": "customer_id",
            "products": "product_id",
            "stores": "store_id",
        }
        for filename, payload in validation_result["failed"].items():
            failed = frame_from_json(Path(payload).read_text(encoding="utf-8"))
            if failed.empty:
                continue
            if "customers" in filename:
                failed = mask_customer_frame(failed)
            if "stores" in filename:
                failed = failed.drop(columns=["manager_name"], errors="ignore")
            stem = Path(filename).stem
            output = QUARANTINE_DIR / f"{stem}_failed_{run_key}.csv"
            failed.to_csv(output, index=False)
            rows_written += len(failed)
            key_column = key_columns.get(stem.replace("_2026-10-01_increment", ""), "")
            for _, row in failed.iterrows():
                quarantine_records.append({
                    "run_id": validation_result["run_id"],
                    "dag_id": DAG_ID,
                    "source_file": filename,
                    "row_key": str(row.get(key_column, "")) if key_column else "",
                    "failure_reason": str(row.get("failure_reason", "")),
                    "record_json": row.to_json(force_ascii=False),
                    "quarantined_at": pendulum.now("UTC").to_iso8601_string(),
                })
            print(f"[Quarantine] {len(failed)} rows: {output.name}")
        if rows_written == 0:
            print("[Quarantine] No critical failures in this run.")
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS etl_quarantine (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    source_file VARCHAR,
                    row_key VARCHAR,
                    failure_reason VARCHAR,
                    record_json JSON,
                    quarantined_at TIMESTAMPTZ
                )
                """
            )
            con.execute("DELETE FROM etl_quarantine WHERE run_id = ? AND dag_id = ?", [validation_result["run_id"], DAG_ID])
            if quarantine_records:
                incoming = pd.DataFrame(quarantine_records)
                con.register("incoming_quarantine", incoming)
                con.execute("INSERT INTO etl_quarantine SELECT * FROM incoming_quarantine")
                con.unregister("incoming_quarantine")
        finally:
            con.close()
        return rows_written

    @task
    def transform(validation_result: dict) -> dict:
        passed = validation_result["passed"]
        orders = frame_from_json(Path(passed["orders"]).read_text(encoding="utf-8"))
        customers = frame_from_json(Path(passed["customers"]).read_text(encoding="utf-8"))
        products = frame_from_json(Path(passed["products"]).read_text(encoding="utf-8"))
        stores = frame_from_json(Path(passed["stores"]).read_text(encoding="utf-8"))

        if orders.empty:
            raise ValueError("No order rows passed critical validation; fact table cannot be built.")

        orders["_source_priority"] = orders["_source_file"].map(
            lambda value: 1 if "increment" in str(value) else 0
        )
        orders = orders.sort_values(["_source_priority", "order_id"]).drop_duplicates(
            "order_id", keep="last"
        )
        for column in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"):
            orders[column] = pd.to_numeric(orders[column], errors="coerce")
        orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce", utc=True)
        orders["order_date"] = orders["order_ts"].dt.date

        for column in ("mrp", "cost_price"):
            products[column] = pd.to_numeric(products[column], errors="coerce")

        customers = customers.drop_duplicates("customer_id", keep="first")
        products = products.drop_duplicates("product_id", keep="first")
        stores = stores.drop_duplicates("store_id", keep="first")

        customer_dim = customers[[
            column for column in (
                "customer_id", "email", "phone", "loyalty_tier"
            ) if column in customers
        ]].rename(columns={"email": "masked_email", "phone": "masked_phone"})
        product_dim = products[[
            column for column in (
                "product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable"
            ) if column in products
        ]]
        store_dim = stores[[
            column for column in ("store_id", "store_name", "zone", "area", "pincode", "capacity_orders_per_day")
            if column in stores
        ]]

        merged = orders.merge(customers[["customer_id", "loyalty_tier"]], on="customer_id", how="left")
        merged = merged.merge(products[["product_id", "product_name", "category", "cost_price"]], on="product_id", how="left")
        merged = merged.merge(stores[["store_id", "store_name", "zone"]], on="store_id", how="left")
        delivered = merged["order_status"].eq("DELIVERED")
        merged["net_revenue"] = merged["order_amount"].where(delivered, 0.0)
        merged["gross_margin"] = (
            merged["order_amount"] - merged["quantity"] * merged["cost_price"]
        ).where(delivered, 0.0)
        merged["within_15_min"] = delivered & merged["delivery_minutes"].le(15)
        pending_payment = merged.get("warning_reason", pd.Series("", index=merged.index)).astype(str).str.contains(
            "no successful payment has arrived", regex=False
        )
        merged["payment_reconciliation_status"] = "NOT_REQUIRED"
        merged.loc[delivered & ~pending_payment, "payment_reconciliation_status"] = "RECONCILED"
        merged.loc[delivered & pending_payment, "payment_reconciliation_status"] = "PENDING"

        fact_columns = [
            "order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id",
            "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
            "channel", "delivery_minutes", "loyalty_tier", "product_name", "category", "cost_price",
            "store_name", "zone", "net_revenue", "gross_margin", "within_15_min",
            "payment_reconciliation_status", "_source_file", "_source_row_number",
        ]
        fact = merged[fact_columns].rename(columns={"loyalty_tier": "customer_loyalty_tier"})
        payload = {
            "fact": frame_to_json(fact),
            "customer_dim": frame_to_json(customer_dim),
            "product_dim": frame_to_json(product_dim),
            "store_dim": frame_to_json(store_dim),
        }
        print(f"[Transform] {len(fact)} validated, deduplicated orders prepared.")
        return payload

    @task
    def load_to_warehouse(transformed: dict) -> dict:
        frames = {
            "etl_fact_orders": frame_from_json(transformed["fact"]),
            "etl_dim_customer": frame_from_json(transformed["customer_dim"]),
            "etl_dim_product": frame_from_json(transformed["product_dim"]),
            "etl_dim_store": frame_from_json(transformed["store_dim"]),
        }
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            ensure_audit_tables(con)
            con.execute("BEGIN TRANSACTION")
            for table in ("etl_dim_customer", "etl_dim_product", "etl_dim_store"):
                incoming = frames[table]
                con.register("incoming_dimension", incoming)
                con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM incoming_dimension")
                con.unregister("incoming_dimension")

            facts = frames["etl_fact_orders"]
            con.register("incoming_facts", facts)
            con.execute(
                "CREATE TABLE IF NOT EXISTS etl_fact_orders AS "
                "SELECT * FROM incoming_facts WHERE FALSE"
            )
            con.execute(
                "DELETE FROM etl_fact_orders WHERE order_id IN "
                "(SELECT order_id FROM incoming_facts)"
            )
            con.execute("INSERT INTO etl_fact_orders SELECT * FROM incoming_facts")
            con.unregister("incoming_facts")
            con.execute("COMMIT")
            total = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
            print(f"[Load] Upserted {len(facts)} fact rows; current fact count is {total}.")
            return {"rows_loaded": int(len(facts)), "fact_rows": int(total)}
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    @task
    def reconcile(order_source: dict, payment_source: dict) -> int:
        base, increment, _ = _all_order_sources()
        orders = pd.concat([base, increment], ignore_index=True, sort=False)
        orders["order_id"] = orders["order_id"].astype(str).str.strip()
        orders = orders.drop_duplicates("order_id", keep="last")
        orders_by_id = orders.set_index("order_id", drop=False).to_dict("index")
        payments = read_source(payment_source["path"])
        mismatches: list[dict] = []
        seen_success: set[str] = set()

        for _, payment in payments.iterrows():
            order_id = str(payment.get("order_id", "")).strip()
            order = orders_by_id.get(order_id)
            reason = None
            order_amount = None
            if order is None:
                reason = "payment references an unknown order"
            else:
                order_amount = pd.to_numeric(pd.Series([order.get("order_amount")]), errors="coerce").iloc[0]
                payment_amount = pd.to_numeric(pd.Series([payment.get("amount_paid")]), errors="coerce").iloc[0]
                status = str(payment.get("payment_status", "")).strip().upper()
                order_status = str(order.get("order_status", "")).strip().upper()
                if order_status == "CANCELLED":
                    reason = "payment exists for a cancelled order"
                if status == "SUCCESS":
                    seen_success.add(order_id)
                    payment_mode = standardize_payment_mode(payment.get("payment_mode", ""))
                    order_mode = standardize_payment_mode(order.get("payment_mode", ""))
                    if order_status == "CANCELLED":
                        reason = "payment exists for a cancelled order"
                    elif pd.isna(pd.to_datetime(payment.get("payment_ts"), errors="coerce", utc=True)) or pd.to_datetime(payment.get("payment_ts"), errors="coerce", utc=True) > pendulum.now("UTC"):
                        reason = "successful payment timestamp is invalid or in the future"
                    elif pd.isna(order_amount) or pd.isna(payment_amount) or abs(float(order_amount) - float(payment_amount)) > 0.01:
                        reason = "successful payment amount differs from order_amount"
                    elif payment_mode != order_mode:
                        reason = "successful payment mode differs from order"
                    elif order_status == "CANCELLED":
                        reason = "successful payment exists for a cancelled order"
            if reason:
                mismatches.append({
                    "order_id": order_id,
                    "payment_id": payment.get("payment_id"),
                    "payment_status": payment.get("payment_status"),
                    "order_amount": order_amount,
                    "amount_paid": payment.get("amount_paid"),
                    "payment_mode": payment.get("payment_mode"),
                    "reason": reason,
                })

        for order_id, order in orders_by_id.items():
            if str(order.get("order_status", "")).strip().upper() == "DELIVERED" and order_id not in seen_success:
                mismatches.append({
                    "order_id": order_id,
                    "payment_id": None,
                    "payment_status": None,
                    "order_amount": order.get("order_amount"),
                    "amount_paid": None,
                    "payment_mode": order.get("payment_mode"),
                    "reason": "delivered order has no successful matching payment",
                })

        result = pd.DataFrame(mismatches, columns=[
            "order_id", "payment_id", "payment_status", "order_amount", "amount_paid", "payment_mode", "reason"
        ])
        result["reconciliation_amount"] = result.apply(
            lambda row: abs(float(row["order_amount"]) - float(row["amount_paid"]))
            if row["reason"] == "successful payment amount differs from order_amount"
            and pd.notna(row["order_amount"]) and pd.notna(row["amount_paid"])
            else (float(row["amount_paid"]) if pd.notna(row["amount_paid"]) else (float(row["order_amount"]) if pd.notna(row["order_amount"]) else 0.0)),
            axis=1,
        )
        result["checked_at"] = pendulum.now("UTC").to_iso8601_string()
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            con.register("reconciliation_input", result)
            con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM reconciliation_input")
            con.unregister("reconciliation_input")
        finally:
            con.close()
        print(f"[Reconcile] {len(result)} payment/order mismatches recorded.")
        return len(result)

    @task
    def report() -> None:
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            con.execute(
                """
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT order_date, zone, store_id, store_name,
                       COUNT(*) AS delivered_orders,
                       ROUND(SUM(net_revenue), 2) AS net_revenue
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED'
                GROUP BY order_date, zone, store_id, store_name
                ORDER BY order_date, zone, store_id
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT category, ROUND(SUM(net_revenue), 2) AS net_revenue,
                       ROUND(SUM(gross_margin), 2) AS gross_margin
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED'
                GROUP BY category
                ORDER BY category
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TABLE etl_delivery_sla AS
                SELECT store_id, store_name, zone, COUNT(*) AS delivered_orders,
                       ROUND(AVG(delivery_minutes), 2) AS avg_delivery_minutes,
                       ROUND(100.0 * AVG(CASE WHEN delivery_minutes <= 15 THEN 1.0 ELSE 0.0 END), 2) AS pct_within_15_min
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED' AND delivery_minutes IS NOT NULL
                GROUP BY store_id, store_name, zone
                ORDER BY store_id
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TABLE etl_payment_mode_revenue AS
                SELECT payment_mode, COUNT(*) AS delivered_orders,
                       ROUND(SUM(net_revenue), 2) AS net_revenue
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED'
                GROUP BY payment_mode
                ORDER BY payment_mode
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TABLE etl_top_customers AS
                SELECT customer_id, customer_loyalty_tier AS loyalty_tier,
                       COUNT(*) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS total_spend
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED'
                GROUP BY customer_id, customer_loyalty_tier
                ORDER BY total_spend DESC, customer_id
                LIMIT 10
                """
            )
            print("[Report] Daily net revenue by zone and store:")
            print(con.execute("SELECT * FROM etl_daily_zone_revenue").fetchdf().to_string(index=False))
            print("[Report] Revenue and gross margin by category:")
            print(con.execute("SELECT * FROM etl_category_margin").fetchdf().to_string(index=False))
            print("[Report] Delivery SLA:")
            print(con.execute("SELECT * FROM etl_delivery_sla").fetchdf().to_string(index=False))
            print("[Report] Revenue by payment mode:")
            print(con.execute("SELECT * FROM etl_payment_mode_revenue").fetchdf().to_string(index=False))
            print("[Report] Top customers (customer IDs only):")
            print(con.execute("SELECT * FROM etl_top_customers").fetchdf().to_string(index=False))
            print("[Report] Payment reconciliation:")
            print(con.execute("SELECT COUNT(*) AS mismatches, ROUND(SUM(reconciliation_amount), 2) AS amount FROM etl_payment_reconciliation").fetchdf().to_string(index=False))
        finally:
            con.close()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run() -> None:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        dag_run: DagRun = context["dag_run"]
        ti = context["ti"]
        run_id = dag_run.run_id

        extracted = 0
        for task_id in ("extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores"):
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                extracted += int(result.get("rows", 0))
                extracted += int((result.get("increment") or {}).get("rows", 0))
        validation_result = ti.xcom_pull(task_ids="validate") or {}
        loaded = ti.xcom_pull(task_ids="load_to_warehouse") or {}
        quarantined = ti.xcom_pull(task_ids="quarantine") or 0

        run_states = [
            task_instance.state
            for task_instance in dag_run.get_task_instances()
            if task_instance.task_id != "audit_run"
        ]
        failed_states = {"failed", "upstream_failed"}
        status = "FAILED" if any(state in failed_states for state in run_states) else "SUCCESS"

        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            ensure_audit_tables(con)
            con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
            con.execute(
                "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    DAG_ID,
                    dag_run.start_date or pendulum.now("UTC"),
                    pendulum.now("UTC"),
                    extracted or int(validation_result.get("counts", {}).get("rows_extracted", 0)),
                    int(loaded.get("rows_loaded", 0)),
                    int(quarantined),
                    status,
                ],
            )
        finally:
            con.close()
        print(f"[Audit] run_id={run_id} status={status} extracted={extracted} loaded={loaded.get('rows_loaded', 0)} quarantined={quarantined}")

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()

    checked = validate(orders, payments, customers, products, stores)
    quarantined = quarantine(checked)
    transformed = transform(checked)
    loaded = load_to_warehouse(transformed)
    reconciled = reconcile(orders, payments)
    reports = report()
    quarantined >> loaded
    loaded >> reconciled >> reports

    audit = audit_run()
    [quarantined, reports] >> audit


nammamart_etl_Rajan_Gupta()
