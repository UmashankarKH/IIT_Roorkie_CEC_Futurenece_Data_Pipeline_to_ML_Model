"""
NammaMart ETL pipeline — validate in Python, load star schema to DuckDB.
"""

import json
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/usr/local/airflow/include")

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from nammamart_audit import ensure_audit_tables, write_dq_results, write_pipeline_run
from nammamart_config import (
    DATA_DIR,
    ETL_DAG_ID,
    MASTER_FILES,
    QUARANTINE_DIR,
    SCHEDULE_CRON,
    TIMEZONE,
    WAREHOUSE_DB,
)
from nammamart_dq import (
    validate_customers,
    validate_orders,
    validate_payments,
    validate_products,
    validate_stores,
)
from nammamart_transforms import (
    mask_email,
    mask_phone,
    parse_float,
    parse_timestamp,
    standardize_loyalty_tier,
    standardize_order_status,
    standardize_payment_mode,
)
default_args = {
    "owner": "udaykiran.komati",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _run_id() -> str:
    return pendulum.now(TIMEZONE).format("YYYYMMDD_HHmmss")


@dag(
    dag_id=ETL_DAG_ID,
    schedule=SCHEDULE_CRON,
    start_date=pendulum.datetime(2026, 9, 1, tz=TIMEZONE),
    catchup=False,
    default_args=default_args,
    tags=["fde", "nammamart", "etl"],
    doc_md="NammaMart ETL: extract → validate → quarantine → transform → DuckDB star schema.",
)
def nammamart_etl_udaykiran_komati():
    @task
    def extract_file(file_key: str) -> str:
        path = os.path.join(DATA_DIR, MASTER_FILES[file_key])
        df = pd.read_csv(path, dtype=str)
        print(f"[Extract] {file_key}: {len(df)} rows from {path}")
        return df.to_json()

    @task
    def validate_and_quarantine(
        orders_json: str,
        payments_json: str,
        customers_json: str,
        products_json: str,
        stores_json: str,
    ) -> str:
        run_id = _run_id()
        os.makedirs(QUARANTINE_DIR, exist_ok=True)

        orders = pd.read_json(orders_json)
        payments = pd.read_json(payments_json)
        customers = pd.read_json(customers_json)
        products = pd.read_json(products_json)
        stores = pd.read_json(stores_json)

        stores_pass, stores_fail, dq_stores = validate_stores(stores)
        products_pass, products_fail, dq_products = validate_products(products)
        customers_pass, customers_fail, dq_customers = validate_customers(customers)

        order_ids = set(orders["order_id"].astype(str))
        order_amounts = {
            row["order_id"]: parse_float(row["order_amount"]) or 0.0
            for _, row in orders.iterrows()
        }
        payments_pass, payments_fail, dq_payments = validate_payments(
            payments, order_ids, order_amounts
        )

        orders_pass, orders_fail, dq_orders = validate_orders(
            orders,
            set(customers["customer_id"].astype(str)),
            set(products["product_id"].astype(str)),
            set(stores["store_id"].astype(str)),
        )

        failed_files = {
            "orders": orders_fail,
            "payments": payments_fail,
            "customers": customers_fail,
            "products": products_fail,
            "stores": stores_fail,
        }
        for file_name, failed in failed_files.items():
            if failed.empty:
                continue
            out = os.path.join(QUARANTINE_DIR, f"{file_name}_failed_{run_id}.csv")
            failed.to_csv(out, index=False)
            print(f"[Quarantine] {len(failed)} rows -> {out}")

        dq_records = dq_stores + dq_products + dq_customers + dq_payments + dq_orders
        con = duckdb.connect(WAREHOUSE_DB)
        ensure_audit_tables(con)
        write_dq_results(con, run_id, ETL_DAG_ID, dq_records)
        con.close()

        payload = {
            "run_id": run_id,
            "passed": {
                "orders": orders_pass.to_json(),
                "payments": payments_pass.to_json(),
                "customers": customers_pass.to_json(),
                "products": products_pass.to_json(),
                "stores": stores_pass.to_json(),
            },
            "failed_counts": {k: len(v) for k, v in failed_files.items()},
            "rows_extracted": len(orders) + len(payments) + len(customers) + len(products) + len(stores),
            "rows_quarantined": sum(len(v) for v in failed_files.values()),
        }
        return json.dumps(payload)

    @task
    def transform_and_load(validated_json: str) -> str:
        payload = json.loads(validated_json)
        run_id = payload["run_id"]

        orders = pd.read_json(payload["passed"]["orders"])
        payments = pd.read_json(payload["passed"]["payments"])
        customers = pd.read_json(payload["passed"]["customers"])
        products = pd.read_json(payload["passed"]["products"])
        stores = pd.read_json(payload["passed"]["stores"])

        customers["loyalty_tier"] = customers["loyalty_tier"].apply(standardize_loyalty_tier)
        customers["email_masked"] = customers["email"].apply(mask_email)
        customers["phone_masked"] = customers["phone"].apply(mask_phone)

        orders["payment_mode"] = orders["payment_mode"].apply(standardize_payment_mode)
        orders["order_status"] = orders["order_status"].apply(standardize_order_status)
        orders["order_ts_parsed"] = orders["order_ts"].apply(parse_timestamp)
        orders["order_date"] = orders["order_ts_parsed"].apply(
            lambda dt: dt.date().isoformat() if dt else None
        )
        orders["quantity_num"] = orders["quantity"].apply(parse_float)
        orders["unit_price_num"] = orders["unit_price"].apply(parse_float)
        orders["discount_pct_num"] = orders["discount_pct"].apply(parse_float)
        orders["order_amount_num"] = orders["order_amount"].apply(parse_float)
        orders["delivery_minutes_num"] = orders["delivery_minutes"].apply(parse_float)
        orders["within_15_min"] = orders["delivery_minutes_num"].apply(
            lambda v: v is not None and v <= 15
        )

        fact = orders.merge(customers, on="customer_id", how="left", suffixes=("", "_cust"))
        fact = fact.merge(products, on="product_id", how="left", suffixes=("", "_prod"))
        fact = fact.merge(stores, on="store_id", how="left", suffixes=("", "_store"))

        fact["net_revenue"] = fact.apply(
            lambda r: r["order_amount_num"] if r["order_status"] == "DELIVERED" else 0.0,
            axis=1,
        )
        fact["gross_margin"] = fact.apply(
            lambda r: (
                (r["order_amount_num"] or 0)
                - (r["quantity_num"] or 0) * (parse_float(r.get("cost_price")) or 0)
                if r["order_status"] == "DELIVERED"
                else 0.0
            ),
            axis=1,
        )
        fact["run_id"] = run_id

        dim_customer = customers[
            ["customer_id", "full_name", "email_masked", "phone_masked", "city", "area", "loyalty_tier", "is_active"]
        ].copy()
        dim_product = products[
            ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit"]
        ].copy()
        dim_store = stores[
            ["store_id", "store_name", "zone", "area", "pincode", "capacity_orders_per_day"]
        ].copy()

        fact_cols = [
            "order_id",
            "order_ts",
            "order_date",
            "customer_id",
            "store_id",
            "product_id",
            "quantity_num",
            "unit_price_num",
            "discount_pct_num",
            "order_amount_num",
            "payment_mode",
            "order_status",
            "channel",
            "delivery_minutes_num",
            "within_15_min",
            "net_revenue",
            "gross_margin",
            "zone",
            "category",
            "email_masked",
            "phone_masked",
            "run_id",
        ]
        fact_out = fact[fact_cols].rename(
            columns={
                "quantity_num": "quantity",
                "unit_price_num": "unit_price",
                "discount_pct_num": "discount_pct",
                "order_amount_num": "order_amount",
                "delivery_minutes_num": "delivery_minutes",
            }
        )

        con = duckdb.connect(WAREHOUSE_DB)
        con.register("fact_out", fact_out)
        con.register("dim_customer", dim_customer)
        con.register("dim_product", dim_product)
        con.register("dim_store", dim_store)
        con.register("payments_df", payments)

        for table in (
            "etl_fact_orders",
            "etl_dim_customer",
            "etl_dim_product",
            "etl_dim_store",
            "etl_payment_reconciliation",
            "etl_daily_zone_revenue",
            "etl_category_margin",
        ):
            con.execute(f"DROP TABLE IF EXISTS {table}")

        con.execute("CREATE TABLE etl_fact_orders AS SELECT * FROM fact_out")
        con.execute("CREATE TABLE etl_dim_customer AS SELECT * FROM dim_customer")
        con.execute("CREATE TABLE etl_dim_product AS SELECT * FROM dim_product")
        con.execute("CREATE TABLE etl_dim_store AS SELECT * FROM dim_store")

        con.execute(
            """
            CREATE TABLE etl_payment_reconciliation AS
            SELECT *
            FROM (
                SELECT
                    o.order_id,
                    o.order_amount AS order_amount,
                    p.amount_paid,
                    p.payment_status,
                    p.payment_mode,
                    CASE
                        WHEN p.payment_id IS NULL THEN 'missing payment row'
                        WHEN upper(p.payment_status) = 'SUCCESS'
                             AND abs(cast(p.amount_paid AS DOUBLE) - cast(o.order_amount AS DOUBLE)) > 0.02
                            THEN 'amount mismatch'
                        WHEN upper(p.payment_status) NOT IN ('SUCCESS', 'REFUNDED', 'FAILED')
                            THEN 'unexpected payment status'
                        ELSE 'matched'
                    END AS mismatch_reason
                FROM etl_fact_orders o
                LEFT JOIN payments_df p ON o.order_id = p.order_id
            ) recon
            WHERE mismatch_reason != 'matched'
            """
        )

        con.execute(
            """
            CREATE TABLE etl_daily_zone_revenue AS
            SELECT
                order_date,
                zone,
                store_id,
                round(sum(net_revenue), 2) AS net_revenue
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """
        )
        con.execute(
            """
            CREATE TABLE etl_category_margin AS
            SELECT
                category,
                round(sum(net_revenue), 2) AS revenue,
                round(sum(gross_margin), 2) AS gross_margin
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY 1
            ORDER BY revenue DESC
            """
        )

        zone_rev = con.execute("SELECT * FROM etl_daily_zone_revenue LIMIT 10").fetchdf()
        cat_margin = con.execute("SELECT * FROM etl_category_margin").fetchdf()
        print("[Report] etl_daily_zone_revenue sample:\n", zone_rev.to_string(index=False))
        print("[Report] etl_category_margin:\n", cat_margin.to_string(index=False))

        row_count = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.close()
        print(f"[Load] etl_fact_orders rows: {row_count}")
        return json.dumps({"run_id": run_id, "rows_loaded": row_count})

    @task
    def load_increment(validated_json: str, load_meta_json: str) -> str:
        """Upsert validated increment orders into etl_fact_orders by order_id."""
        path = os.path.join(DATA_DIR, "orders_2026-10-01_increment.csv")
        if not os.path.exists(path):
            print("[Increment] file not found, skipping")
            return load_meta_json

        base_payload = json.loads(validated_json)
        customers = pd.read_json(base_payload["passed"]["customers"])
        products = pd.read_json(base_payload["passed"]["products"])
        stores = pd.read_json(base_payload["passed"]["stores"])
        inc = pd.read_csv(path, dtype=str)

        inc_pass, inc_fail, _ = validate_orders(
            inc,
            set(customers["customer_id"].astype(str)),
            set(products["product_id"].astype(str)),
            set(stores["store_id"].astype(str)),
            as_of=datetime(2026, 10, 1, 23, 59, 59),
        )
        if not inc_fail.empty:
            out = os.path.join(QUARANTINE_DIR, f"orders_increment_failed_{base_payload['run_id']}.csv")
            inc_fail.to_csv(out, index=False)

        inc_pass["payment_mode"] = inc_pass["payment_mode"].apply(standardize_payment_mode)
        inc_pass["order_status"] = inc_pass["order_status"].apply(standardize_order_status)
        inc_pass["order_date"] = inc_pass["order_ts"].apply(
            lambda v: parse_timestamp(v).date().isoformat() if parse_timestamp(v) else None
        )
        for col in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"):
            inc_pass[col] = inc_pass[col].apply(parse_float)

        fact_inc = inc_pass.merge(customers, on="customer_id", how="left")
        fact_inc = fact_inc.merge(products, on="product_id", how="left", suffixes=("", "_prod"))
        fact_inc = fact_inc.merge(stores, on="store_id", how="left", suffixes=("", "_store"))
        fact_inc["within_15_min"] = fact_inc["delivery_minutes"].apply(
            lambda v: v is not None and v <= 15
        )
        fact_inc["net_revenue"] = fact_inc.apply(
            lambda r: r["order_amount"] if r["order_status"] == "DELIVERED" else 0.0,
            axis=1,
        )
        fact_inc["gross_margin"] = fact_inc.apply(
            lambda r: (
                (r["order_amount"] or 0)
                - (r["quantity"] or 0) * (parse_float(r.get("cost_price")) or 0)
                if r["order_status"] == "DELIVERED"
                else 0.0
            ),
            axis=1,
        )
        fact_inc["email_masked"] = fact_inc["email"].apply(mask_email)
        fact_inc["phone_masked"] = fact_inc["phone"].apply(mask_phone)
        fact_inc["run_id"] = base_payload["run_id"]

        con = duckdb.connect(WAREHOUSE_DB)
        existing = con.execute("SELECT * FROM etl_fact_orders").fetchdf()
        con.register("inc_fact", fact_inc)
        con.register("existing_fact", existing)
        con.execute(
            """
            CREATE OR REPLACE TABLE etl_fact_orders AS
            SELECT * FROM existing_fact
            WHERE order_id NOT IN (SELECT order_id FROM inc_fact)
            UNION ALL
            SELECT
                order_id, order_ts, order_date, customer_id, store_id, product_id,
                quantity, unit_price, discount_pct, order_amount, payment_mode, order_status,
                channel, delivery_minutes, within_15_min, net_revenue, gross_margin,
                zone, category, email_masked, phone_masked, run_id
            FROM inc_fact
            """
        )
        count = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.close()
        print(f"[Increment] upserted {len(fact_inc)} rows; fact count now {count}")
        return load_meta_json

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def pipeline_run_audit(validated_json: str, load_meta_json: str) -> None:
        try:
            validated = json.loads(validated_json)
        except json.JSONDecodeError:
            validated = {"run_id": _run_id(), "rows_extracted": 0, "rows_quarantined": 0}
        try:
            load_meta = json.loads(load_meta_json)
        except json.JSONDecodeError:
            load_meta = {"rows_loaded": 0}

        end = datetime.utcnow()
        start = end - timedelta(minutes=5)
        con = duckdb.connect(WAREHOUSE_DB)
        write_pipeline_run(
            con,
            run_id=validated.get("run_id", _run_id()),
            dag_id=ETL_DAG_ID,
            start_time=start,
            end_time=end,
            rows_extracted=int(validated.get("rows_extracted", 0)),
            rows_loaded=int(load_meta.get("rows_loaded", 0)),
            rows_quarantined=int(validated.get("rows_quarantined", 0)),
            status="success",
        )
        con.close()

    orders_j = extract_file.override(task_id="extract_orders")("orders")
    payments_j = extract_file.override(task_id="extract_payments")("payments")
    customers_j = extract_file.override(task_id="extract_customers")("customers")
    products_j = extract_file.override(task_id="extract_products")("products")
    stores_j = extract_file.override(task_id="extract_stores")("stores")

    validated = validate_and_quarantine(orders_j, payments_j, customers_j, products_j, stores_j)
    loaded_meta = transform_and_load(validated)
    increment = load_increment(validated, loaded_meta)
    audit = pipeline_run_audit(validated, loaded_meta)

    [orders_j, payments_j, customers_j, products_j, stores_j] >> validated
    validated >> loaded_meta >> increment >> audit


nammamart_etl_udaykiran_komati()
