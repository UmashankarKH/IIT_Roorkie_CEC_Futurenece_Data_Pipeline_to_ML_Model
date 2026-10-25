"""
dags/nammamart_elt_rudin.py
--------------------------
NammaMart ELT pipeline.

Flow:
  CSV exports -> raw DuckDB tables -> SQL staging -> SQL marts

Raw tables retain every source value as VARCHAR together with load metadata.
All cleaning, casting, masking, deduplication, and quality status logic is
kept in SQL files under include/sql/.
"""

import os
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sdk import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from include.slack_notifier import send_slack_alert


DATA_DIR = "/usr/local/airflow/include/data/nammamart"
SQL_DIR = "/usr/local/airflow/include/sql"
# Keep ELT storage separate from the ETL warehouse so scheduled or manual runs
# can execute side by side without DuckDB file-lock contention.
WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_rudin_elt.duckdb"
ETL_WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_rudin.duckdb"

REQUIRED_COLUMNS = {
    "orders.csv": {"order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"},
    "orders_2026-10-01_increment.csv": {"order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"},
    "payments.csv": {"payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"},
    "customers.csv": {"customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"},
    "products.csv": {"product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"},
    "stores.csv": {"store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"},
}

default_args = {
    "owner": "fde-cohort",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _execute_sql(con, filename: str, run_id: str = "", dag_id: str = "", as_of_date: str = "") -> None:
    """Read a version-controlled SQL transformation and execute it in DuckDB."""
    with open(os.path.join(SQL_DIR, filename), encoding="utf-8") as sql_file:
        statement = (
            sql_file.read()
            .replace("{{ run_id }}", run_id)
            .replace("{{ dag_id }}", dag_id)
            .replace("{{ as_of_date }}", as_of_date)
        )
    con.execute(statement)


def _as_of_date() -> str:
    """Return the reference date used by the freshness checks.

    Defaults to the current run date in Asia/Kolkata and can be pinned with
    the NAMMAMART_AS_OF_DATE environment variable so a historical run can be
    reproduced exactly.
    """
    configured = os.environ.get("NAMMAMART_AS_OF_DATE")
    if configured:
        return configured
    return pendulum.now("Asia/Kolkata").to_date_string()


def _assert_required_columns(frame: pd.DataFrame, filename: str) -> None:
    """Fail before raw loading when a source contract changes."""
    missing = REQUIRED_COLUMNS[filename] - set(frame.columns)
    if missing:
        raise ValueError(f"Schema validation failed for {filename}; missing columns: {sorted(missing)}")


def _load_csv_to_raw(filename: str, table_name: str) -> int:
    """Append an untouched CSV snapshot to a raw table with audit metadata."""
    context = get_current_context()
    run_id = context["run_id"]
    frame = pd.read_csv(f"{DATA_DIR}/{filename}", dtype=str, keep_default_na=False)
    _assert_required_columns(frame, filename)
    frame["_loaded_at"] = pendulum.now("UTC").to_iso8601_string()
    frame["_source_file"] = filename
    frame["_run_id"] = run_id

    con = duckdb.connect(WAREHOUSE_DB)
    con.register("raw_source_df", frame)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM raw_source_df WHERE 1=0")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM raw_source_df")
    con.close()
    print(f"[Raw] {filename}: appended {len(frame)} rows to {table_name}")
    return len(frame)


@dag(
    dag_id="nammamart_elt_rudin",
    description="NammaMart SQL ELT pipeline; supports scheduled and manual runs.",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fde", "nammamart", "elt"],
)
def nammamart_elt_rudin():
    # -------------------- RAW: append every source record --------------------
    @task
    def load_raw_orders():
        return _load_csv_to_raw("orders.csv", "raw_orders")

    @task
    def load_raw_payments():
        return _load_csv_to_raw("payments.csv", "raw_payments")

    @task
    def load_raw_customers():
        return _load_csv_to_raw("customers.csv", "raw_customers")

    @task
    def load_raw_products():
        return _load_csv_to_raw("products.csv", "raw_products")

    @task
    def load_raw_stores():
        return _load_csv_to_raw("stores.csv", "raw_stores")

    @task
    def load_raw_increment():
        return _load_csv_to_raw("orders_2026-10-01_increment.csv", "raw_orders")

    # -------------------- STAGING: SQL-only transformations ------------------
    @task
    def build_staging(*raw_loads):
        """Run all staging casts, standardization, deduplication, and DQ logic."""
        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        _execute_sql(con, "nammamart_elt_staging.sql", context["run_id"], context["dag"].dag_id, _as_of_date())
        con.close()
        print(f"[Staging] SQL transformations complete for {len(raw_loads)} raw loads")
        return True

    @task
    def apply_order_upsert(staging_complete):
        """MERGE the latest raw order version into persistent staging."""
        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        inserted_count, updated_count = con.execute("""
            WITH increment_ids AS (
                SELECT DISTINCT order_id
                FROM raw_orders
                WHERE _source_file = 'orders_2026-10-01_increment.csv'
            ), base_ids AS (
                SELECT DISTINCT order_id
                FROM raw_orders
                WHERE _source_file = 'orders.csv'
            )
            SELECT
                COUNT(*) FILTER (WHERE b.order_id IS NULL),
                COUNT(*) FILTER (WHERE b.order_id IS NOT NULL)
            FROM increment_ids i
            LEFT JOIN base_ids b USING (order_id)
        """).fetchone()
        _execute_sql(con, "nammamart_elt_orders_upsert.sql", context["run_id"], context["dag"].dag_id, _as_of_date())
        con.execute("""
            CREATE TABLE IF NOT EXISTS elt_incremental_audit (
                run_id VARCHAR, source_file VARCHAR, inserted_count BIGINT,
                updated_count BIGINT, recorded_at TIMESTAMP
            )
        """)
        con.execute("INSERT INTO elt_incremental_audit VALUES (?, ?, ?, ?, ?)", [
            context["run_id"], "orders_2026-10-01_increment.csv",
            inserted_count, updated_count, pendulum.now("UTC"),
        ])
        counts = con.execute("SELECT COUNT(*) AS total_orders, COUNT(*) FILTER (WHERE order_status = 'RETURNED') AS returned_orders FROM stg_orders").fetchone()
        print(f"[Staging][MERGE] stg_orders={counts[0]} rows; returned={counts[1]}; inserted={inserted_count}; updated={updated_count}")
        con.close()
        return staging_complete

    # -------------------- DQ: persist SQL quality scorecard ------------------
    @task
    def record_dq_results(staging_complete):
        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        _execute_sql(con, "nammamart_elt_dq_results.sql", context["run_id"], context["dag"].dag_id, _as_of_date())
        print(con.execute("SELECT * FROM dq_results WHERE run_id = ?", [context["run_id"]]).fetchdf().to_string(index=False))
        con.close()
        return staging_complete

    # -------------------- MART: SQL-only business tables ---------------------
    @task
    def build_marts(dq_complete):
        """Build marts and return an audit success marker.

        All ELT staging and DQ tasks are upstream of this task. The marker is
        therefore sufficient for ``audit_run`` to determine overall success
        without using Airflow's restricted metadata ORM inside a task.
        """
        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        _execute_sql(con, "nammamart_elt_marts.sql", context["run_id"], context["dag"].dag_id, _as_of_date())
        for table_name in ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla", "mart_payment_reconciliation", "mart_top_customers"]:
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[Mart] {table_name}: {row_count} rows")
        con.close()
        return True

    @task
    def compare_etl_totals(marts_complete):
        """Persist an October-only delivered-revenue comparison with ETL."""
        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute(f"ATTACH '{ETL_WAREHOUSE_DB}' AS etl_db (READ_ONLY)")
        con.execute("""
            CREATE OR REPLACE TABLE elt_etl_september_reconciliation AS
            SELECT
                'September delivered revenue' AS metric,
                (SELECT COUNT(*) FROM etl_db.etl_fact_orders
                 WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) < DATE '2026-10-01') AS etl_order_count,
                (SELECT COUNT(*) FROM stg_orders
                 WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
                   AND CAST(order_date AS DATE) < DATE '2026-10-01') AS elt_order_count,
                (SELECT SUM(net_revenue) FROM etl_db.etl_fact_orders
                 WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) < DATE '2026-10-01') AS etl_value,
                (SELECT SUM(net_revenue) FROM stg_orders
                 WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
                   AND CAST(order_date AS DATE) < DATE '2026-10-01') AS elt_value,
                (SELECT SUM(net_revenue) FROM stg_orders
                 WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
                   AND CAST(order_date AS DATE) < DATE '2026-10-01')
                - (SELECT SUM(net_revenue) FROM etl_db.etl_fact_orders
                   WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) < DATE '2026-10-01') AS difference,
                CASE WHEN ABS(
                    (SELECT SUM(net_revenue) FROM stg_orders
                     WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
                       AND CAST(order_date AS DATE) < DATE '2026-10-01')
                    - (SELECT SUM(net_revenue) FROM etl_db.etl_fact_orders
                       WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) < DATE '2026-10-01')
                ) < 0.01 THEN 'MATCH' ELSE 'MISMATCH' END AS comparison_status,
                CURRENT_TIMESTAMP AS compared_at
        """)
        con.execute("""
            CREATE OR REPLACE TABLE elt_etl_october_reconciliation AS
            SELECT
                'October delivered revenue' AS metric,
                (SELECT COUNT(*) FROM etl_db.etl_fact_orders
                 WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                   AND CAST(order_date AS DATE) < DATE '2026-11-01') AS etl_order_count,
                (SELECT COUNT(*) FROM stg_orders
                 WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
                   AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                   AND CAST(order_date AS DATE) < DATE '2026-11-01') AS elt_order_count,
                (SELECT SUM(net_revenue) FROM etl_db.etl_fact_orders
                 WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                   AND CAST(order_date AS DATE) < DATE '2026-11-01') AS etl_value,
                (SELECT SUM(net_revenue) FROM stg_orders
                 WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
                   AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                   AND CAST(order_date AS DATE) < DATE '2026-11-01') AS elt_value,
                (SELECT SUM(net_revenue) FROM stg_orders
                 WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
                   AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                   AND CAST(order_date AS DATE) < DATE '2026-11-01')
                - (SELECT SUM(net_revenue) FROM etl_db.etl_fact_orders
                   WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                     AND CAST(order_date AS DATE) < DATE '2026-11-01') AS difference,
                CASE WHEN (SELECT COUNT(*) FROM etl_db.etl_fact_orders
                           WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                             AND CAST(order_date AS DATE) < DATE '2026-11-01') = 0
                     THEN 'ETL_INCREMENT_NOT_LOADED'
                     WHEN ABS(
                    (SELECT SUM(net_revenue) FROM stg_orders
                     WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
                       AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                       AND CAST(order_date AS DATE) < DATE '2026-11-01')
                    - (SELECT SUM(net_revenue) FROM etl_db.etl_fact_orders
                       WHERE order_status = 'DELIVERED' AND CAST(order_date AS DATE) >= DATE '2026-10-01'
                         AND CAST(order_date AS DATE) < DATE '2026-11-01')
                ) < 0.01 THEN 'MATCH' ELSE 'MISMATCH' END AS comparison_status,
                CURRENT_TIMESTAMP AS compared_at
        """)
        print(con.execute("SELECT * FROM elt_etl_september_reconciliation").fetchdf().to_string(index=False))
        print(con.execute("SELECT * FROM elt_etl_october_reconciliation").fetchdf().to_string(index=False))
        con.close()
        return marts_complete

    # -------------------- AUDIT: always record the run outcome ---------------
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run():
        """Record run timing and row counts even when an upstream task fails."""
        context = get_current_context()
        task_instance = context["ti"]
        dag_run = context["dag_run"]
        rows_extracted = sum(task_instance.xcom_pull(task_ids=task_id) or 0 for task_id in [
            "load_raw_orders", "load_raw_payments", "load_raw_customers", "load_raw_products", "load_raw_stores", "load_raw_increment"
        ])
        con = duckdb.connect(WAREHOUSE_DB)
        try:
            rows_loaded = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
            rows_quarantined = con.execute("""
                SELECT COALESCE((SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'FAIL'), 0)
                     + COALESCE((SELECT COUNT(*) FROM stg_payments WHERE dq_status = 'FAIL'), 0)
                     + COALESCE((SELECT COUNT(*) FROM stg_customers WHERE dq_status = 'FAIL'), 0)
                     + COALESCE((SELECT COUNT(*) FROM stg_products WHERE dq_status = 'FAIL'), 0)
                     + COALESCE((SELECT COUNT(*) FROM stg_stores WHERE dq_status = 'FAIL'), 0)
            """).fetchone()[0]
        except duckdb.CatalogException:
            rows_loaded = 0
            rows_quarantined = 0
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
        )""")
        # `build_marts` depends on the complete ELT transformation chain and
        # returns a marker only when it completes successfully. This avoids
        # direct metadata DB access, which Airflow 3 disallows from task code.
        upstream_failed = task_instance.xcom_pull(task_ids="build_marts") is not True
        status = "failed" if upstream_failed else "success"
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
            dag_run.run_id, context["dag"].dag_id, dag_run.start_date,
            pendulum.now("UTC"), rows_extracted, rows_loaded, rows_quarantined, status,
        ])
        con.close()
        print(f"[Audit] run_id={dag_run.run_id} status={status} extracted={rows_extracted} loaded={rows_loaded} rejected={rows_quarantined}")

        # Bonus: Slack failure alert via Airflow Variable.
        if status == "failed":
            send_slack_alert(
                f":red_circle: *NammaMart ELT run failed*\n"
                f"• DAG: `{context['dag'].dag_id}`\n"
                f"• Run: `{dag_run.run_id}`\n"
                f"• Extracted: {rows_extracted}, Loaded: {rows_loaded}, Quarantined: {rows_quarantined}\n"
                f"• Check Airflow UI for task logs."
            )

    raw_orders = load_raw_orders()
    raw_payments = load_raw_payments()
    raw_customers = load_raw_customers()
    raw_products = load_raw_products()
    raw_stores = load_raw_stores()
    raw_increment = load_raw_increment()

    # DuckDB permits one writer at a time. Serialize all raw-load tasks so
    # concurrent workers cannot contend for the warehouse file lock. The
    # increment remains immediately after the base orders so staging's latest-
    # record window function applies correction rows over the September
    # snapshot.
    raw_orders >> raw_increment >> raw_payments >> raw_customers >> raw_products >> raw_stores
    staging = build_staging(raw_orders, raw_increment, raw_payments, raw_customers, raw_products, raw_stores)
    upserted = apply_order_upsert(staging)
    dq = record_dq_results(upserted)
    marts = build_marts(dq)
    compared = compare_etl_totals(marts)
    audit_task = audit_run()
    compared >> audit_task


nammamart_elt_rudin()
