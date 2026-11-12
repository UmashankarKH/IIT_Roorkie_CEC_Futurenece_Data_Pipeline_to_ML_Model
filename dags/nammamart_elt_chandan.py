"""
dags/nammamart_elt_chandan.py
-------------------------------
NammaMart Quick Commerce — ELT Pipeline
Author : Chandan
Schedule: 9:00 AM IST, Monday–Saturday

Flow:
  extract_and_load_raw (5 parallel) ──►
  run_staging_sql (stg_orders, stg_payments, stg_customers, stg_products, stg_stores) ──►
  run_mart_sql   (mart_daily_store_kpis, mart_category_margin, mart_delivery_sla,
                  mart_payment_reconciliation, mart_top_customers) ──►
  log_dq_results ──►
  load_increment ──►
  audit_run (trigger_rule=all_done)

Design notes
─────────────
• ELT: ALL raw rows land in DuckDB exactly as received.  SQL-only transforms happen
  inside the warehouse; nothing is cleaned in Python before loading.
• Raw tables: append-only with _loaded_at, _source_file, _run_id metadata.
• Staging: idempotent CREATE OR REPLACE TABLE — no accumulation.
• Mart:    idempotent CREATE OR REPLACE TABLE.
• Incremental: raw APPEND + staging MERGE/upsert on order_id.
• IST = UTC+5:30  →  9:00 AM IST = 03:30 UTC  →  cron "30 3 * * 1-6"
• catchup=False: we want today's report only, not historical backfills.
"""

import sys
import os
sys.path.insert(0, "/usr/local/airflow/include")

from airflow.decorators import dag, task
import pendulum
from datetime import timedelta
import pandas as pd
import duckdb

# ─── paths ────────────────────────────────────────────────────────────────────
DATA_DIR   = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE  = "/usr/local/airflow/include/warehouse_chandan.duckdb"
SQL_DIR    = "/usr/local/airflow/include/sql/nammamart"

# ─── default_args ─────────────────────────────────────────────────────────────
default_args = {
    "owner":             "chandan",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def _run_sql_file(filename: str, con: duckdb.DuckDBPyConnection) -> None:
    """Read a .sql file and execute it against the open connection."""
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r") as f:
        sql = f.read()
    con.execute(sql)
    print(f"[SQL] Executed {filename}")


@dag(
    dag_id     = "nammamart_elt_chandan",
    schedule   = "30 3 * * 1-6",
    start_date = pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup    = False,
    default_args = default_args,
    tags       = ["nammamart", "elt", "chandan"],
    doc_md     = __doc__,
)
def nammamart_elt_chandan():

    # ══════════════════════════════════════════════════════════════════════════
    # EXTRACT & LOAD RAW — 5 parallel tasks
    # Each task reads one CSV and appends ALL rows to the raw table.
    # No cleaning, no filtering.
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def load_raw_orders(**context):
        run_id = context["run_id"]
        loaded_at = pendulum.now("UTC").isoformat()
        df = pd.read_csv(f"{DATA_DIR}/orders.csv", dtype=str)
        df["_loaded_at"]   = loaded_at
        df["_source_file"] = "orders.csv"
        df["_run_id"]      = run_id

        con = duckdb.connect(WAREHOUSE)
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id         VARCHAR,
                order_ts         VARCHAR,
                customer_id      VARCHAR,
                store_id         VARCHAR,
                product_id       VARCHAR,
                quantity         VARCHAR,
                unit_price       VARCHAR,
                discount_pct     VARCHAR,
                order_amount     VARCHAR,
                payment_mode     VARCHAR,
                order_status     VARCHAR,
                channel          VARCHAR,
                delivery_minutes VARCHAR,
                _loaded_at       VARCHAR,
                _source_file     VARCHAR,
                _run_id          VARCHAR
            )
        """)
        # Idempotent: delete rows from this run before inserting
        con.execute(f"DELETE FROM raw_orders WHERE _run_id = '{run_id}'")
        con.execute("INSERT INTO raw_orders SELECT * FROM df")
        n = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        con.close()
        print(f"[Raw] raw_orders total rows: {n}")
        return {"file": "orders.csv", "rows_loaded": len(df)}

    @task
    def load_raw_payments(**context):
        run_id = context["run_id"]
        loaded_at = pendulum.now("UTC").isoformat()
        df = pd.read_csv(f"{DATA_DIR}/payments.csv", dtype=str)
        df["_loaded_at"]   = loaded_at
        df["_source_file"] = "payments.csv"
        df["_run_id"]      = run_id

        con = duckdb.connect(WAREHOUSE)
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_payments (
                payment_id     VARCHAR,
                order_id       VARCHAR,
                payment_ts     VARCHAR,
                payment_mode   VARCHAR,
                amount_paid    VARCHAR,
                payment_status VARCHAR,
                gateway_ref    VARCHAR,
                _loaded_at     VARCHAR,
                _source_file   VARCHAR,
                _run_id        VARCHAR
            )
        """)
        con.execute(f"DELETE FROM raw_payments WHERE _run_id = '{run_id}'")
        con.execute("INSERT INTO raw_payments SELECT * FROM df")
        n = con.execute("SELECT COUNT(*) FROM raw_payments").fetchone()[0]
        con.close()
        print(f"[Raw] raw_payments total rows: {n}")
        return {"file": "payments.csv", "rows_loaded": len(df)}

    @task
    def load_raw_customers(**context):
        run_id = context["run_id"]
        loaded_at = pendulum.now("UTC").isoformat()
        df = pd.read_csv(f"{DATA_DIR}/customers.csv", dtype=str)
        df["_loaded_at"]   = loaded_at
        df["_source_file"] = "customers.csv"
        df["_run_id"]      = run_id

        con = duckdb.connect(WAREHOUSE)
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_customers (
                customer_id  VARCHAR,
                full_name    VARCHAR,
                email        VARCHAR,
                phone        VARCHAR,
                city         VARCHAR,
                area         VARCHAR,
                pincode      VARCHAR,
                signup_date  VARCHAR,
                loyalty_tier VARCHAR,
                is_active    VARCHAR,
                _loaded_at   VARCHAR,
                _source_file VARCHAR,
                _run_id      VARCHAR
            )
        """)
        con.execute(f"DELETE FROM raw_customers WHERE _run_id = '{run_id}'")
        con.execute("INSERT INTO raw_customers SELECT * FROM df")
        n = con.execute("SELECT COUNT(*) FROM raw_customers").fetchone()[0]
        con.close()
        print(f"[Raw] raw_customers total rows: {n}")
        return {"file": "customers.csv", "rows_loaded": len(df)}

    @task
    def load_raw_products(**context):
        run_id = context["run_id"]
        loaded_at = pendulum.now("UTC").isoformat()
        df = pd.read_csv(f"{DATA_DIR}/products.csv", dtype=str)
        df["_loaded_at"]   = loaded_at
        df["_source_file"] = "products.csv"
        df["_run_id"]      = run_id

        con = duckdb.connect(WAREHOUSE)
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_products (
                product_id    VARCHAR,
                product_name  VARCHAR,
                category      VARCHAR,
                sub_category  VARCHAR,
                brand         VARCHAR,
                mrp           VARCHAR,
                cost_price    VARCHAR,
                unit          VARCHAR,
                is_perishable VARCHAR,
                launch_date   VARCHAR,
                _loaded_at    VARCHAR,
                _source_file  VARCHAR,
                _run_id       VARCHAR
            )
        """)
        con.execute(f"DELETE FROM raw_products WHERE _run_id = '{run_id}'")
        con.execute("INSERT INTO raw_products SELECT * FROM df")
        n = con.execute("SELECT COUNT(*) FROM raw_products").fetchone()[0]
        con.close()
        print(f"[Raw] raw_products total rows: {n}")
        return {"file": "products.csv", "rows_loaded": len(df)}

    @task
    def load_raw_stores(**context):
        run_id = context["run_id"]
        loaded_at = pendulum.now("UTC").isoformat()
        df = pd.read_csv(f"{DATA_DIR}/stores.csv", dtype=str)
        df["_loaded_at"]   = loaded_at
        df["_source_file"] = "stores.csv"
        df["_run_id"]      = run_id

        con = duckdb.connect(WAREHOUSE)
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_stores (
                store_id                VARCHAR,
                store_name              VARCHAR,
                zone                    VARCHAR,
                area                    VARCHAR,
                pincode                 VARCHAR,
                open_date               VARCHAR,
                manager_name            VARCHAR,
                capacity_orders_per_day VARCHAR,
                _loaded_at              VARCHAR,
                _source_file            VARCHAR,
                _run_id                 VARCHAR
            )
        """)
        con.execute(f"DELETE FROM raw_stores WHERE _run_id = '{run_id}'")
        con.execute("INSERT INTO raw_stores SELECT * FROM df")
        n = con.execute("SELECT COUNT(*) FROM raw_stores").fetchone()[0]
        con.close()
        print(f"[Raw] raw_stores total rows: {n}")
        return {"file": "stores.csv", "rows_loaded": len(df)}

    # ══════════════════════════════════════════════════════════════════════════
    # STAGING — SQL-only transforms
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def run_staging(raw_results: list, **context):
        """Run all staging SQL files in sequence."""
        con = duckdb.connect(WAREHOUSE)
        for sql_file in [
            "stg_orders.sql",
            "stg_payments.sql",
            "stg_customers.sql",
            "stg_products.sql",
            "stg_stores.sql",
        ]:
            _run_sql_file(sql_file, con)

        # Print DQ summary
        dq_summary = con.execute("""
            SELECT dq_status, COUNT(*) AS cnt
            FROM stg_orders
            GROUP BY dq_status
        """).fetchdf()
        print(f"[Staging] stg_orders DQ summary:\n{dq_summary.to_string(index=False)}")
        con.close()
        return "staging_done"

    # ══════════════════════════════════════════════════════════════════════════
    # MART — business-ready tables
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def run_marts(staging_done: str, **context):
        """Run all mart SQL files."""
        con = duckdb.connect(WAREHOUSE)
        for sql_file in [
            "mart_daily_store_kpis.sql",
            "mart_category_margin.sql",
            "mart_delivery_sla.sql",
            "mart_payment_reconciliation.sql",
            "mart_top_customers.sql",
        ]:
            _run_sql_file(sql_file, con)

        # Print key metrics
        zone_rev = con.execute("""
            SELECT zone, SUM(net_revenue) AS zone_revenue
            FROM mart_daily_store_kpis
            GROUP BY zone
            ORDER BY zone_revenue DESC
        """).fetchdf()
        print(f"\n[Mart] Zone Revenue:\n{zone_rev.to_string(index=False)}")

        cat = con.execute("SELECT * FROM mart_category_margin").fetchdf()
        print(f"\n[Mart] Category Margin:\n{cat.to_string(index=False)}")
        con.close()
        return "marts_done"

    # ══════════════════════════════════════════════════════════════════════════
    # LOG DQ RESULTS to dq_results table (for scorecard)
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def log_dq_results(marts_done: str, **context):
        run_id     = context["run_id"]
        checked_at = pendulum.now("UTC").isoformat()

        con = duckdb.connect(WAREHOUSE)
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id        VARCHAR,
                dag_id        VARCHAR,
                source_file   VARCHAR,
                check_name    VARCHAR,
                dimension     VARCHAR,
                severity      VARCHAR,
                rows_checked  INTEGER,
                rows_failed   INTEGER,
                status        VARCHAR,
                checked_at    VARCHAR
            )
        """)

        dq_checks = [
            # (check_name, dimension, severity, source_file, sql_to_count_failures)
            ("stg_orders_dq_pass",     "Completeness",  "Critical", "orders.csv",
             "SELECT COUNT(*) FROM stg_orders WHERE dq_status='FAIL'",
             "SELECT COUNT(*) FROM stg_orders"),
            ("stg_payments_dq_pass",   "Completeness",  "Critical", "payments.csv",
             "SELECT COUNT(*) FROM stg_payments WHERE dq_status='FAIL'",
             "SELECT COUNT(*) FROM stg_payments"),
            ("stg_customers_dq_pass",  "Completeness",  "Critical", "customers.csv",
             "SELECT COUNT(*) FROM stg_customers WHERE dq_status='FAIL'",
             "SELECT COUNT(*) FROM stg_customers"),
            ("stg_products_dq_pass",   "Completeness",  "Critical", "products.csv",
             "SELECT COUNT(*) FROM stg_products WHERE dq_status='FAIL'",
             "SELECT COUNT(*) FROM stg_products"),
            ("stg_stores_dq_pass",     "Completeness",  "Warning",  "stores.csv",
             "SELECT COUNT(*) FROM stg_stores WHERE dq_status='FAIL'",
             "SELECT COUNT(*) FROM stg_stores"),
            ("payment_reconciliation", "Consistency",   "Critical", "payments.csv",
             "SELECT COUNT(*) FROM mart_payment_reconciliation",
             "SELECT COUNT(*) FROM stg_payments WHERE payment_status='SUCCESS'"),
        ]

        rows = []
        for (check_name, dimension, severity, src, fail_sql, total_sql) in dq_checks:
            rows_failed  = con.execute(fail_sql).fetchone()[0]
            rows_checked = con.execute(total_sql).fetchone()[0]
            rows.append({
                "run_id":       run_id,
                "dag_id":       "nammamart_elt_chandan",
                "source_file":  src,
                "check_name":   check_name,
                "dimension":    dimension,
                "severity":     severity,
                "rows_checked": rows_checked,
                "rows_failed":  rows_failed,
                "status":       "FAIL" if rows_failed > 0 else "PASS",
                "checked_at":   checked_at,
            })

        dq_df = pd.DataFrame(rows)
        con.execute("INSERT INTO dq_results SELECT * FROM dq_df")
        print(f"[DQ] Logged {len(rows)} DQ checks to dq_results")
        con.close()
        return "dq_logged"

    # ══════════════════════════════════════════════════════════════════════════
    # INCREMENTAL LOAD — orders_2026-10-01_increment.csv
    # Raw: APPEND all rows
    # Staging: MERGE/upsert on order_id  (DuckDB doesn't have MERGE natively,
    #          so we do DELETE + INSERT on stg_orders for the affected order_ids)
    # ══════════════════════════════════════════════════════════════════════════

    @task
    def load_increment(**context):
        run_id    = context["run_id"]
        loaded_at = pendulum.now("UTC").isoformat()
        inc_path  = f"{DATA_DIR}/orders_2026-10-01_increment.csv"

        if not os.path.exists(inc_path):
            print("[Increment] No increment file found, skipping.")
            return "skipped"

        inc_df = pd.read_csv(inc_path, dtype=str)
        inc_df["_loaded_at"]   = loaded_at
        inc_df["_source_file"] = "orders_2026-10-01_increment.csv"
        inc_df["_run_id"]      = run_id

        con = duckdb.connect(WAREHOUSE)

        # ── 1. Append to raw_orders ──────────────────────────────────────────
        # Don't delete existing raw rows — raw is immutable append-only audit log
        con.execute("INSERT INTO raw_orders SELECT * FROM inc_df")
        raw_count = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        print(f"[Increment] raw_orders after append: {raw_count} rows")

        # ── 2. Upsert into stg_orders ────────────────────────────────────────
        # DuckDB doesn't support MERGE ON, so we emulate it:
        #   DELETE rows where order_id is in the increment
        #   then re-run stg_orders SQL over the full raw_orders (latest row wins
        #   because raw_orders already has both old + new; the ROW_NUMBER
        #   in stg_orders picks the latest _loaded_at per order_id)

        # But stg_orders.sql does not deduplicate by order_id — orders are facts.
        # Instead: for corrections (status changes), we need the increment row
        # to override the original.  We achieve this by:
        # 1. Deleting from stg_orders where order_id in increment
        # 2. Re-staging those rows from the freshest raw_orders rows

        inc_ids     = tuple(inc_df["order_id"].tolist())
        id_list     = ",".join(f"'{x}'" for x in inc_ids)

        con.execute(f"DELETE FROM stg_orders WHERE order_id IN ({id_list})")

        # Re-stage only the increment rows from raw
        # We use the full stg_orders SQL logic in a CTE scoped to inc_ids
        inc_stg_sql = f"""
            INSERT INTO stg_orders
            WITH base AS (
                SELECT
                    order_id,
                    CAST(order_ts AS TIMESTAMP)                              AS order_ts,
                    CAST(order_ts AS DATE)                                   AS order_date,
                    customer_id,
                    store_id,
                    product_id,
                    CAST(quantity        AS DOUBLE)                          AS quantity,
                    CAST(unit_price      AS DOUBLE)                          AS unit_price,
                    COALESCE(CAST(discount_pct AS DOUBLE), 0)                AS discount_pct,
                    CAST(order_amount    AS DOUBLE)                          AS order_amount,
                    CASE
                        WHEN LOWER(TRIM(payment_mode)) = 'upi'               THEN 'UPI'
                        WHEN LOWER(TRIM(payment_mode)) = 'card'              THEN 'Card'
                        WHEN LOWER(TRIM(payment_mode)) = 'cash on delivery'  THEN 'Cash on Delivery'
                        WHEN LOWER(TRIM(payment_mode)) = 'wallet'            THEN 'Wallet'
                        ELSE TRIM(payment_mode)
                    END                                                      AS payment_mode_raw,
                    CASE
                        WHEN LOWER(TRIM(payment_mode)) = 'upi'               THEN 'UPI'
                        WHEN LOWER(TRIM(payment_mode)) = 'card'              THEN 'Card'
                        WHEN LOWER(TRIM(payment_mode)) = 'cash on delivery'  THEN 'Cash on Delivery'
                        WHEN LOWER(TRIM(payment_mode)) = 'wallet'            THEN 'Wallet'
                        ELSE TRIM(payment_mode)
                    END                                                      AS payment_mode,
                    UPPER(TRIM(order_status))                                AS order_status,
                    TRIM(channel)                                            AS channel,
                    CAST(delivery_minutes AS DOUBLE)                         AS delivery_minutes,
                    _loaded_at,
                    _source_file,
                    _run_id
                FROM raw_orders
                WHERE order_id IN ({id_list})
                  AND _source_file = 'orders_2026-10-01_increment.csv'
            ),
            with_derived AS (
                SELECT *,
                    ROUND(quantity * unit_price * (1 - discount_pct / 100.0), 2) AS net_revenue_calc,
                    ROUND(quantity * unit_price * (1 - discount_pct / 100.0), 2) AS net_revenue,
                    CASE WHEN COALESCE(delivery_minutes, 999) <= 15 THEN 1 ELSE 0 END AS within_15_min,
                    CASE
                        WHEN customer_id IS NULL                                       THEN 'customer_id is null'
                        WHEN order_status NOT IN ('DELIVERED','CANCELLED','RETURNED')  THEN 'invalid order_status'
                        WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'invalid payment_mode'
                        WHEN quantity IS NULL OR quantity <= 0                         THEN 'quantity must be > 0'
                        WHEN order_ts > CURRENT_TIMESTAMP                              THEN 'order_ts is in the future'
                        WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL   THEN 'DELIVERED missing delivery_minutes'
                        WHEN ABS(order_amount - ROUND(quantity * unit_price * (1 - discount_pct/100.0), 2)) > 0.02
                                                                                       THEN 'order_amount formula mismatch'
                        ELSE NULL
                    END AS failure_reason
                FROM base
            )
            SELECT *,
                CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status
            FROM with_derived
        """
        con.execute(inc_stg_sql)

        stg_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
        new_count = con.execute(
            f"SELECT COUNT(*) FROM stg_orders WHERE _run_id = '{run_id}'"
        ).fetchone()[0]
        print(f"[Increment] stg_orders total: {stg_count}, upserted: {len(inc_df)}, new run rows: {new_count}")
        con.close()
        return "increment_done"

    # ══════════════════════════════════════════════════════════════════════════
    # AUDIT RUN
    # ══════════════════════════════════════════════════════════════════════════

    @task(trigger_rule="all_done")
    def audit_run(raw_results: list, **context):
        run_id     = context["run_id"]
        dag_id     = context["dag"].dag_id
        start_time = context["data_interval_start"].isoformat()
        end_time   = pendulum.now("UTC").isoformat()

        total_extracted = sum(r.get("rows_loaded", 0) for r in raw_results if r)

        con = duckdb.connect(WAREHOUSE)
        con.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id           VARCHAR,
                dag_id           VARCHAR,
                start_time       VARCHAR,
                end_time         VARCHAR,
                rows_extracted   INTEGER,
                rows_loaded      INTEGER,
                rows_quarantined INTEGER,
                status           VARCHAR
            )
        """)

        try:
            rows_loaded = con.execute(
                f"SELECT COUNT(*) FROM raw_orders WHERE _run_id = '{run_id}'"
            ).fetchone()[0]
        except Exception:
            rows_loaded = 0

        try:
            rows_failed_stg = con.execute(
                "SELECT COUNT(*) FROM stg_orders WHERE dq_status='FAIL'"
            ).fetchone()[0]
        except Exception:
            rows_failed_stg = 0

        audit_df = pd.DataFrame([{
            "run_id":           run_id,
            "dag_id":           dag_id,
            "start_time":       start_time,
            "end_time":         end_time,
            "rows_extracted":   total_extracted,
            "rows_loaded":      rows_loaded,
            "rows_quarantined": rows_failed_stg,
            "status":           "SUCCESS",
        }])
        con.execute("INSERT INTO pipeline_runs SELECT * FROM audit_df")
        print(f"[Audit] Run logged: {run_id} | loaded={rows_loaded} | stg_failed={rows_failed_stg}")
        con.close()

    # ══════════════════════════════════════════════════════════════════════════
    # DAG WIRING
    # ══════════════════════════════════════════════════════════════════════════

    r_orders    = load_raw_orders()
    r_payments  = load_raw_payments()
    r_customers = load_raw_customers()
    r_products  = load_raw_products()
    r_stores    = load_raw_stores()

    raw_results = [r_orders, r_payments, r_customers, r_products, r_stores]

    staged     = run_staging(raw_results)
    marts_done = run_marts(staged)
    dq_logged  = log_dq_results(marts_done)
    increment  = load_increment()

    dq_logged  >> increment
    audit_run(raw_results)


nammamart_elt_chandan()
