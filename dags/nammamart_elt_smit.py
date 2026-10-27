"""
dags/nammamart_elt_smit.py
--------------------------
NammaMart ELT pipeline: Extract -> Load raw -> Transform in the warehouse with SQL

Analytics and compliance want every record kept exactly as received. So every
row (good or bad) is loaded into raw_* tables first, as text, and all cleaning
happens afterwards in DuckDB using the .sql files in include/sql/.

Layers in include/warehouse_smit.duckdb:
  raw_*      exact copy of each CSV + _row_num, _loaded_at, _source_file, _run_id
  stg_*      typed, standardised, PII masked, dq_status + failure_reason per row
  mart_*     business tables that answer the six questions

Flow:
  create_raw_tables
     └─► load_raw_stores / load_raw_products / load_raw_customers /
         load_raw_orders / load_raw_orders_increment / load_raw_payments   (parallel)
            └─► stg_stores, stg_products, stg_customers                      (parallel)
                  ├─► dim_customer_history (bonus: SCD Type 2, loyalty-tier history)
                  └─► stg_orders ──► merge_orders_increment (MERGE on order_id)
                           └─► stg_payments ──► dq_checks
                                 └─► mart_* (parallel) ──► report ──► dq_trend_chart ──► run_audit (all_done)

Increment files: a scheduled run loads every orders_<date>_increment.csv dated
before today that isn't in raw_orders yet.
For a manual run, trigger with config {"increment_file": "orders_2026-10-01_increment.csv"}.
"""

import os
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk import Param, dag, task

from include.nammamart_smit.config import (
    BASE_BUSINESS_DATE, DATA_DIR, as_of_for, business_date_for,
)
from include.nammamart_smit.files import check_schema, pick_increment_files
from include.nammamart_smit import quality
from include.nammamart_smit.warehouse import (
    get_connection, last_row_count, run_sql_file, save_dq_results, save_pipeline_run,
)

DAG_ID = "nammamart_elt_smit"

default_args = {
    "owner": "smit",
    "retries": 2,                                # a file landing a few minutes late shouldn't fail the run
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),  # a hung task must not block the 9:30 stand-up
}


def load_csv_to_raw(file_name, source, run_id):
    """Append one CSV to raw_<source> exactly as received. No pandas, no cleaning."""
    path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"[Load raw] Source file not found: {path}")

    # Only the header is read here, to fail loudly if a column was renamed
    check_schema(list(pd.read_csv(path, nrows=0).columns), source, file_name)

    con = get_connection()
    run_sql_file(con, "raw_create_tables.sql", run_id)

    # A retry or a cleared task must not load the same file twice for the same run
    already = con.execute(
        f"SELECT count(*) FROM raw_{source} WHERE _run_id = ? AND _source_file = ?", [run_id, file_name]
    ).fetchone()[0]
    if already:
        con.close()
        print(f"[Load raw] {file_name} already loaded for this run ({already} rows), skipping")
        return already

    # nullstr = '\N' keeps empty cells as '' instead of NULL, so raw is a true copy
    con.execute(f"""
        INSERT INTO raw_{source} BY NAME
        SELECT *,
               row_number() OVER () AS _row_num,
               now()::TIMESTAMP     AS _loaded_at,
               ?                    AS _source_file,
               ?                    AS _run_id
        FROM read_csv('{path}', all_varchar = true, header = true, nullstr = '\\N')
    """, [file_name, run_id])
    rows = con.execute(
        f"SELECT count(*) FROM raw_{source} WHERE _run_id = ? AND _source_file = ?", [run_id, file_name]
    ).fetchone()[0]
    total = con.execute(f"SELECT count(*) FROM raw_{source}").fetchone()[0]
    con.close()

    print(f"[Load raw] {file_name}: {rows} rows appended to raw_{source} (raw_{source} now holds {total} rows)")
    return rows


def build_table(sql_file, run_id, table):
    con = get_connection()
    run_sql_file(con, sql_file, run_id)
    counts = con.execute(f"SELECT dq_status, count(*) FROM {table} GROUP BY 1 ORDER BY 1").fetchall() \
        if table.startswith("stg") else con.execute(f"SELECT 'rows', count(*) FROM {table}").fetchall()
    con.close()
    print(f"[Transform] {table}: {dict(counts)}")
    return dict(counts)


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                      # 9:00 AM, Monday to Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),  # cron is read in IST, not UTC
    catchup=False,                                               # don't backfill missed days, see SUBMISSION.md
    max_active_runs=1,                                           # two runs must never write the warehouse together
    default_args=default_args,
    params={"increment_file": Param(None, type=["null", "string"],
                                    description="Optional: orders_<date>_increment.csv to load in this run")},
    tags=["nammamart", "elt", "smit"],
)
def nammamart_elt_smit():

    # ── EXTRACT + LOAD RAW (run in parallel) ──────────────────────────────────

    @task
    def create_raw_tables(run_id=None):
        con = get_connection()
        run_sql_file(con, "raw_create_tables.sql", run_id)
        con.close()
        return "ok"

    @task
    def load_raw_orders(run_id=None):
        return load_csv_to_raw("orders.csv", "orders", run_id)

    @task
    def load_raw_orders_increment(params=None, dag_run=None, run_id=None):
        con = get_connection()
        run_sql_file(con, "raw_create_tables.sql", run_id)
        already_loaded = {row[0] for row in con.execute(
            "SELECT DISTINCT _source_file FROM raw_orders WHERE _source_file <> 'orders.csv'").fetchall()}
        con.close()

        files = pick_increment_files(params.get("increment_file"), dag_run.run_after, already_loaded)
        return sum(load_csv_to_raw(file_name, "orders", run_id) for file_name in files)

    @task
    def load_raw_payments(run_id=None):
        return load_csv_to_raw("payments.csv", "payments", run_id)

    @task
    def load_raw_customers(run_id=None):
        return load_csv_to_raw("customers.csv", "customers", run_id)

    @task
    def load_raw_products(run_id=None):
        return load_csv_to_raw("products.csv", "products", run_id)

    @task
    def load_raw_stores(run_id=None):
        return load_csv_to_raw("stores.csv", "stores", run_id)

    # ── STAGING (SQL only) ────────────────────────────────────────────────────

    @task
    def stg_stores(run_id=None):
        return build_table("stg_stores.sql", run_id, "stg_stores")

    @task
    def stg_products(run_id=None):
        return build_table("stg_products.sql", run_id, "stg_products")

    @task
    def stg_customers(run_id=None):
        return build_table("stg_customers.sql", run_id, "stg_customers")

    @task
    def dim_customer_history(run_id=None):
        """Bonus: SCD Type 2 customer dimension, keeps loyalty-tier history."""
        con = get_connection()
        before = con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'dim_customer_scd2'").fetchone()[0]
        old_versions = con.execute("SELECT count(*) FROM dim_customer_scd2").fetchone()[0] if before else 0
        run_sql_file(con, "dim_customer_scd2.sql", run_id)
        versions, current = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE is_current) FROM dim_customer_scd2").fetchone()
        changed = con.execute("""
            SELECT customer_id, list(loyalty_tier ORDER BY valid_from) AS tier_history
            FROM dim_customer_scd2 GROUP BY 1 HAVING count(*) > 1 ORDER BY 1
        """).fetchall()
        con.close()
        print(f"[SCD2] {versions - old_versions} new versions this run | {versions} versions, {current} current customers | "
              f"customers with history: {changed or 'none'}")
        return {"new_versions": versions - old_versions, "versions": versions}

    @task
    def stg_orders(run_id=None):
        return build_table("stg_orders.sql", run_id, "stg_orders")

    @task
    def merge_orders_increment(run_id=None):
        con = get_connection()

        # Count what the MERGE is about to do, so the log shows inserts vs updates
        inserted, updated = con.execute("""
            WITH s AS (
                SELECT * FROM stg_orders_validated
                WHERE _source_file <> 'orders.csv' AND dup_rank = 1
                QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _batch_date DESC) = 1
            )
            SELECT
                count(*) FILTER (WHERE t.order_id IS NULL),
                count(*) FILTER (WHERE t.order_id IS NOT NULL AND s.dq_status = 'PASSED'
                                   AND s._batch_date >= t._batch_date
                                   AND (s.order_status <> t.order_status OR s.order_amount <> t.order_amount))
            FROM s
            LEFT JOIN stg_orders t ON t.order_id = s.order_id AND t.dup_rank = 1
        """).fetchone()

        run_sql_file(con, "stg_orders_merge_increment.sql", run_id)
        total = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
        dupes = con.execute("""
            SELECT count(*) FROM (SELECT order_id FROM stg_orders WHERE dq_status = 'PASSED'
                                  GROUP BY 1 HAVING count(*) > 1)
        """).fetchone()[0]
        con.close()

        print(f"[Merge] inserted {inserted} new orders | updated {updated} existing orders | "
              f"stg_orders now {total} rows | duplicate PASSED order_ids: {dupes}")
        return {"inserted": inserted, "updated": updated, "stg_orders_rows": total}

    @task
    def stg_payments(run_id=None):
        return build_table("stg_payments.sql", run_id, "stg_payments")

    # ── DATA QUALITY SCORECARD ────────────────────────────────────────────────

    @task
    def dq_checks(run_id=None):
        con = get_connection()
        results = run_sql_file(con, "dq_results_elt.sql", run_id).fetchdf().to_dict("records")

        # File-level checks: volume (every file loaded this run) and freshness
        loaded = con.execute("""
            SELECT _source_file, count(*) FROM raw_orders WHERE _run_id = ? GROUP BY 1
            UNION ALL SELECT _source_file, count(*) FROM raw_payments WHERE _run_id = ? GROUP BY 1
            UNION ALL SELECT _source_file, count(*) FROM raw_customers WHERE _run_id = ? GROUP BY 1
            UNION ALL SELECT _source_file, count(*) FROM raw_products WHERE _run_id = ? GROUP BY 1
            UNION ALL SELECT _source_file, count(*) FROM raw_stores WHERE _run_id = ? GROUP BY 1
        """, [run_id] * 5).fetchall()
        for file_name, rows in loaded:
            previous = last_row_count(con, DAG_ID, file_name, run_id)
            results.append(quality.volume_check(file_name, rows, previous))

        for file_name, latest in con.execute("""
            SELECT _source_file, max(order_ts) FILTER (WHERE order_ts <= _batch_date + INTERVAL 1 DAY + INTERVAL 9 HOUR)
            FROM stg_orders_validated GROUP BY 1
        """).fetchall():
            results.append(quality.freshness_check(file_name, latest, as_of_for(business_date_for(file_name))))

        as_of = as_of_for(BASE_BUSINESS_DATE)
        latest_payment = con.execute("SELECT max(payment_ts) FROM stg_payments WHERE payment_ts <= ?", [as_of]).fetchone()[0]
        results.append(quality.freshness_check("payments.csv", latest_payment, as_of))

        # Bonus: daily clean orders per store vs the store's capacity
        capacity = run_sql_file(con, "dq_store_capacity.sql", run_id).fetchdf().to_dict("records")
        print(f"[Capacity] {capacity[0]['rows_failed']} of {capacity[0]['rows_checked']} store-days over capacity")
        results += capacity

        save_dq_results(con, run_id, DAG_ID, results)

        # Business question 6: rows rejected today, by file and reason
        rejected = con.execute("""
            SELECT _source_file AS file, failure_reason, count(*) AS rows FROM (
                SELECT _source_file, failure_reason, dq_status FROM stg_stores
                UNION ALL SELECT _source_file, failure_reason, dq_status FROM stg_products
                UNION ALL SELECT _source_file, failure_reason, dq_status FROM stg_customers
                UNION ALL SELECT _source_file, failure_reason, dq_status FROM stg_orders_validated
                UNION ALL SELECT _source_file, failure_reason, dq_status FROM stg_payments
            ) WHERE dq_status = 'FAILED'
            GROUP BY ALL ORDER BY 1, 3 DESC
        """).fetchdf()
        con.close()

        failed = int(rejected["rows"].sum()) if len(rejected) else 0
        print(f"[DQ] {len(results)} check results saved | {failed} rows rejected\n{rejected.to_string(index=False)}")
        return {"rows_quarantined": failed}

    # ── MARTS (SQL only) ──────────────────────────────────────────────────────

    @task
    def mart_daily_store_kpis(run_id=None):
        return build_table("mart_daily_store_kpis.sql", run_id, "mart_daily_store_kpis")

    @task
    def mart_category_margin(run_id=None):
        return build_table("mart_category_margin.sql", run_id, "mart_category_margin")

    @task
    def mart_delivery_sla(run_id=None):
        return build_table("mart_delivery_sla.sql", run_id, "mart_delivery_sla")

    @task
    def mart_payment_reconciliation(run_id=None):
        return build_table("mart_payment_reconciliation.sql", run_id, "mart_payment_reconciliation")

    @task
    def mart_payment_mode_revenue(run_id=None):
        return build_table("mart_payment_mode_revenue.sql", run_id, "mart_payment_mode_revenue")

    @task
    def mart_top_customers(run_id=None):
        return build_table("mart_top_customers.sql", run_id, "mart_top_customers")

    @task
    def mart_store_capacity(run_id=None):
        return build_table("mart_store_capacity.sql", run_id, "mart_store_capacity")

    # ── REPORT: print the marts and compare September with the ETL ────────────

    @task
    def report():
        con = get_connection()
        zone = con.execute("""
            SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
            FROM mart_daily_store_kpis GROUP BY 1 ORDER BY 3 DESC
        """).fetchdf()
        category = con.execute("SELECT * FROM mart_category_margin").fetchdf()
        recon = con.execute("""
            SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount
            FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC
        """).fetchdf()

        elt_sept = con.execute("""
            SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis
            WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
        """).fetchone()[0]
        try:
            etl_sept = con.execute("""
                SELECT round(sum(net_revenue), 2) FROM etl_fact_orders
                WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
            """).fetchone()[0]
        except Exception:
            etl_sept = None
        con.close()

        print(f"[Report] Net revenue by zone:\n{zone.to_string(index=False)}")
        print(f"[Report] Category margin:\n{category.to_string(index=False)}")
        print(f"[Report] Payment reconciliation:\n{recon.to_string(index=False)}")
        print(f"[Report] September net revenue  ELT: {elt_sept}  |  ETL: {etl_sept}  |  "
              f"{'MATCH' if etl_sept == elt_sept else 'DIFFERENT (run both DAGs on the same files to compare)'}")
        return {"september_net_revenue": elt_sept}

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

        steps = ["create_raw_tables", "load_raw_orders", "load_raw_orders_increment", "load_raw_payments",
                 "load_raw_customers", "load_raw_products", "load_raw_stores", "stg_stores", "stg_products",
                 "stg_customers", "dim_customer_history", "stg_orders", "merge_orders_increment", "stg_payments", "dq_checks",
                 "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
                 "mart_payment_reconciliation", "mart_payment_mode_revenue", "mart_top_customers",
                 "mart_store_capacity", "report"]
        outputs = {step: ti.xcom_pull(task_ids=step) for step in steps}
        not_finished = [step for step, value in outputs.items() if value is None]

        rows_extracted = sum(outputs[s] or 0 for s in steps if s.startswith("load_raw"))
        rows_quarantined = (outputs["dq_checks"] or {}).get("rows_quarantined", 0)
        status = "success" if not not_finished else "failed"

        con = get_connection()
        try:
            rows_loaded = con.execute("SELECT count(*) FROM stg_orders WHERE dq_status = 'PASSED'").fetchone()[0]
        except Exception:
            rows_loaded = 0
        save_pipeline_run(con, {
            "run_id": run_id, "dag_id": DAG_ID,
            "start_time": dag_run.start_date, "end_time": pendulum.now("UTC"),
            "rows_extracted": rows_extracted, "rows_loaded": rows_loaded,
            "rows_quarantined": rows_quarantined, "status": status,
            "failed_tasks": ", ".join(not_finished) or None,
        })
        con.close()
        print(f"[Audit] {status} | raw rows loaded {rows_extracted} | clean orders {rows_loaded} | "
              f"rejected {rows_quarantined} | not finished: {not_finished}")

        if status == "failed":
            message = (f":red_circle: {DAG_ID} run {run_id} FAILED.\n"
                       f"Tasks that did not finish: {', '.join(not_finished)}.\n"
                       f"Check the Airflow task logs before the 9:30 stand-up.")
            send_slack_alert(message, webhook_url=Variable.get("SLACK_WEBHOOK_URL", default=None))
            raise RuntimeError(f"[Audit] Run failed, tasks not finished: {not_finished}")

    # ── DAG WIRING ────────────────────────────────────────────────────────────

    raw_ready = create_raw_tables()

    raw_orders = load_raw_orders()
    raw_increment = load_raw_orders_increment()
    raw_payments = load_raw_payments()
    raw_customers = load_raw_customers()
    raw_products = load_raw_products()
    raw_stores = load_raw_stores()
    raw_ready >> [raw_orders, raw_increment, raw_payments, raw_customers, raw_products, raw_stores]

    s_stores = stg_stores()
    s_products = stg_products()
    s_customers = stg_customers()
    raw_stores >> s_stores
    raw_products >> s_products
    raw_customers >> s_customers
    history = dim_customer_history()
    s_customers >> history

    s_orders = stg_orders()
    [raw_orders, raw_increment, s_stores, s_products, s_customers] >> s_orders

    merged = merge_orders_increment()
    s_payments = stg_payments()
    s_orders >> merged
    [s_orders, raw_payments] >> s_payments

    checks = dq_checks()
    [merged, s_payments] >> checks

    recon = mart_payment_reconciliation()
    marts = [mart_daily_store_kpis(), mart_category_margin(), mart_delivery_sla(), mart_top_customers(),
             mart_store_capacity(), recon]
    checks >> marts
    by_mode = mart_payment_mode_revenue()
    recon >> by_mode

    reported = report()
    marts >> reported
    by_mode >> reported
    history >> reported
    reported >> dq_trend_chart() >> run_audit()


nammamart_elt_smit()
