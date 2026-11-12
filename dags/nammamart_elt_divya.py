"""
dags/nammamart_elt_divya.py
---------------------------
NammaMart ELT pipeline: Extract -> Load raw -> Transform in DuckDB with SQL.

  extract_* ─► load_raw_* ─► stg_stores / stg_products / stg_customers ─► stg_orders
                                                                              │
                         load_raw_increment ─► stg_orders_merge_increment ◄───┘
                                                         │
                                  stg_payments ─► mart_* (parallel) ─► compare_with_etl ─► run_audit

Layers in include/warehouse_divya.duckdb:
  raw_*   exact copy of each CSV, all text, + _loaded_at/_source_file/_run_id (append-only)
  stg_*   SQL only: typed, standardised, deduped, PII masked, dq_status + failure_reason
  mart_*  SQL only: business tables answering the six business questions
Every transformation lives in include/sql/. Python only orchestrates.
Schedule: 09:00 IST, Monday-Saturday.
"""

import csv
import sys

sys.path.insert(0, "/usr/local/airflow/include")

from airflow.sdk import dag, task, get_current_context

from nammamart_divya.config import (
    DATA_DIR, EXPECTED_COLUMNS, INCREMENT_DIR, SOURCE_FILES, SQL_DIR, STALE_AFTER_DAYS,
    VOLUME_DROP_THRESHOLD, checks_for,
)
from nammamart_divya.runtime import (
    DEFAULT_ARGS, SCHEDULE_CRON, START_DATE, business_cutoff, notify_failure, run_audit,
)
from nammamart_divya.warehouse import (
    connect, previous_order_volume, write_dq_results, write_pipeline_run,
)

DAG_ID = "nammamart_elt_divya"


def _literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_sql(con, relative_path, **params):
    """Render {{placeholders}} as SQL string literals and run the file."""
    sql = (SQL_DIR / relative_path).read_text(encoding="utf-8")
    for key, value in params.items():
        sql = sql.replace("{{" + key + "}}", _literal(value))
    if "{{" in sql:
        raise ValueError(f"Unrendered placeholder in {relative_path}")
    print(f"[sql] running {relative_path}")
    con.execute(sql)


def _run_id():
    return get_current_context()["ti"].run_id


def record_dq(con, table, file_key, run_id, where="TRUE", source_file=None, extra=None):
    """Count failures of every f_<check> column of a staging table into dq_results."""
    checks = [c for c in checks_for(file_key)
              if con.execute(f"SELECT count(*) FROM information_schema.columns "
                             f"WHERE table_name = '{table}' AND column_name = 'f_{c}'").fetchone()[0]]
    select = ", ".join(f"count_if(f_{c})" for c in checks)
    row = con.execute(f"SELECT count(*), {select} FROM {table} WHERE {where}").fetchone()
    counts = {c: (row[0], row[i + 1]) for i, c in enumerate(checks)}
    counts.update(extra or {})
    write_dq_results(con, run_id, DAG_ID, counts, source_file)
    status = con.execute(f"SELECT dq_status, count(*) FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1").fetchall()
    print(f"[dq] {table}: {dict(status)}")
    for name, (checked, failed) in counts.items():
        if failed:
            print(f"    {name}: {failed} of {checked} rows")
    return dict(status)


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: land raw CSVs in DuckDB, transform with SQL into staging and marts",
    schedule=SCHEDULE_CRON,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": notify_failure},
    tags=["nammamart", "elt", "divya"],
)
def nammamart_elt_divya():

    # ── EXTRACT: confirm each file exists and matches its column contract ────
    def make_extract(file_key):
        @task(task_id=f"extract_{file_key}")
        def _extract():
            path = DATA_DIR / SOURCE_FILES[file_key]
            if not path.exists():
                raise FileNotFoundError(f"Source file missing: {path}")
            with open(path, newline="", encoding="utf-8") as f:
                header = next(csv.reader(f))
            if header != EXPECTED_COLUMNS[file_key]:
                raise ValueError(f"Schema change in {path.name}: got {header}, "
                                 f"expected {EXPECTED_COLUMNS[file_key]}")
            print(f"[extract] {path.name} found, header OK")
            return str(path)
        return _extract()

    @task
    def extract_increment():
        paths = [str(p) for p in sorted(INCREMENT_DIR.glob("orders_*_increment.csv"))]
        print(f"[extract] increment files: {paths or 'none'}")
        return paths

    # ── LOAD RAW: every row exactly as received, no cleaning ─────────────────
    def make_load_raw(file_key, path_arg):
        @task(task_id=f"load_raw_{file_key}")
        def _load(path):
            run_id = _run_id()
            source_file = SOURCE_FILES[file_key]
            with connect() as con:
                run_sql(con, "raw/load_raw.sql", table=f"raw_{file_key}", path=path,
                        source_file=source_file, run_id=run_id)
                n = con.execute(f"SELECT count(*) FROM raw_{file_key} WHERE _run_id = ? AND _source_file = ?",
                                [run_id, source_file]).fetchone()[0]
                total = con.execute(f"SELECT count(*) FROM raw_{file_key}").fetchone()[0]
            print(f"[raw] raw_{file_key}: +{n} rows this run, {total} rows in total (append-only)")
            return n
        return _load(path_arg)

    @task
    def load_raw_increment(paths):
        run_id, loaded = _run_id(), 0
        with connect() as con:
            for path in paths:
                name = path.replace("\\", "/").split("/")[-1]
                run_sql(con, "raw/load_raw.sql", table="raw_orders", path=path, source_file=name, run_id=run_id)
                n = con.execute("SELECT count(*) FROM raw_orders WHERE _run_id = ? AND _source_file = ?",
                                [run_id, name]).fetchone()[0]
                print(f"[raw] raw_orders: +{n} rows from {name} (appended)")
                loaded += n
        return loaded

    # ── STAGING (SQL) ────────────────────────────────────────────────────────
    def make_staging(table, file_key):
        @task(task_id=table)
        def _stage(_raw_rows):
            run_id = _run_id()
            with connect() as con:
                run_sql(con, "staging/00_macros.sql")
                run_sql(con, f"staging/{table}.sql", run_id=run_id, cutoff=str(business_cutoff()))
                return record_dq(con, table, file_key, run_id)
        return _stage

    @task
    def stg_orders(_raw_rows, _stores, _products, _customers):
        run_id = _run_id()
        with connect() as con:
            run_sql(con, "staging/00_macros.sql")
            run_sql(con, "staging/01_validate_orders_macro.sql")
            run_sql(con, "staging/stg_orders.sql", run_id=run_id, cutoff=str(business_cutoff()))
            # Dataset-level checks: volume vs previous run and freshness of the newest order.
            n = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            previous = previous_order_volume(con, DAG_ID, run_id)
            dropped = previous is not None and n < VOLUME_DROP_THRESHOLD * previous
            stale = con.execute(f"""
                SELECT coalesce(max(order_ts) < now()::TIMESTAMP - INTERVAL {STALE_AFTER_DAYS} DAY, true)
                FROM stg_orders WHERE NOT f_ord_ts_not_future""").fetchone()[0]
            print(f"[dq] volume {n} rows (previous run {previous}); stale={stale}")
            if dropped:
                print(f"[WARNING] order volume dropped by more than 90% ({previous} -> {n})")
            if stale:
                print("[WARNING] data is stale: latest order_ts is older than 1 day")
            extra = {"ord_volume_drop": (1, int(dropped)), "ord_freshness": (1, int(stale))}
            return record_dq(con, "stg_orders", "orders", run_id, extra=extra)

    @task
    def stg_orders_merge_increment(_inc_rows, _orders):
        run_id = _run_id()
        with connect() as con:
            run_sql(con, "staging/00_macros.sql")
            run_sql(con, "staging/01_validate_orders_macro.sql")
            before = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            con.execute("CREATE OR REPLACE TEMP TABLE _before AS SELECT order_id, order_status FROM stg_orders WHERE dq_status = 'PASSED'")
            run_sql(con, "staging/stg_orders_merge_increment.sql", run_id=run_id, cutoff=str(business_cutoff()))
            after = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            updated = con.execute("""SELECT count(*) FROM stg_orders_increment i JOIN _before b USING (order_id)
                                     WHERE i.dq_status = 'PASSED'""").fetchone()[0]
            changes = con.execute("""SELECT i.order_id, b.order_status AS old_status, i.order_status AS new_status
                                     FROM stg_orders_increment i JOIN _before b USING (order_id)
                                     WHERE i.dq_status = 'PASSED' AND b.order_status <> i.order_status
                                     ORDER BY 1""").fetchall()
            inc_total = con.execute("SELECT count(*) FROM stg_orders_increment").fetchone()[0]
            print(f"[merge] {inc_total} increment rows: {after - before} inserted, {updated} updated; "
                  f"stg_orders {before} -> {after} rows")
            for c in changes:
                print(f"    {c[0]}: {c[1]} -> {c[2]}")
            if inc_total:
                files = ",".join(r[0] for r in con.execute(
                    "SELECT DISTINCT _source_file FROM stg_orders_increment").fetchall())
                record_dq(con, "stg_orders_increment", "orders", run_id, source_file=files)
            return {"inserted": after - before, "updated": updated}

    # ── MARTS (SQL) ──────────────────────────────────────────────────────────
    MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
             "mart_payment_reconciliation", "mart_top_customers", "mart_rejections"]

    @task
    def mart_view(_merge, _payments, _customers):
        with connect() as con:
            run_sql(con, "mart/00_mart_orders_view.sql")

    def make_mart(name):
        @task(task_id=name)
        def _mart(_view):
            with connect() as con:
                run_sql(con, f"mart/{name}.sql", run_id=_run_id())
                n = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
                print(f"[mart] {name}: {n} rows")
                print(con.execute(f"SELECT * FROM {name} LIMIT 10").df().to_string())
            return n
        return _mart

    @task
    def compare_with_etl(_marts):
        """September totals must match the ETL pipeline; log any difference."""
        with connect() as con:
            elt = con.execute("""SELECT round(sum(net_revenue), 2), count(*) FILTER (WHERE delivered_orders > 0)
                                 FROM mart_daily_store_kpis WHERE order_date < DATE '2026-10-01'""").fetchone()[0]
            elt_all = con.execute("SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis").fetchone()[0]
            has_etl = con.execute("SELECT count(*) FROM information_schema.tables "
                                  "WHERE table_name = 'etl_fact_orders'").fetchone()[0]
            etl = etl_all = None
            if has_etl:
                etl, etl_all = con.execute("""SELECT round(sum(net_revenue) FILTER (WHERE order_date < DATE '2026-10-01'), 2),
                                                     round(sum(net_revenue), 2) FROM etl_fact_orders""").fetchone()
        print(f"[compare] September net revenue  ELT {elt}  ETL {etl}")
        print(f"[compare] All loaded net revenue ELT {elt_all}  ETL {etl_all}")
        if etl is not None and abs((elt or 0) - etl) > 0.01:
            print("[WARNING] ELT and ETL September totals differ")
        return {"elt_september": elt, "etl_september": etl}

    # ── RUN AUDIT ────────────────────────────────────────────────────────────
    @task(trigger_rule="all_done", retries=0)
    def audit():
        ti = get_current_context()["ti"]
        extracted = sum(ti.xcom_pull(task_ids=f"load_raw_{k}") or 0 for k in SOURCE_FILES)
        extracted += ti.xcom_pull(task_ids="load_raw_increment") or 0
        loaded = quarantined = None
        try:
            with connect() as con:
                loaded = con.execute("SELECT count(*) FROM stg_orders WHERE dq_status = 'PASSED' "
                                     "AND _run_id = ?", [ti.run_id]).fetchone()[0]
                quarantined = con.execute("SELECT sum(rows_rejected) FROM mart_rejections WHERE run_id = ?",
                                          [ti.run_id]).fetchone()[0]
        except Exception as exc:          # tables may not exist if the run failed early
            print(f"[audit] could not count rows: {exc}")

        def write(row):
            with connect() as con:
                write_pipeline_run(con, row)

        run_audit(DAG_ID, {"rows_extracted": extracted, "rows_loaded": loaded,
                           "rows_quarantined": quarantined}, write)

    paths = {k: make_extract(k) for k in SOURCE_FILES}
    raw = {k: make_load_raw(k, paths[k]) for k in SOURCE_FILES}
    raw_inc = load_raw_increment(extract_increment())

    s_stores = make_staging("stg_stores", "stores")(raw["stores"])
    s_products = make_staging("stg_products", "products")(raw["products"])
    s_customers = make_staging("stg_customers", "customers")(raw["customers"])
    s_orders = stg_orders(raw["orders"], s_stores, s_products, s_customers)
    merged = stg_orders_merge_increment(raw_inc, s_orders)
    s_payments = make_staging("stg_payments", "payments")(raw["payments"])
    raw["orders"] >> s_payments
    raw_inc >> s_payments

    view = mart_view(merged, s_payments, s_customers)
    marts = [make_mart(m)(view) for m in MARTS]
    compare_with_etl(marts) >> audit()


nammamart_elt_divya()
