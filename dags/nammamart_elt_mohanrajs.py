"""
dags/nammamart_elt_mohanrajs.py
---------------------------
NammaMart ELT pipeline: load everything first, clean inside the warehouse with SQL.

  extract_orders    ─┐
  extract_payments  ─┤
  extract_customers ─┼─► load_raw ─► check_schema ─► stg_stores ─► stg_products ─► stg_customers ─► stg_orders ─► stg_payments
  extract_products  ─┤                                                                                            │
  extract_stores    ─┘                              run_audit (all_done) ◄─ report ◄─ build_marts ◄─ dq_results ◄──┘

Layers (include/warehouse_mohanrajs.duckdb):  raw_*  ->  stg_*  ->  mart_*      (SQL in include/sql/elt/)
* Raw     : exact copy of the CSVs, all text, + _loaded_at / _source_file / _run_id. Never cleaned, never deleted.
* Staging : cast, standardise, dedupe, mask PII, DQ flags (dq_status, failure_reason). Bad rows stay visible here.
* Marts   : only rows with dq_status <> 'FAIL'.
* Schedule: 09:00 IST, Monday-Saturday.  Param load_increment=True appends the increment file to raw and MERGEs it
            into stg_orders on order_id.
"""
import sys
from datetime import timedelta

import pandas as pd
import pendulum

try:  # Airflow 3
    from airflow.sdk import Param, Variable, dag, get_current_context, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task
    from airflow.models import Variable
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context

try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # Airflow 2
    from airflow.exceptions import AirflowFailException

sys.path.insert(0, "/usr/local/airflow/include")
import nammamart_common as nc  # noqa: E402
import nammamart_elt_lib as elt  # noqa: E402

DAG_ID = "nammamart_elt_mohanrajs"

def _read_source(name):
    """A missing file may just be late, so it is retried. A renamed/missing COLUMN will not fix itself: fail at once."""
    try:
        return nc.read_csv_raw(name)
    except ValueError as e:
        raise AirflowFailException(str(e)) from e


default_args = {
    "owner": "mohanrajs",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": nc.slack_alert,
}


def _mode():
    return "incremental" if get_current_context()["params"].get("load_increment") else "full"


def _params():
    ctx = get_current_context()
    as_of = nc.resolve_as_of(Variable.get("nammamart_as_of", default=None))
    return {"run_id": ctx["run_id"], "dag_id": DAG_ID, "as_of": as_of.strftime("%Y-%m-%d %H:%M:%S"), "mode": _mode()}


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"load_increment": Param(False, type="boolean",
                                    description="Append orders_2026-10-01_increment.csv to raw and MERGE into stg_orders")},
    tags=["fde", "nammamart", "elt"],
)
def nammamart_elt_mohanrajs():

    # ───────────────────────── EXTRACT (5 parallel tasks) ─────────────────────────
    def _extract(name, source=None):
        source = source or name
        df = _read_source(source)  # fails fast if the file is missing or a column was renamed
        print(f"[Extract] {name}: {len(df)} rows in {nc.file_path(source).name}")
        return {"name": name, "source": source, "rows": len(df)}

    @task
    def extract_orders():
        return _extract("orders", "orders_increment" if _mode() == "incremental" else "orders")

    @task
    def extract_payments():
        return _extract("payments")

    @task
    def extract_customers():
        return _extract("customers")

    @task
    def extract_products():
        return _extract("products")

    @task
    def extract_stores():
        return _extract("stores")

    # ───────────────────────── LOAD RAW (no cleaning) ─────────────────────────
    @task
    def load_raw(extracts):
        run_id, mode = get_current_context()["run_id"], _mode()
        con = nc.connect()
        loaded = {}
        try:
            for e in extracts:
                if mode == "incremental" and e["name"] != "orders":
                    continue  # the increment only carries orders
                loaded[e["source"]] = elt.load_raw(con, e["source"], run_id)
            elt.run_sql_file(con, "05_raw_views.sql")
            for t in sorted(set(elt.RAW_TABLES.values())):
                print(f"[Raw] {t}: {con.execute(f'SELECT count(*) FROM {t}').fetchone()[0]} rows total")
        finally:
            con.close()
        print(f"[Raw] loaded this run: {loaded}")
        return {"rows": sum(loaded.values()), "by_file": loaded}

    # ───────────────────────── STAGING (SQL) ─────────────────────────
    def _stage(file, table=None, **extra):
        p = _params()
        con = nc.connect()
        try:
            elt.run_sql_file(con, file, **p, **extra)
            n_fail = con.execute(f"SELECT count(*) FILTER (WHERE dq_status = 'FAIL'), count(*) FROM {table}").fetchone()
        finally:
            con.close()
        print(f"[Staging] {table}: rows={n_fail[1]} failed={n_fail[0]}")
        return {"table": table, "rows": n_fail[1], "failed": n_fail[0]}

    @task
    def check_schema(raw):
        # Raw accepted whatever arrived; refuse to transform if a column the SQL needs is gone.
        con = nc.connect(read_only=True)
        try:
            for name, table in (("orders", "raw_orders"), ("payments", "raw_payments"), ("customers", "raw_customers"),
                                ("products", "raw_products"), ("stores", "raw_stores")):
                cols = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
                nc.check_schema(name, cols)
        finally:
            con.close()
        return raw

    @task
    def stg_stores(raw):
        return _stage("20_stg_stores.sql", "stg_stores")

    @task
    def stg_products(prev):
        return _stage("21_stg_products.sql", "stg_products")

    @task
    def stg_customers(prev):
        return _stage("22_stg_customers.sql", "stg_customers")

    @task
    def stg_orders(prev):
        """Full: rebuild from the latest orders.csv load. Incremental: MERGE the increment into stg_orders."""
        mode = _mode()
        src = nc.INCREMENT_FILE if mode == "incremental" else nc.FILES["orders"]
        out = _stage("23_stg_orders.sql", "stg_orders_batch", target="stg_orders_batch", source_file=src)
        p = _params()
        con = nc.connect()
        try:
            before = (con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
                      if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'stg_orders'")
                      .fetchone()[0] else 0)
            elt.run_sql_file(con, "25_stg_orders_merge.sql" if mode == "incremental" else "24_stg_orders_publish.sql",
                             source_file=src, **p)
            after = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            updated = con.execute("SELECT count(*) FROM stg_order_status_history WHERE changed_in_run = ?",
                                  [p["run_id"]]).fetchone()[0]
        finally:
            con.close()
        print(f"[Staging] mode={mode} batch_rows={out['rows']} stg_orders {before} -> {after} "
              f"(inserted={after - before}, status corrections={updated})")
        return {**out, "inserted": after - before, "updated": updated, "total": after}

    @task
    def stg_payments(prev):
        return _stage("26_stg_payments.sql", "stg_payments")

    # ───────────────────────── DATA QUALITY SCORECARD ─────────────────────────
    @task
    def dq_results(prev):
        con = nc.connect()
        try:
            elt.run_sql_file(con, "30_dq_results.sql", **_params())
            rows = con.execute("SELECT severity, status, count(*) FROM dq_results WHERE run_id = ? GROUP BY ALL "
                               "ORDER BY 1, 2", [get_current_context()["run_id"]]).fetchall()
        finally:
            con.close()
        print(f"[DQ] checks by severity/status: {rows}")
        return {"checks": sum(r[2] for r in rows)}

    # ───────────────────────── MARTS (SQL) ─────────────────────────
    @task
    def build_marts(prev):
        con = nc.connect()
        try:
            elt.run_sql_file(con, "40_marts.sql", **_params())
            for t in ("mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
                      "mart_payment_mode_revenue", "mart_payment_reconciliation", "mart_top_customers",
                      "mart_rejected_rows"):
                print(f"[Mart] {t}: {con.execute(f'SELECT count(*) FROM {t}').fetchone()[0]} rows")
        finally:
            con.close()
        return {"ok": True}

    @task
    def report(prev):
        con = nc.connect(read_only=True)
        try:
            print("[Report] mart_category_margin")
            print(con.execute("SELECT * FROM mart_category_margin").fetchdf().to_string(index=False))
            print("[Report] mart_delivery_sla")
            print(con.execute("SELECT * FROM mart_delivery_sla").fetchdf().to_string(index=False))
            print("[Report] reconciliation: mismatches by reason")
            print(con.execute("SELECT mismatch_reason, count(*) AS rows, round(sum(difference), 2) AS total_difference "
                              "FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC").fetchdf().to_string(index=False))
            print("[Report] rejected rows by file and reason")
            print(con.execute("SELECT source_file, failure_reason, count(*) AS rows FROM mart_rejected_rows "
                              "GROUP BY 1, 2 ORDER BY 1, 3 DESC").fetchdf().to_string(index=False))
        finally:
            con.close()
        return {"ok": True}

    # ───────────────────────── RUN AUDIT (always runs) ─────────────────────────
    @task(trigger_rule="all_done", retries=0)  # the audit must report a failure, not retry it
    def run_audit():
        ctx = get_current_context()
        ti = ctx["ti"]
        pull = lambda t: ti.xcom_pull(task_ids=t)  # noqa: E731
        raw, rep = pull("load_raw"), pull("report")
        stg = [pull(t) for t in ("stg_stores", "stg_products", "stg_customers", "stg_orders", "stg_payments")]
        row = {
            "run_id": ctx["run_id"], "dag_id": DAG_ID,
            "start_time": pd.Timestamp(ctx["dag_run"].start_date).tz_convert("Asia/Kolkata").tz_localize(None),
            "end_time": pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None),
            "rows_extracted": raw["rows"] if raw else 0,
            "rows_loaded": raw["rows"] if raw else 0,  # ELT lands every extracted row in raw
            "rows_quarantined": sum(s["failed"] for s in stg if s),  # rows flagged FAIL in staging, kept but excluded from marts
            "status": "SUCCESS" if rep and rep.get("ok") else "FAILED",
        }
        con = nc.connect()
        try:
            nc.write_pipeline_run(con, row)
        finally:
            con.close()
        print(f"[Audit] {row}")
        if row["status"] != "SUCCESS":
            raise RuntimeError("Pipeline run did not complete; see failed tasks above")

    # ───────────────────────── WIRING ─────────────────────────
    extracts = [extract_orders(), extract_payments(), extract_customers(), extract_products(), extract_stores()]
    raw = load_raw(extracts)
    s1 = stg_stores(check_schema(raw))
    s2 = stg_products(s1)
    s3 = stg_customers(s2)
    s4 = stg_orders(s3)
    s5 = stg_payments(s4)
    dq = dq_results(s5)
    marts = build_marts(dq)
    rep = report(marts)
    audit = run_audit()
    for t in (*extracts, raw, s1, s2, s3, s4, s5, dq, marts, rep):
        t >> audit


nammamart_elt_mohanrajs()
