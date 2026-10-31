"""NammaMart ETL: Extract -> Validate/Quarantine -> Load -> Increment -> Reconcile -> Report -> Audit."""
import re
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum

try:
    from airflow.sdk import dag, task
except ImportError:                       # older Airflow versions
    from airflow.decorators import dag, task

try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    from airflow.exceptions import AirflowFailException

from include.etl_audit import append_dq_results, write_dq_results, write_pipeline_run
from include.etl_incremental import apply_increment
from include.etl_load import bad_order_ids, load_dim, load_fact, purge_fact
from include.etl_quarantine import write_quarantine
from include.etl_reconcile import reconcile
from include.etl_reports import category_margin, daily_zone_revenue
from include.etl_steps import prepare, read_csv
from include.nammamart_config import DATA_DIR, ETL_DAG_ID, INCREMENT_FILE, WAREHOUSE
from include.schema_contract import SchemaError, assert_schema
from include.volume_gate import volume_row

SOURCE_FILES = ["orders", "payments", "customers", "products", "stores"]

default_args = {
    "owner": "anikesh",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


def _now():
    """Current time in IST, without timezone info (our data timestamps are naive IST)."""
    return pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)


def _safe(run_id):
    return re.sub(r"[^A-Za-z0-9_-]", "_", run_id or "manual")


def _table(name):
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    df = con.execute(f"SELECT * FROM {name}").df()
    con.close()
    return df


@dag(
    dag_id=ETL_DAG_ID,
    schedule="0 9 * * 1-6",                                   # 9:00 AM, Monday to Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),   # IST, not UTC
    catchup=False,
    default_args=default_args,
    tags=["nammamart", "etl"],
)
def nammamart_etl():

    @task
    def extract(name: str) -> dict:
        df = read_csv(f"{name}.csv")          # raises FileNotFoundError if the file is missing
        try:
            assert_schema(name, df.columns)   # schema contract: fail loudly, do not guess
        except SchemaError as e:
            raise AirflowFailException(f"schema drift in {name}.csv: {e}")
        print(f"extracted {name}: {len(df)} rows")
        return {"file": name, "rows": len(df)}

    @task(pool="duckdb_writer")
    def validate_and_quarantine(extracted: list, run_id=None) -> dict:
        rid = _safe(run_id)
        now = _now()
        rows = {e["file"]: e["rows"] for e in extracted}
        vrow, base = volume_row(ETL_DAG_ID, rid, rows["orders"], now)
        if vrow["rows_failed"]:                # volume gate: stop before anything reaches the reports
            write_dq_results([vrow], ETL_DAG_ID, rid)
            raise AirflowFailException(
                f"volume gate: orders.csv has {rows['orders']} rows, recent runs had {base}. Nothing was loaded.")
        out = prepare(now, ETL_DAG_ID, rid)
        written = write_quarantine(out["failed"], rid)
        write_dq_results(out["results"] + [vrow], ETL_DAG_ID, rid)      # quality scorecard
        summary = {}
        for name in SOURCE_FILES:
            summary[name] = {"passed": len(out["passed"][name]),
                             "quarantined": len(out["failed"][name])}
            print(f"{name:10} passed={summary[name]['passed']:5} "
                  f"quarantined={summary[name]['quarantined']:4}")
        print("quarantine files:", [p.name for p, _ in written.values()])
        print(f"dq_results: {len(out['results']) + 1} checks recorded")
        return summary

    @task(pool="duckdb_writer")
    def transform_and_load(summary: dict, run_id=None) -> dict:
        out = prepare(_now(), ETL_DAG_ID, _safe(run_id))
        counts = {
            "etl_dim_customer": load_dim(out["dim_customer"], "etl_dim_customer"),
            "etl_dim_product": load_dim(out["dim_product"], "etl_dim_product"),
            "etl_dim_store": load_dim(out["dim_store"], "etl_dim_store"),
            "etl_fact_orders": load_fact(out["fact"]),
        }
        purged = purge_fact(bad_order_ids(out["failed"]["orders"], out["passed"]["orders"]))
        print("loaded:", counts)
        print(f"removed {purged} order(s) from etl_fact_orders that now fail validation")
        return counts

    @task(pool="duckdb_writer")
    def load_increment(counts: dict, run_id=None) -> dict:
        rid = _safe(run_id)
        try:
            res = apply_increment(_now(), ETL_DAG_ID, rid)
        except SchemaError as e:
            raise AirflowFailException(f"schema drift in the increment file: {e}")
        if res["applied"]:
            if len(res["failed"]):
                write_quarantine({"orders_increment": res["failed"]}, rid)
            append_dq_results(res["results"], ETL_DAG_ID, rid)
            print(f"increment: {res['rows']} rows, {res['passed']} clean, {res['quarantined']} quarantined; "
                  f"upsert on order_id: {res['updated']} updated, {res['inserted']} inserted")
        else:
            print("increment file not found: skipped")
        keep = ("applied", "rows", "passed", "quarantined", "inserted", "updated")
        return {k: res[k] for k in keep}

    @task(pool="duckdb_writer")
    def reconcile_payments(inc: dict, run_id=None) -> int:
        fact = _table("etl_fact_orders")                       # includes the increment
        ids = set(read_csv("orders.csv").order_id)
        if (DATA_DIR / INCREMENT_FILE).exists():
            ids |= set(read_csv(INCREMENT_FILE).order_id)
        rec = reconcile(fact, read_csv("payments.csv"), ids)
        n = load_dim(rec, "etl_payment_reconciliation")
        print(f"etl_payment_reconciliation: {n} mismatch rows")
        print(rec.reason.value_counts().to_string())
        return n

    @task(pool="duckdb_writer")
    def report(n_mismatch: int, run_id=None) -> str:
        fact = _table("etl_fact_orders")
        zone = daily_zone_revenue(fact, _table("etl_dim_store"))
        cat = category_margin(fact, _table("etl_dim_product"))
        load_dim(zone, "etl_daily_zone_revenue")
        load_dim(cat, "etl_category_margin")
        print("=== Net revenue by zone ===")
        print(zone.groupby("zone").net_revenue.sum().round(2).to_string())
        print("=== Category margin ===")
        print(cat.to_string(index=False))
        return "done"

    @task(pool="duckdb_writer", trigger_rule="all_done")      # runs even if an upstream task failed
    def run_audit(run_id=None, dag_run=None, ti=None):
        extracted = 0
        for n in SOURCE_FILES:
            v = ti.xcom_pull(task_ids=f"extract_{n}")
            extracted += v["rows"] if v else 0
        summary = ti.xcom_pull(task_ids="validate_and_quarantine")
        quarantined = sum(x["quarantined"] for x in summary.values()) if summary else 0
        loaded = ti.xcom_pull(task_ids="transform_and_load")
        rows_loaded = sum(loaded.values()) if loaded else 0
        inc = ti.xcom_pull(task_ids="load_increment") or {}
        extracted += inc.get("rows", 0)
        rows_loaded += inc.get("passed", 0)
        quarantined += inc.get("quarantined", 0)
        ok = ti.xcom_pull(task_ids="report") == "done"

        end = _now()
        start = dag_run.start_date if dag_run and dag_run.start_date else None
        start = pd.Timestamp(start).tz_convert("Asia/Kolkata").tz_localize(None) if start else end

        write_pipeline_run(ETL_DAG_ID, _safe(run_id), start, end, extracted,
                           rows_loaded, quarantined, "SUCCESS" if ok else "FAILED")
        print(f"audit: extracted={extracted} loaded={rows_loaded} "
              f"quarantined={quarantined} status={'SUCCESS' if ok else 'FAILED'}")
        if not ok:                             # make the DAG run RED, not green, when the pipeline failed
            raise AirflowFailException("pipeline failed: see the failed task above; details in pipeline_runs")

    extracts = [extract.override(task_id=f"extract_{n}")(n) for n in SOURCE_FILES]
    v = validate_and_quarantine(extracts)
    t = transform_and_load(v)
    i = load_increment(t)
    r = reconcile_payments(i)
    rep = report(r)
    audit = run_audit()
    [*extracts, v, t, i, r, rep] >> audit


nammamart_etl()
