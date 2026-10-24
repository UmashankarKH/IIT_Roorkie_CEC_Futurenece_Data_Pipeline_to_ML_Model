"""NammaMart ETL: Extract -> Validate/Quarantine -> Transform+Load -> Reconcile -> Report -> Audit."""
import re
from datetime import timedelta

import pandas as pd
import pendulum

try:
    from airflow.sdk import dag, task
except ImportError:                       # older Airflow versions
    from airflow.decorators import dag, task

from include.etl_audit import write_dq_results, write_pipeline_run
from include.etl_load import load_dim, load_fact
from include.etl_quarantine import write_quarantine
from include.etl_reconcile import reconcile
from include.etl_reports import category_margin, daily_zone_revenue
from include.etl_steps import prepare, read_csv
from include.nammamart_config import ETL_DAG_ID

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
        print(f"extracted {name}: {len(df)} rows")
        return {"file": name, "rows": len(df)}

    @task
    def validate_and_quarantine(extracted: list, run_id=None) -> dict:
        rid = _safe(run_id)
        out = prepare(_now(), ETL_DAG_ID, rid)
        written = write_quarantine(out["failed"], rid)
        write_dq_results(out["results"], ETL_DAG_ID, rid)      # quality scorecard
        summary = {}
        for name in SOURCE_FILES:
            summary[name] = {"passed": len(out["passed"][name]),
                             "quarantined": len(out["failed"][name])}
            print(f"{name:10} passed={summary[name]['passed']:5} "
                  f"quarantined={summary[name]['quarantined']:4}")
        print("quarantine files:", [p.name for p, _ in written.values()])
        print(f"dq_results: {len(out['results'])} checks recorded")
        return summary

    @task
    def transform_and_load(summary: dict, run_id=None) -> dict:
        out = prepare(_now(), ETL_DAG_ID, _safe(run_id))
        counts = {
            "etl_dim_customer": load_dim(out["dim_customer"], "etl_dim_customer"),
            "etl_dim_product": load_dim(out["dim_product"], "etl_dim_product"),
            "etl_dim_store": load_dim(out["dim_store"], "etl_dim_store"),
            "etl_fact_orders": load_fact(out["fact"]),
        }
        print("loaded:", counts)
        return counts

    @task
    def reconcile_payments(counts: dict, run_id=None) -> int:
        out = prepare(_now(), ETL_DAG_ID, _safe(run_id))
        rec = reconcile(out["fact"], out["payments_raw"], out["all_order_ids"])
        n = load_dim(rec, "etl_payment_reconciliation")
        print(f"etl_payment_reconciliation: {n} mismatch rows")
        print(rec.reason.value_counts().to_string())
        return n

    @task
    def report(n_mismatch: int, run_id=None) -> str:
        out = prepare(_now(), ETL_DAG_ID, _safe(run_id))
        zone = daily_zone_revenue(out["fact"], out["dim_store"])
        cat = category_margin(out["fact"], out["dim_product"])
        load_dim(zone, "etl_daily_zone_revenue")
        load_dim(cat, "etl_category_margin")
        print("=== Net revenue by zone ===")
        print(zone.groupby("zone").net_revenue.sum().round(2).to_string())
        print("=== Category margin ===")
        print(cat.to_string(index=False))
        return "done"

    @task(trigger_rule="all_done")            # runs even if an upstream task failed
    def run_audit(run_id=None, dag_run=None, ti=None):
        extracted = 0
        for n in SOURCE_FILES:
            v = ti.xcom_pull(task_ids=f"extract_{n}")
            extracted += v["rows"] if v else 0
        summary = ti.xcom_pull(task_ids="validate_and_quarantine")
        quarantined = sum(x["quarantined"] for x in summary.values()) if summary else 0
        loaded = ti.xcom_pull(task_ids="transform_and_load")
        rows_loaded = sum(loaded.values()) if loaded else 0
        ok = ti.xcom_pull(task_ids="report") == "done"

        end = _now()
        start = dag_run.start_date if dag_run and dag_run.start_date else None
        start = pd.Timestamp(start).tz_convert("Asia/Kolkata").tz_localize(None) if start else end

        write_pipeline_run(ETL_DAG_ID, _safe(run_id), start, end, extracted,
                           rows_loaded, quarantined, "SUCCESS" if ok else "FAILED")
        print(f"audit: extracted={extracted} loaded={rows_loaded} "
              f"quarantined={quarantined} status={'SUCCESS' if ok else 'FAILED'}")

    extracts = [extract.override(task_id=f"extract_{n}")(n) for n in SOURCE_FILES]
    v = validate_and_quarantine(extracts)
    t = transform_and_load(v)
    r = reconcile_payments(t)
    rep = report(r)
    audit = run_audit()
    [*extracts, v, t, r, rep] >> audit


nammamart_etl()