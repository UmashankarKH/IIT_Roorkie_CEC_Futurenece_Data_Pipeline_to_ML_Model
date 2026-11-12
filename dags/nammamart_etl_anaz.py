"""
### NammaMart ETL pipeline (anaz)

**Extract → Validate → Transform → Load → Reconcile → Report → Audit**

* Five source files are extracted in parallel. The next-day increment file is
  loaded only when the run is triggered with `{"load_increment": true}`.
* Every row is checked with the shared rules in `include/dq_rules.py`.
  Critical failures go to `include/quarantine/anaz/<file>_failed_<run_id>.csv`
  with a `failure_reason`; warnings load and are noted in `dq_warnings`.
* Loads a star schema into `include/warehouse_anaz.duckdb`:
  `etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`.
  Email, phone and name are masked. Re-running never double-counts.
* Builds `etl_payment_reconciliation`, `etl_daily_zone_revenue`,
  `etl_category_margin`, and writes `dq_results` and `pipeline_runs`.
* Run-level checks (volume, store distribution, margin) turn the run red when
  today's numbers are suspect (`include/observability.py`).

Runs at **09:00 IST, Monday to Saturday**. Pipeline logic: `include/nammamart_etl.py`.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

from include import nammamart_etl as etl

DAG_ID = "nammamart_etl_anaz"

default_args = {
    "owner": "anaz",
    "retries": 2,                              # absorbs transient failures (file not yet copied, DB lock)
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),  # a hung task fails instead of blocking the 9:30 review
}


@dag(
    dag_id=DAG_ID,
    description="NammaMart ETL: validate in Python, load only clean rows to a DuckDB star schema",
    schedule="0 9 * * 1-6",                    # 09:00, Monday-Saturday ...
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),  # ... in IST, not UTC
    catchup=False,                             # only today's report matters; don't replay missed days
    max_active_runs=1,
    default_args=default_args,
    params={
        "as_of": "2026-10-01",                 # run date used by the timeliness checks
        "load_increment": False,               # trigger with {"load_increment": true} for Experiment 3
    },
    tags=["nammamart", "etl", "anaz"],
    doc_md=__doc__,
)
def nammamart_etl_anaz():

    @task
    def extract(name: str, **context) -> dict:
        return etl.extract(name, context["run_id"])

    @task
    def extract_increment(**context) -> dict | None:
        if not context["params"].get("load_increment"):
            print("load_increment is false: the increment file is not part of this run")
            return None
        return etl.extract(etl.INCREMENT, context["run_id"])

    @task
    def validate(name: str, orders: dict, payments: dict, customers: dict, products: dict,
                 stores: dict, orders_increment: dict | None, **context) -> dict | None:
        raw_infos = {"orders": orders, "payments": payments, "customers": customers,
                     "products": products, "stores": stores, etl.INCREMENT: orders_increment}
        return etl.validate(name, raw_infos, as_of=context["params"]["as_of"],
                            run_id=context["run_id"], dag_id=context["dag_run"].dag_id)

    @task
    def transform(orders: dict, payments: dict, customers: dict, products: dict, stores: dict,
                  orders_increment: dict | None, **context) -> dict:
        validated = {"orders": orders, "payments": payments, "customers": customers,
                     "products": products, "stores": stores, etl.INCREMENT: orders_increment}
        return etl.transform(validated, as_of=context["params"]["as_of"], run_id=context["run_id"])

    @task
    def load(transformed: dict, orders: dict, payments: dict, customers: dict, products: dict,
             stores: dict, orders_increment: dict | None, **context) -> dict:
        validated = {"orders": orders, "payments": payments, "customers": customers,
                     "products": products, "stores": stores, etl.INCREMENT: orders_increment}
        return etl.load(transformed, validated, dag_id=context["dag_run"].dag_id)

    @task
    def reconcile(loaded: dict, orders: dict, payments: dict, orders_increment: dict | None,
                  **context) -> dict:
        validated = {"orders": orders, "payments": payments, etl.INCREMENT: orders_increment}
        return etl.reconcile(validated, run_id=context["run_id"])

    @task
    def report(reconciled: dict) -> dict:
        return etl.report()

    @task(retries=0)  # an anomaly in the data won't go away by retrying: report it at once
    def observability_checks(loaded: dict, reported: dict, **context) -> dict:
        return etl.observe(loaded, context["run_id"], context["dag_run"].dag_id)

    @task(trigger_rule="all_done", retries=0)  # runs even when something upstream failed; no retry
    def run_audit(**context) -> dict:
        ti = context["ti"]

        def pull(task_id):
            return ti.xcom_pull(task_ids=task_id)

        extracted = [pull(f"extract_{n}") for n in etl.MASTER_FILES] + [pull("extract_increment")]
        validated = [pull(f"validate_{n}") for n in etl.MASTER_FILES + [etl.INCREMENT]]
        loaded = pull("load")
        finished = pull("report") is not None
        checked = pull("observability_checks") is not None
        status = "success" if finished and checked else ("anomaly" if finished else "failed")
        row = etl.audit(context["run_id"], context["dag_run"].dag_id, context["dag_run"].start_date,
                        extracted, validated, loaded, finished, status=status)
        if status != "success":
            # The audit row is written; now fail this task so the whole run shows as FAILED.
            # (Airflow judges a run by its last task: a green run_audit would make a broken run look green.)
            if status == "anomaly":
                raise RuntimeError("Pipeline finished but a critical observability check failed: today's numbers "
                                   "are suspect (see observability_checks). Audit row written with status 'anomaly'.")
            raise RuntimeError("Pipeline did not finish: see the failed/upstream_failed tasks. "
                               "Audit row written with status 'failed'.")
        return row

    # ---- wiring --------------------------------------------------------------
    raw = {n: extract.override(task_id=f"extract_{n}")(n) for n in etl.MASTER_FILES}
    raw_inc = extract_increment()
    raw_args = [raw[n] for n in etl.MASTER_FILES] + [raw_inc]

    val = {n: validate.override(task_id=f"validate_{n}")(n, *raw_args)
           for n in etl.MASTER_FILES + [etl.INCREMENT]}
    val_args = [val[n] for n in etl.MASTER_FILES] + [val[etl.INCREMENT]]

    transformed = transform(*val_args)
    loaded = load(transformed, *val_args)
    reconciled = reconcile(loaded, val["orders"], val["payments"], val[etl.INCREMENT])
    reported = report(reconciled)
    observability_checks(loaded, reported) >> run_audit()


nammamart_etl_anaz()
