"""
### NammaMart ELT pipeline (anaz)

**Extract → Load raw → Transform in DuckDB with SQL (staging → mart)**

* **Raw** (`raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores`):
  every row exactly as received, all text, plus `_loaded_at`, `_source_file`, `_run_id`.
  Append-only: never cleaned, never deleted. Re-runs make it grow, on purpose.
* **Staging** (`stg_*`): SQL only. Casts, standardises, de-duplicates, masks personal data,
  and flags every row with `dq_status` / `failure_reason`. Bad rows stay, flagged.
  `stg_orders` is upserted (MERGE) on `order_id`; the 1 October increment updates
  returned orders and adds new ones.
* **Mart** (`mart_*`): SQL only. Business tables for the six questions.
* Run-level checks (volume, store distribution, margin) turn the run red when
  today's numbers are suspect (`include/observability.py`).

All SQL lives in `include/sql/`. The quality rules are shared with the ETL DAG through
`include/dq_rules.py` (`include/sql/staging/generated/`). Runs at **09:00 IST, Monday to Saturday**.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

from include import nammamart_elt as elt

DAG_ID = "nammamart_elt_anaz"

default_args = {
    "owner": "anaz",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


def _params(context) -> dict:
    return elt.base_params(context["run_id"], context["dag_run"].dag_id, context["params"]["as_of"])


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: load raw as-is, transform with SQL in DuckDB (raw -> staging -> mart)",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"as_of": "2026-10-01", "load_increment": False},
    tags=["nammamart", "elt", "anaz"],
    doc_md=__doc__,
)
def nammamart_elt_anaz():

    @task
    def extract(name: str, **context) -> dict:
        return elt.extract(name, context["run_id"])

    @task
    def extract_increment(**context) -> dict | None:
        if not context["params"].get("load_increment"):
            print("load_increment is false: the increment file is not part of this run")
            return None
        return elt.extract(elt.INCREMENT, context["run_id"])

    @task
    def load_raw(orders: dict, payments: dict, customers: dict, products: dict, stores: dict,
                 orders_increment: dict | None, **context) -> dict:
        extracted = {"orders": orders, "payments": payments, "customers": customers,
                     "products": products, "stores": stores, elt.INCREMENT: orders_increment}
        return elt.load_raw(extracted, context["run_id"])

    @task
    def staging_standardise(raw_info: dict, **context) -> dict:
        elt.staging_standardise(raw_info, _params(context))
        return raw_info

    @task
    def staging_quality_checks(raw_info: dict, **context) -> dict:
        elt.staging_quality_checks(raw_info, _params(context))
        return raw_info

    @task
    def staging_build(raw_info: dict, **context) -> dict:
        elt.staging_build(_params(context))
        return raw_info

    @task
    def merge_orders(raw_info: dict, **context) -> dict:
        elt.merge_orders("orders", raw_info, _params(context))
        return raw_info

    @task
    def merge_increment(raw_info: dict, **context) -> dict:
        elt.merge_orders(elt.INCREMENT, raw_info, _params(context))
        return raw_info

    @task
    def build_marts(raw_info: dict, **context) -> dict:
        return elt.build_marts(_params(context))

    @task
    def compare_with_etl(marts: dict, **context) -> dict:
        return elt.compare_with_etl(context["params"]["as_of"])

    @task(retries=0)  # an anomaly in the data won't go away by retrying: report it at once
    def observability_checks(marts: dict, **context) -> dict:
        return elt.observe(context["run_id"], context["dag_run"].dag_id)

    @task(trigger_rule="all_done", retries=0)  # runs even when something upstream failed; no retry
    def run_audit(**context) -> dict:
        ti = context["ti"]
        raw_info = ti.xcom_pull(task_ids="load_raw")
        if raw_info is None:  # load_raw never ran: count what the extract tasks did read
            got = [ti.xcom_pull(task_ids=f"extract_{n}") for n in elt.BASE_FILES] + \
                  [ti.xcom_pull(task_ids="extract_increment")]
            raw_info = {"rows_extracted": sum(g["rows"] for g in got if g)}
        finished = (ti.xcom_pull(task_ids="build_marts") is not None
                    and ti.xcom_pull(task_ids="compare_with_etl") is not None)
        checked = ti.xcom_pull(task_ids="observability_checks") is not None
        status = "success" if finished and checked else ("anomaly" if finished else "failed")
        row = elt.audit(context["run_id"], context["dag_run"].dag_id, context["dag_run"].start_date,
                        raw_info, finished, status=status)
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
    raw = [extract.override(task_id=f"extract_{n}")(n) for n in elt.BASE_FILES] + [extract_increment()]
    loaded = load_raw(*raw)
    staged = staging_build(staging_quality_checks(staging_standardise(loaded)))
    merged = merge_increment(merge_orders(staged))
    marts = build_marts(merged)
    checked = observability_checks(marts)
    compared = compare_with_etl(marts)
    compared >> checked >> run_audit()        # one at a time: DuckDB allows a single writer


nammamart_elt_anaz()
