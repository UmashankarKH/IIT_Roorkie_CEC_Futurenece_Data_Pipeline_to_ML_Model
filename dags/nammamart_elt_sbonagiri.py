"""
NammaMart ELT pipeline: Extract -> Load raw (untouched) -> Transform in the warehouse with SQL.

  extract_* (parallel) -> load_raw -> stg_* (SQL) -> stg_orders_merge (MERGE) -> mart_* (SQL) -> report -> run_audit

Raw tables are append-only. Staging and marts are rebuilt or merged from SQL files in include/sql/.
Runs at 09:00 IST, Monday to Saturday. Set the `load_increment` param to also load the next-day orders file.
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

import nammamart_common as nc  # noqa: E402
import nammamart_elt as elt  # noqa: E402

DAG_ID = elt.DAG_ID

default_args = {
    "owner": nc.OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


def _ctx():
    ctx = get_current_context()
    params = ctx["params"]
    return ctx["run_id"], bool(params["load_increment"]), params["data_dir"] or None, params["as_of_date"] or None, ctx


def _extract(name, data_dir):
    try:
        return elt.extract(name, data_dir)
    except ValueError as exc:  # a schema change will not fix itself, so skip the retries
        raise AirflowFailException(str(exc)) from exc


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,  # staging is rebuilt from the latest raw load, so replaying missed days adds nothing
    max_active_runs=1,
    default_args=default_args,
    params={
        "load_increment": Param(False, type="boolean", description="Also load orders_2026-10-01_increment.csv"),
        "data_dir": Param("", type="string", description="Override the source folder (empty = include/data/nammamart)"),
        "as_of_date": Param("", type="string", description="Override 'today' for timeliness checks, YYYY-MM-DD"),
    },
    tags=["nammamart", "elt", "assignment"],
)
def nammamart_elt():

    # ---- Extract: check each file and its schema, in parallel ------------------------------------------
    @task
    def extract_orders():
        return _extract("orders", _ctx()[2])

    @task
    def extract_payments():
        return _extract("payments", _ctx()[2])

    @task
    def extract_customers():
        return _extract("customers", _ctx()[2])

    @task
    def extract_products():
        return _extract("products", _ctx()[2])

    @task
    def extract_stores():
        return _extract("stores", _ctx()[2])

    @task
    def extract_increment():
        _, load_inc, data_dir, _, _ = _ctx()
        if not load_inc:
            print("[Extract] increment skipped (load_increment=False)")
            return {"skipped": True}
        return _extract(nc.INCREMENT_FILE, data_dir)

    # ---- Load raw: exact copy, all columns as text -----------------------------------------------------
    @task
    def load_raw(_extracts):
        run_id, load_inc, data_dir, _, _ = _ctx()
        return elt.load_raw(run_id, load_inc, data_dir)

    # ---- Staging: one SQL file per table ---------------------------------------------------------------
    def sql_task(task_id: str, file_name: str):
        @task(task_id=task_id)
        def run(_upstream=None):
            run_id, _, _, as_of, _ = _ctx()
            elt.run_sql_file(file_name, run_id, as_of)
            return {"sql": file_name}

        return run

    @task
    def stg_orders_merge(_upstream):
        run_id, _, _, as_of, _ = _ctx()
        return elt.merge_orders(run_id, as_of)

    @task
    def report(_upstream):
        return elt.report()

    @task(trigger_rule="all_done")
    def run_audit():
        run_id, _, _, _, ctx = _ctx()
        ti = ctx["ti"]
        pull = lambda task_id: ti.xcom_pull(task_ids=task_id, default=None)  # noqa: E731
        raw = pull("load_raw")
        status = nc.dag_run_status(ti, final_task="report")
        try:
            counts = elt.run_counts(run_id) if pull("stg_orders_merge") else None
        except Exception:  # staging tables may not exist if the run failed early
            counts = None
        counts = counts or {"rows_loaded": 0, "rows_quarantined": 0}
        con = nc.connect()
        try:
            nc.write_run_audit(con, run_id, DAG_ID, ctx["dag_run"].start_date,
                               raw["rows_extracted"] if raw else 0, counts["rows_loaded"],
                               counts["rows_quarantined"], status)
        finally:
            con.close()
        print(f"[Audit] run {run_id}: {status}")
        if status == "FAILED":  # run_audit is the only leaf task, so without this the run would show green
            raise AirflowFailException("A task in this run failed; see pipeline_runs and the failed task logs")

    extracts = [extract_orders(), extract_payments(), extract_customers(), extract_products(), extract_stores(),
                extract_increment()]
    raw = load_raw(extracts)

    stg_customers = sql_task("stg_customers", "stg_customers.sql")(raw)
    stg_products = sql_task("stg_products", "stg_products.sql")(raw)
    stg_stores = sql_task("stg_stores", "stg_stores.sql")(raw)
    stg_payments = sql_task("stg_payments", "stg_payments.sql")(raw)
    stg_orders_incoming = sql_task("stg_orders_incoming", "stg_orders_incoming.sql")(raw)
    [stg_customers, stg_products, stg_stores] >> stg_orders_incoming
    merged = stg_orders_merge(stg_orders_incoming)

    mart_kpis = sql_task("mart_kpis", "mart_kpis.sql")(merged)
    mart_recon = sql_task("mart_payment_reconciliation", "mart_payment_reconciliation.sql")(merged)
    mart_rejected = sql_task("mart_rejected_rows", "mart_rejected_rows.sql")(merged)
    reported = report(merged)
    [mart_kpis, mart_recon, mart_rejected] >> reported
    [reported, stg_payments] >> run_audit()


nammamart_elt()
