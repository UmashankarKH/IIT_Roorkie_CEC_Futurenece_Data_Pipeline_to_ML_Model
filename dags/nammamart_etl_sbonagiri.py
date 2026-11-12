"""
NammaMart ETL pipeline: Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report.

  extract_* (5 parallel) -> validate_* -> load_warehouse -> reconcile -> report -> run_audit (always runs)

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
import nammamart_etl as etl  # noqa: E402

DAG_ID = etl.DAG_ID

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
        return etl.extract(name, data_dir)
    except ValueError as exc:  # a schema change will not fix itself, so skip the retries
        raise AirflowFailException(str(exc)) from exc


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,  # reports are rebuilt from full files, so replaying missed days adds nothing
    max_active_runs=1,
    default_args=default_args,
    params={
        "load_increment": Param(False, type="boolean", description="Also load orders_2026-10-01_increment.csv"),
        "data_dir": Param("", type="string", description="Override the source folder (empty = include/data/nammamart)"),
        "as_of_date": Param("", type="string", description="Override 'today' for timeliness checks, YYYY-MM-DD"),
    },
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl():

    # ---- Extract: one task per file, run in parallel ---------------------------------------------------
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
        run_id, load_inc, data_dir, _, _ = _ctx()
        if not load_inc:
            print("[Extract] increment skipped (load_increment=False)")
            return {"skipped": True}
        return _extract(nc.INCREMENT_FILE, data_dir)

    # ---- Validate (and quarantine) ---------------------------------------------------------------------
    @task
    def validate_customers(_extract):
        run_id, _, data_dir, as_of, _ = _ctx()
        return etl.validate_dimension("customers", run_id, data_dir, as_of)

    @task
    def validate_products(_extract):
        run_id, _, data_dir, as_of, _ = _ctx()
        return etl.validate_dimension("products", run_id, data_dir, as_of)

    @task
    def validate_stores(_extract):
        run_id, _, data_dir, as_of, _ = _ctx()
        return etl.validate_dimension("stores", run_id, data_dir, as_of)

    @task
    def validate_orders(_extract, customers, products, stores):
        run_id, _, data_dir, as_of, _ = _ctx()
        return etl.validate_orders("orders", run_id, customers, products, stores, data_dir, as_of)

    @task
    def validate_increment(_extract, customers, products, stores):
        run_id, load_inc, data_dir, as_of, _ = _ctx()
        if not load_inc:
            return {"skipped": True}
        return etl.validate_orders(nc.INCREMENT_FILE, run_id, customers, products, stores, data_dir, as_of)

    @task
    def validate_payments(_extract):
        run_id, load_inc, data_dir, as_of, _ = _ctx()
        return etl.validate_payments(run_id, load_inc, data_dir, as_of)

    # ---- Transform + Load, Reconcile, Report -----------------------------------------------------------
    @task
    def load_warehouse(customers, products, stores, orders, increment, payments):
        run_id = _ctx()[0]
        return etl.load_warehouse(run_id, customers, products, stores, orders, increment, payments)

    @task
    def reconcile(_load):
        return etl.reconcile(_ctx()[0])

    @task
    def report(_reconcile):
        return etl.report()

    @task(trigger_rule="all_done")
    def run_audit():
        run_id, _, _, _, ctx = _ctx()
        ti = ctx["ti"]
        pull = lambda task_id: ti.xcom_pull(task_ids=task_id, default=None)  # noqa: E731
        extracts = [pull(t) for t in ("extract_orders", "extract_payments", "extract_customers", "extract_products",
                                      "extract_stores", "extract_increment")]
        validates = [pull(t) for t in ("validate_customers", "validate_products", "validate_stores",
                                       "validate_orders", "validate_increment", "validate_payments")]
        load = pull("load_warehouse")
        status = nc.dag_run_status(ti, final_task="report")
        con = nc.connect()
        try:
            nc.write_run_audit(
                con, run_id, DAG_ID, ctx["dag_run"].start_date,
                sum(e.get("rows", 0) for e in extracts if e),
                load["rows_loaded"] if load else 0,
                sum(v.get("rows_quarantined", 0) for v in validates if v), status)
        finally:
            con.close()
        etl.cleanup_stage(run_id)
        print(f"[Audit] run {run_id}: {status}")
        if status == "FAILED":  # run_audit is the only leaf task, so without this the run would show green
            raise AirflowFailException("A task in this run failed; see pipeline_runs and the failed task logs")

    e_orders, e_pay, e_cust, e_prod, e_store, e_inc = (
        extract_orders(), extract_payments(), extract_customers(), extract_products(), extract_stores(),
        extract_increment())
    v_cust, v_prod, v_store = validate_customers(e_cust), validate_products(e_prod), validate_stores(e_store)
    v_orders = validate_orders(e_orders, v_cust, v_prod, v_store)
    v_inc = validate_increment(e_inc, v_cust, v_prod, v_store)
    v_pay = validate_payments(e_pay)
    loaded = load_warehouse(v_cust, v_prod, v_store, v_orders, v_inc, v_pay)
    reported = report(reconcile(loaded))
    [reported, v_orders, v_inc, v_pay, v_cust, v_prod, v_store] >> run_audit()


nammamart_etl()
