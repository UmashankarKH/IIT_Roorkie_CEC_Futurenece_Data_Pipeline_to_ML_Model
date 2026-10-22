"""
dags/nammamart_etl_mohanrajs.py
---------------------------
NammaMart ETL pipeline: bad rows are stopped BEFORE the warehouse.

  extract_orders    ─┐                                   ┌─► validate_orders ──┐
  extract_payments  ─┤   validate_customers ─┐           │                     │
  extract_customers ─┼─► validate_products  ─┼───────────┤                     ├─► transform ─► load ─► reconcile ─► report ─► run_audit
  extract_products  ─┤   validate_stores    ─┘           └─► validate_payments ┘                                              (all_done)
  extract_stores    ─┘

* Quarantine : include/quarantine/mohanrajs/<file>_failed_<run_id>.csv  (+ etl_quarantine_log table)
* Warehouse  : include/warehouse_mohanrajs.duckdb  (etl_fact_orders, etl_dim_*, etl_payment_reconciliation, dq_results, pipeline_runs)
* Schedule   : 09:00 IST, Monday-Saturday.
* Param load_increment=True loads orders_2026-10-01_increment.csv and UPSERTS it into the fact table.
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

sys.path.insert(0, "/usr/local/airflow/include")  # Astro container path; harmless elsewhere
import nammamart_common as nc  # noqa: E402
import nammamart_etl_lib as lib  # noqa: E402
import nammamart_quality as dq  # noqa: E402

DAG_ID = "nammamart_etl_mohanrajs"

def _read_source(name):
    """A missing file may just be late, so it is retried. A renamed/missing COLUMN will not fix itself: fail at once."""
    try:
        return nc.read_csv_raw(name)
    except ValueError as e:
        raise AirflowFailException(str(e)) from e


default_args = {
    "owner": "mohanrajs",
    "retries": 2,                              # a transient failure (locked file, slow disk) should not wake anyone
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=10),  # a hung task must fail before the 9:30 stand-up
    "on_failure_callback": nc.slack_alert,
}


def _work(run_id):
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in run_id)
    d = nc.WORK_DIR / safe
    d.mkdir(parents=True, exist_ok=True)
    return d


def _as_of():
    return nc.resolve_as_of(Variable.get("nammamart_as_of", default=None))


def _read(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _mode():
    return "incremental" if get_current_context()["params"].get("load_increment") else "full"


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                   # 09:00, Mon-Sat
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),  # explicit IST, otherwise Airflow assumes UTC
    catchup=False,                                            # report is "as of today"; replaying 30 old mornings is noise
    max_active_runs=1,                                        # DuckDB allows one writer
    default_args=default_args,
    params={"load_increment": Param(False, type="boolean",
                                    description="Load orders_2026-10-01_increment.csv (upsert) instead of orders.csv")},
    tags=["fde", "nammamart", "etl"],
)
def nammamart_etl_mohanrajs():

    # ───────────────────────── EXTRACT (5 parallel tasks) ─────────────────────────
    def _extract(name, source=None):
        ctx = get_current_context()
        run_id = ctx["run_id"]
        df = _read_source(source or name)
        path = _work(run_id) / f"{name}_raw.csv"
        df.to_csv(path, index=False)
        print(f"[Extract] {name}: {len(df)} rows from {nc.file_path(source or name).name}")
        return {"name": name, "rows": len(df), "path": str(path)}

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

    # ───────────────────────── VALIDATE + QUARANTINE ─────────────────────────
    def _finish(name, ex, passed, failed, results, key_col):
        run_id = get_current_context()["run_id"]
        w = _work(run_id)
        qpath, qlog = lib.write_quarantine(failed, name, run_id, key_col)
        passed.to_csv(w / f"{name}_passed.csv", index=False)
        qlog.to_csv(w / f"{name}_qlog.csv", index=False)
        print(f"[Validate] {name}: in={ex['rows']} passed={len(passed)} quarantined={len(failed)} -> {qpath}")
        if len(failed):
            print(failed.failure_reason.str.split("; ").explode().value_counts().to_string())
        return {"name": name, "rows_in": ex["rows"], "rows_passed": len(passed), "rows_failed": len(failed),
                "passed_path": str(w / f"{name}_passed.csv"), "qlog_path": str(w / f"{name}_qlog.csv"),
                "quarantine_path": qpath, "results": results}

    @task
    def validate_customers(ex):
        p, f, r = dq.validate_customers(_read(ex["path"]), _as_of())
        return _finish("customers", ex, p, f, r, "customer_id")

    @task
    def validate_products(ex):
        p, f, r = dq.validate_products(_read(ex["path"]), _as_of())
        return _finish("products", ex, p, f, r, "product_id")

    @task
    def validate_stores(ex):
        p, f, r = dq.validate_stores(_read(ex["path"]), _as_of())
        return _finish("stores", ex, p, f, r, "store_id")

    @task
    def validate_orders(ex, cust, prod, store):
        good = lambda v, col: set(_read(v["passed_path"])[col])  # noqa: E731
        products = _read(prod["passed_path"])
        mrp = dict(zip(products.product_id, pd.to_numeric(products.mrp)))
        p, f, r = dq.validate_orders(_read(ex["path"]), _as_of(), good(cust, "customer_id"),
                                     good(prod, "product_id"), good(store, "store_id"), mrp)
        return _finish("orders", ex, p, f, r, "order_id")

    @task
    def validate_payments(ex):
        ids = set(_read_source("orders").order_id.str.strip())
        if nc.file_path("orders_increment").exists():
            ids |= set(_read_source("orders_increment").order_id.str.strip())
        p, f, r = dq.validate_payments(_read(ex["path"]), _as_of(), ids)
        res = _finish("payments", ex, p, f, r, "payment_id")
        f[f.failure_reason.str.contains("payments_order_exists")].to_csv(
            _work(get_current_context()["run_id"]) / "payments_orphans.csv", index=False)
        return res

    # ───────────────────────── TRANSFORM ─────────────────────────
    @task
    def transform(orders_v, cust_v, prod_v, store_v):
        w = _work(get_current_context()["run_id"])
        dc, dp, ds = lib.transform_dims(_read(cust_v["passed_path"]), _read(prod_v["passed_path"]),
                                        _read(store_v["passed_path"]))
        fact = lib.transform_orders(_read(orders_v["passed_path"]), dp, ds)
        dc.to_csv(w / "dim_customer.csv", index=False)
        dp.to_csv(w / "dim_product.csv", index=False)
        ds.to_csv(w / "dim_store.csv", index=False)
        fact.to_csv(w / "fact_orders.csv", index=False)
        print(f"[Transform] fact={len(fact)} dim_customer={len(dc)} dim_product={len(dp)} dim_store={len(ds)}")
        print(f"[Transform] sample masked customer: {dc.iloc[0][['email_masked', 'phone_masked']].to_dict()}")
        return {"fact_rows": len(fact), "dim_rows": len(dc) + len(dp) + len(ds)}

    # ───────────────────────── LOAD ─────────────────────────
    @task
    def load(tf, validations):
        ctx = get_current_context()
        run_id, w, mode = ctx["run_id"], _work(ctx["run_id"]), _mode()
        dc, dp, ds = (pd.read_csv(w / f"dim_{n}.csv", dtype={"pincode": str, "phone_masked": str})
                      for n in ("customer", "product", "store"))
        fact = pd.read_csv(w / "fact_orders.csv", parse_dates=["order_ts"], dtype={"delivery_minutes": float})
        fact["order_date"] = pd.to_datetime(fact.order_date).dt.date
        fact["within_15_min"] = fact.within_15_min.astype(bool)
        results = [r for v in validations for r in v["results"]]
        con = nc.connect()
        try:
            lib.load_dims(con, dc, dp, ds)
            counts = lib.load_fact(con, fact, run_id, mode)
            lib.load_quarantine_log(con, [_read(v["qlog_path"]).assign(source_row=lambda d: pd.to_numeric(d.source_row))
                                          for v in validations])
            orders_in = next(v for v in validations if v["name"] == "orders")["rows_in"]
            if mode == "full":
                results.append(nc.volume_check(orders_in, nc.previous_orders_volume(con, DAG_ID, run_id), DAG_ID))
            nc.write_dq_results(con, run_id, DAG_ID, results)
        finally:
            con.close()
        print(f"[Load] mode={mode} fact inserted={counts['inserted']} updated={counts['updated']} "
              f"total={counts['total']}")
        warned = [r for r in results if r["rows_failed"] and r["severity"] == "Warning"]
        for r in warned:
            print(f"[DQ warning] {r['check_name']}: {r['rows_failed']} of {r['rows_checked']}")
        return {**counts, "rows_loaded": counts["inserted"] + counts["updated"] + tf["dim_rows"]}

    # ───────────────────────── RECONCILE ─────────────────────────
    @task
    def reconcile(ld, payments_v):
        w = _work(get_current_context()["run_id"])
        orphans = _read(w / "payments_orphans.csv")
        con = nc.connect()
        try:
            n = lib.reconcile_payments(con, _read(payments_v["passed_path"]), orphans, get_current_context()["run_id"])
            print(con.execute("SELECT mismatch_reason, count(*) AS rows, round(sum(difference), 2) AS total_difference "
                              "FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC").fetchdf().to_string())
        finally:
            con.close()
        return {"mismatches": n}

    # ───────────────────────── REPORT ─────────────────────────
    @task
    def report(rec):
        con = nc.connect()
        try:
            out = lib.build_reports(con)
        finally:
            con.close()
        print("[Report] etl_daily_zone_revenue (first 20 rows)")
        print(out["etl_daily_zone_revenue"].head(20).to_string(index=False))
        print("[Report] etl_category_margin")
        print(out["etl_category_margin"].to_string(index=False))
        return {"ok": True}

    # ───────────────────────── RUN AUDIT (always runs) ─────────────────────────
    @task(trigger_rule="all_done", retries=0)  # the audit must report a failure, not retry it
    def run_audit():
        ctx = get_current_context()
        ti = ctx["ti"]
        pull = lambda t: ti.xcom_pull(task_ids=t)  # noqa: E731  (None when the task never produced output)
        extracts = [pull(t) for t in ("extract_orders", "extract_payments", "extract_customers",
                                      "extract_products", "extract_stores")]
        vals = [pull(t) for t in ("validate_orders", "validate_payments", "validate_customers",
                                  "validate_products", "validate_stores")]
        ld, rep = pull("load"), pull("report")
        dag_run = ctx["dag_run"]
        incremental = bool(ctx["params"].get("load_increment"))
        row = {
            "run_id": ctx["run_id"], "dag_id": DAG_ID,
            "start_time": pd.Timestamp(dag_run.start_date).tz_convert("Asia/Kolkata").tz_localize(None),
            "end_time": pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None),
            # incremental runs only read the increment file; the other extracts are re-read just to refresh the dimensions
            "rows_extracted": sum(e["rows"] for e in extracts if e and (not incremental or e["name"] == "orders")),
            "rows_loaded": ld["rows_loaded"] if ld else 0,
            "rows_quarantined": sum(v["rows_failed"] for v in vals if v),
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
    eo, ep, ec, epr, es = (extract_orders(), extract_payments(), extract_customers(), extract_products(),
                           extract_stores())
    vc, vpr, vs = validate_customers(ec), validate_products(epr), validate_stores(es)
    vo = validate_orders(eo, vc, vpr, vs)
    vp = validate_payments(ep)
    tf = transform(vo, vc, vpr, vs)
    ld = load(tf, [vo, vp, vc, vpr, vs])
    rec = reconcile(ld, vp)
    rep = report(rec)
    audit = run_audit()
    for t in (eo, ep, ec, epr, es, vc, vpr, vs, vo, vp, tf, ld, rec, rep):
        t >> audit


nammamart_etl_mohanrajs()
