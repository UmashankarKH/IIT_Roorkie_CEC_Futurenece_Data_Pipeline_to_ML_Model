"""NammaMart ELT: Extract -> Load raw -> Volume gate -> Staging -> Increment (MERGE) -> Marts -> DQ -> Audit."""
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

from include.elt_dq import write_elt_dq
from include.elt_incremental import apply_increment
from include.elt_raw import RAW_TABLES, load_raw
from include.elt_sql import run_sql
from include.etl_audit import write_dq_results, write_pipeline_run
from include.etl_steps import read_csv
from include.nammamart_config import ELT_DAG_ID, WAREHOUSE
from include.schema_contract import SchemaError, assert_schema
from include.volume_gate import volume_row

SOURCE_FILES = ["orders", "payments", "customers", "products", "stores"]
STAGING = ["stg_stores", "stg_products", "stg_customers", "stg_orders", "stg_payments"]
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers"]

default_args = {
    "owner": "anikesh",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


def _now():
    return pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)


def _safe(run_id):
    return re.sub(r"[^A-Za-z0-9_-]", "_", run_id or "manual")


@dag(
    dag_id=ELT_DAG_ID,
    schedule="0 9 * * 1-6",                                   # 9:00 AM, Monday to Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),   # IST, not UTC
    catchup=False,
    default_args=default_args,
    tags=["nammamart", "elt"],
)
def nammamart_elt():

    @task
    def extract(name: str) -> dict:
        df = read_csv(f"{name}.csv")          # raises FileNotFoundError if the file is missing
        try:
            assert_schema(name, df.columns)   # schema contract: raw loads by position, so check first
        except SchemaError as e:
            raise AirflowFailException(f"schema drift in {name}.csv: {e}")
        print(f"extracted {name}: {len(df)} rows")
        return {"file": name, "rows": len(df)}

    @task(pool="duckdb_writer")
    def load_raw_table(table: str, csv_name: str, run_id=None) -> int:
        n = load_raw(table, csv_name, _safe(run_id))      # every row, untouched
        print(f"{table}: {n} rows loaded exactly as received")
        return n

    @task(pool="duckdb_writer")
    def volume_gate(run_id=None, ti=None) -> dict:
        rid = _safe(run_id)
        rows = (ti.xcom_pull(task_ids="extract_orders") or {}).get("rows")
        if rows is None:
            raise AirflowFailException("volume gate: no row count from extract_orders")
        vrow, base = volume_row(ELT_DAG_ID, rid, rows, _now())
        write_dq_results([vrow], ELT_DAG_ID, rid)
        if vrow["rows_failed"]:                # raw keeps the file as received; staging and marts stay untouched
            raise AirflowFailException(
                f"volume gate: orders.csv has {rows} rows, recent runs had {base}. Staging and marts not rebuilt.")
        print(f"volume gate passed: orders has {rows} rows, recent max {base}")
        return {"rows": rows, "baseline": base}

    @task(pool="duckdb_writer")
    def run_sql_file(name: str, run_id=None) -> str:
        run_sql(f"{name}.sql", _safe(run_id))
        print(f"ran include/sql/{name}.sql")
        return name

    @task(pool="duckdb_writer")
    def load_increment(run_id=None) -> dict:
        try:
            res = apply_increment(_safe(run_id))
        except SchemaError as e:
            raise AirflowFailException(f"schema drift in the increment file: {e}")
        if res["applied"]:
            print(f"increment: {res['rows']} rows -> raw; MERGE on order_id: "
                  f"{res['updated']} updated, {res['inserted']} inserted")
        else:
            print("increment file not found: skipped")
        return res

    @task(pool="duckdb_writer")
    def elt_dq(run_id=None) -> str:
        results = write_elt_dq(_safe(run_id))
        failed = [r for r in results if r["rows_failed"] > 0]
        print(f"dq_results: {len(results)} checks recorded, {len(failed)} with failures")
        return "done"

    @task(pool="duckdb_writer", trigger_rule="all_done")      # runs even if an upstream task failed
    def run_audit(run_id=None, dag_run=None, ti=None):
        rid = _safe(run_id)
        extracted = sum((ti.xcom_pull(task_ids=f"extract_{n}") or {"rows": 0})["rows"] for n in SOURCE_FILES)
        loaded = sum((ti.xcom_pull(task_ids=f"load_{t}") or 0) for t in RAW_TABLES)
        inc = ti.xcom_pull(task_ids="load_increment") or {"rows": 0}
        extracted += inc["rows"]
        loaded += inc["rows"]
        try:                                   # rejected rows stay in staging, flagged (not removed)
            con = duckdb.connect(str(WAREHOUSE), read_only=True)
            quarantined = sum(con.execute(f"SELECT count(*) FROM {t} WHERE dq_status = 'REJECTED'").fetchone()[0]
                              for t in STAGING)
            con.close()
        except Exception as e:
            print("could not count rejected rows:", e)
            quarantined = 0
        ok = ti.xcom_pull(task_ids="elt_dq") == "done"
        end = _now()
        start = dag_run.start_date if dag_run and dag_run.start_date else None
        start = pd.Timestamp(start).tz_convert("Asia/Kolkata").tz_localize(None) if start else end
        write_pipeline_run(ELT_DAG_ID, rid, start, end, extracted, loaded, quarantined,
                           "SUCCESS" if ok else "FAILED")
        print(f"audit: extracted={extracted} loaded_raw={loaded} rejected_in_staging={quarantined} "
              f"status={'SUCCESS' if ok else 'FAILED'}")
        if not ok:                             # make the DAG run RED, not green, when the pipeline failed
            raise AirflowFailException("pipeline failed: see the failed task above; details in pipeline_runs")

    extracts = [extract.override(task_id=f"extract_{n}")(n) for n in SOURCE_FILES]
    loads = [load_raw_table.override(task_id=f"load_{t}")(t, c) for t, c in RAW_TABLES.items()]
    gate = volume_gate()
    stg = {s: run_sql_file.override(task_id=s)(s) for s in STAGING}
    inc = load_increment()
    marts = [run_sql_file.override(task_id=m)(m) for m in MARTS]
    dq = elt_dq()
    audit = run_audit()

    # warehouse writers run one after another (DuckDB allows one writer at a time)
    sequence = [*loads, gate, stg["stg_stores"], stg["stg_products"], stg["stg_customers"],
                stg["stg_orders"], inc, stg["stg_payments"], *marts, dq]
    extracts >> sequence[0]
    for a, b in zip(sequence, sequence[1:]):
        a >> b
    [*extracts, *sequence] >> audit


nammamart_elt()
