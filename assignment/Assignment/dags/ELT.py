
"""
NammaMart ELT DAG:
Raw ingestion -> SQL staging -> Incremental merge -> PII masking
-> Data marts -> Audit
"""

import json
import re
from pathlib import Path
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
import yaml

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------
# CONFIGURATION — shared with ETL.py
# ---------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "global_config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file) or {}


def resolve_path(value):
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


DATA_DIR = resolve_path(config.get("DATA_DIR", "include/data"))

# Keep ELT isolated from the ETL warehouse.
DB = resolve_path(
    config.get("WAREHOUSE_DB_ELT", "include/warehouse/warehouse_elt.duckdb")
)

QUARANTINE_DIR = resolve_path(
    config.get("QUARANTINE_DIR", "include/quarantine")
)

INCLUDE = ROOT / "include"
SQL_DIR = ROOT / "include" / "scripts" / "elt"
LOG_DIR = INCLUDE / "logs"

RETRIES = int(config.get("RETRIES", 2))
RETRY_DELAY = int(config.get("RETRY_DELAY", 5))
SCHEDULE = config.get("SCHEDULE", "0 9 * * 1-6")
EXECUTION_TIMEOUT = int(config.get("EXECUTION_TIMEOUT", 30))
TIMEZONE = config.get("TIMEZONE", "Asia/Kolkata")
START_DATE = config.get("START_DATE", "2026-01-01")
CATCHUP = config.get("catchup", False)

TAGS = ["assignment", "nammamart", "ELT"]

SOURCES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

INCREMENT = "orders_2026-10-01_increment.csv"

STAGING_SQL_FILES = [
    "stg_orders.sql",
    "stg_payments.sql",
    "stg_customers.sql",
    "stg_products.sql",
    "stg_stores.sql",
]


# ---------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------

def execution_paths():
    context = get_current_context()
    run_id = context["run_id"]

    safe_run_id = re.sub(r"[^a-zA-Z0-9_.-]+", "_", run_id)
    run_dir = LOG_DIR / safe_run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    return run_id, run_dir, run_dir / "pipeline.log"


def log_phase(phase, message, level="INFO"):
    run_id, _, log_path = execution_paths()

    record = {
        "timestamp": pendulum.now(TIMEZONE).to_iso8601_string(),
        "run_id": run_id,
        "pipeline": "nammamart_elt",
        "phase": phase,
        "level": level,
        "message": message,
    }

    with open(log_path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"[{phase}] {message}")


def read_sql(name, **values):
    """Read SQL from include/elt."""
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    return sql.format(**values) if values else sql


def clean_columns(df):
    """Normalize column names without changing the source values."""
    df = df.copy()
    df.columns = [
        re.sub(
            r"_+",
            "_",
            re.sub(r"[^a-zA-Z0-9]+", "_", str(column).strip().lower()),
        ).strip("_")
        for column in df.columns
    ]
    return df


# ---------------------------------------------------------------
# DAG
# ---------------------------------------------------------------

@dag(
    dag_id="nammamart_elt",
    start_date=pendulum.parse(str(START_DATE), tz=TIMEZONE),
    schedule=SCHEDULE,
    catchup=CATCHUP,
    default_args={
        "owner": "nammamart",
        "retries": RETRIES,
        "retry_delay": timedelta(minutes=RETRY_DELAY),
        "execution_timeout": timedelta(minutes=EXECUTION_TIMEOUT),
    },
    tags=TAGS,
    doc_md=__doc__,
)
def nammamart_elt():

    # -----------------------------------------------------------
    # RAW INGESTION
    # -----------------------------------------------------------

    def raw_task(table, filename):
        @task(task_id=f"load_raw_{table}")
        def _load():
            con = None

            try:
                source_path = DATA_DIR / filename

                if not source_path.is_file():
                    raise FileNotFoundError(
                        f"Source file not found: {source_path}"
                    )

                df = clean_columns(
                    pd.read_csv(
                        source_path,
                        dtype="string",
                        keep_default_na=False,
                    )
                )

                run_id, _, _ = execution_paths()

                df["_loaded_at"] = pendulum.now(
                    TIMEZONE
                ).to_iso8601_string()
                df["_source_file"] = source_path.name
                df["_pipeline_run_id"] = run_id

                DB.parent.mkdir(parents=True, exist_ok=True)

                con = duckdb.connect(str(DB))
                con.register("src", df)

                con.execute(read_sql("create_raw_table.sql", table=table))

                con.unregister("src")
                con.close()
                con = None

                log_phase(
                    "RAW_INGESTION",
                    f"raw_{table}: loaded {len(df)} rows from {filename}",
                )

                return {
                    "table": table,
                    "rows": len(df),
                    "pipeline_run_id": run_id,
                }

            except Exception as exc:
                if con is not None:
                    con.close()

                log_phase(
                    "RAW_INGESTION",
                    f"{table}: {exc}",
                    "ERROR",
                )
                raise

        return _load

    # -----------------------------------------------------------
    # INCREMENTAL FILE INGESTION
    # -----------------------------------------------------------

    @task
    def load_increment():
        con = None

        try:
            source_path = DATA_DIR / INCREMENT

            if not source_path.is_file():
                log_phase(
                    "INCREMENT",
                    f"{INCREMENT} not found; incremental merge skipped",
                )
                return {"present": False, "rows": 0}

            df = clean_columns(
                pd.read_csv(
                    source_path,
                    dtype="string",
                    keep_default_na=False,
                )
            )

            run_id, _, _ = execution_paths()

            df["_loaded_at"] = pendulum.now(
                TIMEZONE
            ).to_iso8601_string()
            df["_source_file"] = source_path.name
            df["_pipeline_run_id"] = run_id

            con = duckdb.connect(str(DB))
            con.register("inc", df)
            con.execute(read_sql("create_increment_table.sql"))

            con.unregister("inc")
            con.close()
            con = None

            log_phase(
                "INCREMENT",
                f"Loaded {len(df)} rows from {INCREMENT}",
            )

            return {"present": True, "rows": len(df)}

        except Exception as exc:
            if con is not None:
                con.close()

            log_phase("INCREMENT", str(exc), "ERROR")
            raise

    # -----------------------------------------------------------
    # PROFILE RAW TABLES
    # -----------------------------------------------------------

    @task
    def profile_raw(raw_meta, increment_meta):
        con = None

        try:
            con = duckdb.connect(str(DB))
            con.execute(read_sql("create_dq_results.sql"))

            run_id, _, _ = execution_paths()

            for table in SOURCES:
                row_count = con.execute(
                    read_sql(
                        "count_table_rows.sql",
                        table=f"raw_{table}",
                    )
                ).fetchone()[0]

                con.execute(
                    read_sql("insert_dq_result.sql"),
                    [
                        run_id,
                        "nammamart_elt",
                        f"raw_{table}_volume",
                        "volume",
                        "warning",
                        row_count,
                        0,
                        "PASS",
                    ],
                )

                log_phase(
                    "RAW_PROFILE",
                    f"raw_{table}: {row_count} rows",
                )

            con.close()
            con = None

            return {"profiled_tables": len(SOURCES)}

        except Exception as exc:
            if con is not None:
                con.close()

            log_phase("RAW_PROFILE", str(exc), "ERROR")
            raise

    # -----------------------------------------------------------
    # CHECK SQL FILES
    # -----------------------------------------------------------

    @task
    def create_staging_sql():
        try:
            missing = [
                name
                for name in STAGING_SQL_FILES
                if not (SQL_DIR / name).is_file()
            ]

            if missing:
                raise FileNotFoundError(
                    f"Missing ELT SQL files in {SQL_DIR}: {missing}"
                )

            log_phase(
                "STAGING",
                f"Verified {len(STAGING_SQL_FILES)} staging SQL files",
            )

            return str(SQL_DIR)

        except Exception as exc:
            log_phase("STAGING", str(exc), "ERROR")
            raise

    # -----------------------------------------------------------
    # RUN STAGING SQL
    # -----------------------------------------------------------

    @task
    def run_staging_sql(sql_dir):
        con = None

        try:
            con = duckdb.connect(str(DB))

            for filename in STAGING_SQL_FILES:
                sql_path = Path(sql_dir) / filename
                con.execute(sql_path.read_text(encoding="utf-8"))

                log_phase(
                    "STAGING",
                    f"Executed {filename}",
                )

            con.close()
            con = None

            return {"staging": "done"}

        except Exception as exc:
            if con is not None:
                con.close()

            log_phase("STAGING", str(exc), "ERROR")
            raise

    # -----------------------------------------------------------
    # MERGE INCREMENT
    # -----------------------------------------------------------

    @task
    def merge_increment(increment_meta):
        con = None

        try:
            con = duckdb.connect(str(DB))

            if increment_meta.get("present"):
                for filename in [
                    "increment_typed.sql",
                    "increment_ranked.sql",
                    "merge_increment.sql",
                    "replace_stg_orders.sql",
                ]:
                    con.execute(read_sql(filename))
                    log_phase(
                        "INCREMENTAL_MERGE",
                        f"Executed {filename}",
                    )

                message = (
                    f"Merged incremental file: "
                    f"{increment_meta.get('rows', 0)} rows"
                )
            else:
                message = "No increment file; merge skipped"

            con.close()
            con = None

            log_phase("INCREMENTAL_MERGE", message)

            return {
                "merge_done": True,
                "increment_rows": increment_meta.get("rows", 0),
            }

        except Exception as exc:
            if con is not None:
                con.close()

            log_phase("INCREMENTAL_MERGE", str(exc), "ERROR")
            raise

    # -----------------------------------------------------------
    # MASK CUSTOMER PII
    # -----------------------------------------------------------

    @task
    def mask_customer_pii():
        con = None

        try:
            con = duckdb.connect(str(DB))
            con.execute(read_sql("mask_customer_pii.sql"))
            con.close()
            con = None

            log_phase(
                "PII_MASKING",
                "Customer PII masking SQL executed",
            )

            return {"pii_masking": "done"}

        except Exception as exc:
            if con is not None:
                con.close()

            log_phase("PII_MASKING", str(exc), "ERROR")
            raise

    # -----------------------------------------------------------
    # BUILD DATA MARTS AND QUALITY RESULTS
    # -----------------------------------------------------------

    @task
    def build_marts():
        con = None

        try:
            con = duckdb.connect(str(DB))

            mart_files = [
                "mart_daily_store_kpis.sql",
                "mart_category_margin.sql",
                "mart_delivery_sla.sql",
                "mart_payment_reconciliation.sql",
                "mart_top_customers.sql",
                "mart_revenue_by_payment_mode.sql",
                "mart_rejected_rows.sql",
            ]

            for filename in mart_files:
                con.execute(read_sql(filename))
                log_phase("DATA_MARTS", f"Executed {filename}")

            con.execute(read_sql("create_dq_results.sql"))

            run_id, _, _ = execution_paths()

            for table in SOURCES:
                total_rows, bad_rows = con.execute(
                    read_sql(
                        "count_staging_quality.sql",
                        table=f"stg_{table}",
                    )
                ).fetchone()

                status = "PASS" if bad_rows == 0 else "FAIL"

                con.execute(
                    read_sql("insert_dq_result.sql"),
                    [
                        run_id,
                        "nammamart_elt",
                        f"{table}_staging_quality",
                        "validity",
                        "critical",
                        total_rows,
                        bad_rows,
                        status,
                    ],
                )

                log_phase(
                    "DATA_QUALITY",
                    f"{table}: checked={total_rows}, "
                    f"failed={bad_rows}, status={status}",
                    "INFO" if bad_rows == 0 else "WARNING",
                )

            con.close()
            con = None

            log_phase("DATA_MARTS", "All data marts built successfully")

            return {"marts": "done"}

        except Exception as exc:
            if con is not None:
                con.close()

            log_phase("DATA_MARTS", str(exc), "ERROR")
            raise

    # -----------------------------------------------------------
    # FINAL AUDIT
    # -----------------------------------------------------------

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def final_run_audit(raw_meta, increment_meta):
        con = None

        try:
            run_id, _, _ = execution_paths()

            total_rows = sum(
                item.get("rows", 0)
                for item in raw_meta
                if isinstance(item, dict)
            )
            total_rows += increment_meta.get("rows", 0)

            con = duckdb.connect(str(DB))
            con.execute(read_sql("create_pipeline_runs.sql"))

            con.execute(
                read_sql("insert_pipeline_run.sql"),
                [
                    run_id,
                    "nammamart_elt",
                    total_rows,
                    total_rows,
                    0,
                    "completed_with_audit",
                ],
            )

            con.close()
            con = None

            log_phase(
                "AUDIT",
                f"Audit record saved; source rows={total_rows}",
            )

        except Exception as exc:
            if con is not None:
                con.close()

            log_phase("AUDIT", str(exc), "ERROR")
            raise

    # -----------------------------------------------------------
    # DAG WIRING
    # -----------------------------------------------------------

    raw = [
        raw_task(table, filename)()
        for table, filename in SOURCES.items()
    ]

    increment = load_increment()

    # Serialize DuckDB writes to avoid concurrent file-lock conflicts.
    for previous, current in zip(raw, raw[1:]):
        previous >> current

    raw[-1] >> increment

    profiled = profile_raw(raw, increment)

    sql_dir = create_staging_sql()
    staged = run_staging_sql(sql_dir)

    merged = merge_increment(increment)
    masked = mask_customer_pii()
    marts = build_marts()

    audit = final_run_audit(raw, increment)

    profiled >> staged
    staged >> merged
    increment >> merged
    merged >> masked >> marts >> audit

    # Ensure audit waits for every raw ingestion task.
    for raw_task_instance in raw:
        raw_task_instance >> audit


nammamart_elt()
