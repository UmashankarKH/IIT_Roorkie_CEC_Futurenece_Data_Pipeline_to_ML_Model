"""
include/nammamart_smit/warehouse.py
-----------------------------------
DuckDB helpers + the two tables both DAGs write to:
  dq_results     -> one row per quality check per run (quality scorecard)
  pipeline_runs  -> one row per DAG run (run audit)
"""

import os
import time
from datetime import datetime, timezone

import duckdb
import pandas as pd

from include.nammamart_smit.config import BASE_BUSINESS_DATE, SQL_DIR, WAREHOUSE_DB


def get_connection():
    # DuckDB allows only one writer at a time. Parallel Airflow tasks run in
    # separate processes, so if the file is locked we wait and try again.
    for attempt in range(60):
        try:
            return duckdb.connect(WAREHOUSE_DB)
        except duckdb.IOException:
            print(f"[DuckDB] Warehouse is locked by another task, retry {attempt + 1}/60")
            time.sleep(2)
    raise RuntimeError("Could not open the warehouse: still locked after 2 minutes")


def run_sql_file(con, file_name, run_id):
    """Run one .sql file from include/sql. The SQL reads the run id and business
    date with getvariable('run_id') / getvariable('base_business_date')."""
    with open(os.path.join(SQL_DIR, file_name)) as f:
        sql = f.read()
    con.execute("SET VARIABLE run_id = ?", [run_id])
    con.execute("SET VARIABLE base_business_date = ?", [BASE_BUSINESS_DATE])
    print(f"[SQL] running {file_name}")
    return con.execute(sql)


def create_audit_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id       VARCHAR,
            dag_id       VARCHAR,
            source_file  VARCHAR,
            check_name   VARCHAR,
            dimension    VARCHAR,
            severity     VARCHAR,
            rows_checked INTEGER,
            rows_failed  INTEGER,
            status       VARCHAR,
            checked_at   TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id           VARCHAR,
            dag_id           VARCHAR,
            start_time       TIMESTAMP,
            end_time         TIMESTAMP,
            rows_extracted   INTEGER,
            rows_loaded      INTEGER,
            rows_quarantined INTEGER,
            status           VARCHAR,
            failed_tasks     VARCHAR
        )
    """)


def save_dq_results(con, run_id, dag_id, results):
    """results = list of dicts: source_file, check_name, dimension, severity, rows_checked, rows_failed"""
    create_audit_tables(con)
    if not results:
        return

    df = pd.DataFrame(results)
    df["run_id"] = run_id
    df["dag_id"] = dag_id
    df["status"] = "PASS"
    df.loc[(df["rows_failed"] > 0) & (df["severity"] == "Critical"), "status"] = "FAIL"
    df.loc[(df["rows_failed"] > 0) & (df["severity"] == "Warning"), "status"] = "WARN"
    df["checked_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    df = df[["run_id", "dag_id", "source_file", "check_name", "dimension", "severity",
             "rows_checked", "rows_failed", "status", "checked_at"]]

    # If a task is cleared and re-run, replace its old results instead of adding them twice
    for source_file in df["source_file"].unique():
        con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND source_file = ?",
                    [run_id, dag_id, source_file])
    con.execute("INSERT INTO dq_results SELECT * FROM df")


def last_row_count(con, dag_id, source_file, run_id):
    """How many rows this file had on the previous run (used by the volume check)."""
    create_audit_tables(con)
    row = con.execute("""
        SELECT rows_checked FROM dq_results
        WHERE dag_id = ? AND source_file = ? AND check_name = 'volume_vs_previous_run' AND run_id <> ?
        ORDER BY checked_at DESC
        LIMIT 1
    """, [dag_id, source_file, run_id]).fetchone()
    return row[0] if row else None


def save_pipeline_run(con, run):
    create_audit_tables(con)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run["run_id"], run["dag_id"]])
    con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [
        run["run_id"], run["dag_id"], run["start_time"], run["end_time"],
        run["rows_extracted"], run["rows_loaded"], run["rows_quarantined"],
        run["status"], run["failed_tasks"],
    ])
