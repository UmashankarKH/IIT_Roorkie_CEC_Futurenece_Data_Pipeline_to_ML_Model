"""
DuckDB warehouse helpers shared by both DAGs: connection, dq_results and
pipeline_runs (run audit).

DuckDB allows one writer process per file. Both DAGs are scheduled at 9:00 AM,
so connect() retries for a while when the file is locked by the other DAG.
"""

import time
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

from nammamart_divya.config import CHECKS, WAREHOUSE_DB

AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    source_file   VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,     -- PASS / FAIL (Critical) / WARN (Warning)
    checked_at    TIMESTAMP
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    rows_extracted    BIGINT,
    rows_loaded       BIGINT,
    rows_quarantined  BIGINT,
    status            VARCHAR,
    failed_tasks      VARCHAR
);
"""


@contextmanager
def connect(read_only=False, attempts=60, wait_seconds=5):
    last_error = None
    for _ in range(attempts):
        try:
            con = duckdb.connect(str(WAREHOUSE_DB), read_only=read_only)
            break
        except duckdb.IOException as exc:      # file locked by the other DAG
            last_error = exc
            print(f"[warehouse] DuckDB file busy, retrying in {wait_seconds}s: {exc}")
            time.sleep(wait_seconds)
    else:
        raise last_error
    try:
        if not read_only:
            con.execute(AUDIT_DDL)
        yield con
    finally:
        con.close()


def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def write_dq_results(con, run_id, dag_id, counts, source_file=None):
    """
    counts: {check_name: (rows_checked, rows_failed)}. source_file defaults to the
    check's file (e.g. orders.csv); pass the increment file name for the next-day feed.
    Re-running the same run_id replaces its rows, so the scorecard never double-counts.
    """
    checked_at = now_utc()
    rows = []
    for name, (checked, failed) in counts.items():
        file_key, dimension, severity, _ = CHECKS[name]
        if failed == 0:
            status = "PASS"
        else:
            status = "FAIL" if severity == "Critical" else "WARN"
        rows.append((run_id, dag_id, name, source_file or f"{file_key}.csv", dimension, severity,
                     int(checked), int(failed), status, checked_at))
    for row in rows:
        con.execute(
            "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND check_name = ? AND source_file = ?",
            [row[0], row[1], row[2], row[3]],
        )
    con.executemany("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    return rows


def previous_order_volume(con, dag_id, run_id):
    """Orders extracted by the most recent earlier run of this DAG (None if first run)."""
    row = con.execute(
        """
        SELECT rows_checked FROM dq_results
        WHERE dag_id = ? AND run_id <> ? AND check_name = 'ord_order_id_unique'
          AND source_file = 'orders.csv'
        ORDER BY checked_at DESC LIMIT 1
        """,
        [dag_id, run_id],
    ).fetchone()
    return row[0] if row else None


def write_pipeline_run(con, row):
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
                [row["run_id"], row["dag_id"]])
    con.execute(
        "INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?)",
        [row["run_id"], row["dag_id"], row["start_time"], row["end_time"],
         row["rows_extracted"], row["rows_loaded"], row["rows_quarantined"],
         row["status"], row["failed_tasks"]],
    )
