"""Run-audit helpers shared by the ETL and ELT implementations."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def record_pipeline_run(
    warehouse_path: str | Path,
    *,
    run_id: str,
    dag_id: str,
    start_time: str,
    end_time: str,
    rows_extracted: int,
    rows_loaded: int,
    rows_quarantined: int,
    status: str,
) -> None:
    """Append one externally visible audit record to ``pipeline_runs``."""

    # The audit table is created independently of the business tables, so a
    # failed pipeline can still leave behind a useful failure record.
    con = duckdb.connect(str(warehouse_path))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR,
                dag_id VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_extracted BIGINT,
                rows_loaded BIGINT,
                rows_quarantined BIGINT,
                status VARCHAR
            )
            """
        )
        audit_row = pd.DataFrame(
            [{
                "run_id": run_id,
                "dag_id": dag_id,
                "start_time": start_time,
                "end_time": end_time,
                "rows_extracted": rows_extracted,
                "rows_loaded": rows_loaded,
                "rows_quarantined": rows_quarantined,
                "status": status,
            }]
        )
        con.register("audit_row", audit_row)
        con.execute("INSERT INTO pipeline_runs SELECT * FROM audit_row")
        con.unregister("audit_row")
    finally:
        con.close()


DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR,
    dag_id VARCHAR,
    check_name VARCHAR,
    dimension VARCHAR,
    severity VARCHAR,
    rows_checked BIGINT,
    rows_failed BIGINT,
    status VARCHAR,
    checked_at VARCHAR
)
"""

VOLUME_CHECK_NAME = "orders.csv:order_id:Order volume dropped more than 90% vs previous run"


def volume_check(
    warehouse_path: str | Path,
    *,
    run_id: str,
    dag_id: str,
    order_rows: int,
    checked_at: str,
) -> pd.DataFrame:
    """Compare this run's order count with the previous run of the same DAG.

    The previous count is read from this check's own history in
    ``dq_results``, so the scorecard is also the volume baseline. The first
    run has no baseline and passes.
    """

    con = duckdb.connect(str(warehouse_path))
    try:
        con.execute(DQ_RESULTS_DDL)
        previous = con.execute(
            """
            SELECT rows_checked FROM dq_results
            WHERE check_name = ? AND dag_id = ? AND run_id <> ?
            ORDER BY checked_at DESC LIMIT 1
            """,
            [VOLUME_CHECK_NAME, dag_id, run_id],
        ).fetchone()
    finally:
        con.close()
    dropped = previous is not None and previous[0] > 0 and order_rows < 0.1 * previous[0]
    if dropped:
        print(f"[Volume] WARNING: {order_rows} orders vs {previous[0]} in the previous run (>90% drop).")
    return pd.DataFrame([{
        "run_id": run_id,
        "dag_id": dag_id,
        "check_name": VOLUME_CHECK_NAME,
        "dimension": "Volume",
        "severity": "Warning",
        "rows_checked": order_rows,
        "rows_failed": int(dropped),
        "status": "FAIL" if dropped else "PASS",
        "checked_at": checked_at,
    }])


def append_dq_results(warehouse_path: str | Path, dq_results: pd.DataFrame) -> None:
    """Append check results so ``dq_results`` keeps one scorecard per run."""

    con = duckdb.connect(str(warehouse_path))
    try:
        con.execute(DQ_RESULTS_DDL)
        # Drop rows from an earlier attempt of the same run so an Airflow
        # retry or Clear does not double-count that run's checks.
        for run_id, dag_id in dq_results[["run_id", "dag_id"]].drop_duplicates().itertuples(index=False):
            con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        con.register("dq_frame", dq_results)
        con.execute(
            "INSERT INTO dq_results SELECT run_id, dag_id, check_name, dimension, severity, "
            "rows_checked, rows_failed, status, CAST(checked_at AS VARCHAR) FROM dq_frame"
        )
        con.unregister("dq_frame")
    finally:
        con.close()
