"""Persistent run and data-quality audit records for NammaMart."""

from pathlib import Path

import duckdb
import pandas as pd

from include.nammamart.dq_results import DQ_COLUMNS

PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUDIT_DB_PATH = (
    PROJECT_ROOT / "include" / "output" / "nammamart_audit.duckdb"
)


def ensure_audit_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create persistent audit tables when they do not exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR NOT NULL,
            dag_id VARCHAR NOT NULL,
            check_name VARCHAR NOT NULL,
            dimension VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,
            rows_checked BIGINT NOT NULL,
            rows_failed BIGINT NOT NULL,
            status VARCHAR NOT NULL,
            checked_at TIMESTAMPTZ NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR NOT NULL,
            dag_id VARCHAR NOT NULL,
            start_time TIMESTAMPTZ,
            end_time TIMESTAMPTZ,
            rows_extracted BIGINT NOT NULL DEFAULT 0,
            rows_loaded BIGINT NOT NULL DEFAULT 0,
            rows_quarantined BIGINT NOT NULL DEFAULT 0,
            rows_deduplicated BIGINT NOT NULL DEFAULT 0,
            rows_superseded BIGINT NOT NULL DEFAULT 0,
            status VARCHAR NOT NULL
        )
    """)


    con.execute("""
        ALTER TABLE pipeline_runs
        ADD COLUMN IF NOT EXISTS rows_deduplicated BIGINT DEFAULT 0
    """)
    con.execute("""
        ALTER TABLE pipeline_runs
        ADD COLUMN IF NOT EXISTS rows_superseded BIGINT DEFAULT 0
    """)


def persist_dq_results(results: list[dict]) -> None:
    """Append quality results to DuckDB and the CSV evidence file."""
    if not results:
        return

    frame = pd.DataFrame(results, columns=DQ_COLUMNS)
    frame["checked_at"] = pd.to_datetime(
        frame["checked_at"], utc=True, errors="raise"
    )

    AUDIT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(AUDIT_DB_PATH)) as con:
        ensure_audit_tables(con)
        con.register("dq_results_batch", frame)

        # Replace this run's previous batch to make retries idempotent.
        run_id = str(frame["run_id"].iloc[0])
        dag_id = str(frame["dag_id"].iloc[0])
        if not (frame["run_id"] == run_id).all():
            raise ValueError("A DQ batch must contain exactly one run_id.")
        if not (frame["dag_id"] == dag_id).all():
            raise ValueError("A DQ batch must contain exactly one dag_id.")

        con.execute(
            "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?",
            [run_id, dag_id],
        )
        con.execute("""
            INSERT INTO dq_results
            SELECT
                run_id, dag_id, check_name, dimension, severity,
                rows_checked, rows_failed, status, checked_at
            FROM dq_results_batch
        """)
        con.unregister("dq_results_batch")


def record_pipeline_run(
    *,
    run_id: str,
    dag_id: str,
    start_time,
    end_time,
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    rows_quarantined: int = 0,
    rows_deduplicated: int = 0,
    rows_superseded: int = 0,
    status: str,
) -> None:
    """Persist one run summary, replacing a prior audit row for the same run."""
    if status not in {"SUCCESS", "FAILED", "PARTIAL"}:
        raise ValueError(f"Unsupported pipeline status: {status}")

    AUDIT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(AUDIT_DB_PATH)) as con:
        ensure_audit_tables(con)
        con.execute(
            "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
            [run_id, dag_id],
        )
        con.execute("""
            INSERT INTO pipeline_runs (
                run_id, dag_id, start_time, end_time,
                rows_extracted, rows_loaded, rows_quarantined,
                rows_deduplicated, rows_superseded, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id, dag_id, start_time, end_time,
            int(rows_extracted), int(rows_loaded),
            int(rows_quarantined), int(rows_deduplicated),
            int(rows_superseded), status,
        ])
