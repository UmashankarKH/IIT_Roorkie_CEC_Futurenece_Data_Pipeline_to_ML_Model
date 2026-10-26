"""Run audit and dq_results helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Mapping

import duckdb


def ensure_audit_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR,
            dag_id VARCHAR,
            check_name VARCHAR,
            dimension VARCHAR,
            severity VARCHAR,
            rows_checked INTEGER,
            rows_failed INTEGER,
            status VARCHAR,
            checked_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR,
            dag_id VARCHAR,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            rows_extracted INTEGER,
            rows_loaded INTEGER,
            rows_quarantined INTEGER,
            status VARCHAR
        )
        """
    )


def write_dq_results(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    dag_id: str,
    records: Iterable[Mapping[str, object]],
) -> None:
    ensure_audit_tables(con)
    rows = list(records)
    if not rows:
        return
    con.executemany(
        """
        INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                dag_id,
                r["check_name"],
                r["dimension"],
                r["severity"],
                r["rows_checked"],
                r["rows_failed"],
                r["status"],
                r.get("checked_at") or datetime.utcnow(),
            )
            for r in rows
        ],
    )


def write_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    dag_id: str,
    start_time: datetime,
    end_time: datetime,
    rows_extracted: int,
    rows_loaded: int,
    rows_quarantined: int,
    status: str,
) -> None:
    ensure_audit_tables(con)
    con.execute(
        """
        INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            dag_id,
            start_time,
            end_time,
            rows_extracted,
            rows_loaded,
            rows_quarantined,
            status,
        ],
    )
