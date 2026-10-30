"""Shared run-audit writer for the NammaMart Airflow pipelines."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb


def record_pipeline_run(
    *,
    warehouse_path: str | Path,
    dag_id: str,
    run_id: str,
    start_time: datetime | None,
    end_time: datetime | None,
    rows_extracted: int,
    rows_loaded: int,
    rows_quarantined: int,
    status: str,
) -> dict[str, Any]:
    """Idempotently write the audit row for one Airflow DAG run."""
    connection = duckdb.connect(str(warehouse_path))
    try:
        connection.execute("""
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
        """)
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
            [run_id, dag_id],
        )
        connection.execute(
            """INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [run_id, dag_id, start_time, end_time, rows_extracted,
             rows_loaded, rows_quarantined, status],
        )
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()
    result = {
        "run_id": run_id,
        "dag_id": dag_id,
        "start_time": start_time,
        "end_time": end_time,
        "rows_extracted": int(rows_extracted),
        "rows_loaded": int(rows_loaded),
        "rows_quarantined": int(rows_quarantined),
        "status": status,
    }
    print(f"[Run audit] {result}")
    return result
