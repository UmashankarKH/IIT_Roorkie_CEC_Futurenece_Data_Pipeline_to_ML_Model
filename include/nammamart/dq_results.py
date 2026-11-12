"""Run-level data-quality results for NammaMart pipelines."""

from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

DQ_COLUMNS = [
    "run_id",
    "dag_id",
    "check_name",
    "dimension",
    "severity",
    "rows_checked",
    "rows_failed",
    "status",
    "checked_at",
]


def new_run_id() -> str:
    """Generate a unique identifier for one pipeline execution."""
    return str(uuid4())


def make_dq_result(
    *,
    run_id: str,
    dag_id: str,
    check_name: str,
    dimension: str,
    severity: str,
    rows_checked: int,
    rows_failed: int,
    checked_at: datetime | None = None,
) -> dict:
    """Build one standardized data-quality result record."""
    if severity not in {"Critical", "Warning"}:
        raise ValueError(f"Unsupported severity: {severity}")

    if dimension not in {
        "Completeness",
        "Uniqueness",
        "Validity",
        "Accuracy",
        "Consistency",
        "Timeliness",
    }:
        raise ValueError(f"Unsupported quality dimension: {dimension}")

    if rows_checked < 0 or rows_failed < 0 or rows_failed > rows_checked:
        raise ValueError("Require 0 <= rows_failed <= rows_checked")

    timestamp = checked_at or datetime.now(ZoneInfo("Asia/Kolkata"))

    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "check_name": check_name,
        "dimension": dimension,
        "severity": severity,
        "rows_checked": rows_checked,
        "rows_failed": rows_failed,
        "status": "PASS" if rows_failed == 0 else "FAIL",
        "checked_at": timestamp.isoformat(),
    }


def dq_results_frame(results: list[dict]) -> pd.DataFrame:
    """Return a DataFrame with the assignment-required schema."""
    return pd.DataFrame(results, columns=DQ_COLUMNS)


def write_dq_results(
    results: list[dict],
    output_path: str,
) -> pd.DataFrame:
    """Persist run-level DQ results as CSV."""
    from pathlib import Path

    frame = dq_results_frame(results)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return frame
