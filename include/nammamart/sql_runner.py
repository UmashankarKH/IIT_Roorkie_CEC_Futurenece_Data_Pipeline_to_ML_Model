"""Utilities for executing NammaMart SQL transformation files."""

from pathlib import Path

import duckdb


SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def execute_sql_file(
    con: duckdb.DuckDBPyConnection,
    filename: str,
) -> None:
    """Execute one SQL file using an existing DuckDB connection."""
    sql_path = SQL_DIR / filename

    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8").strip()

    if not sql:
        raise ValueError(f"SQL file is empty: {sql_path}")

    con.execute(sql)


def execute_sql_files(
    con: duckdb.DuckDBPyConnection,
    filenames: list[str],
) -> None:
    """Execute SQL files sequentially on the same connection."""
    for filename in filenames:
        execute_sql_file(con, filename)
