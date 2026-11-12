"""
include/nammamart/warehouse.py
------------------------------
DuckDB helpers shared by the ETL and the ELT pipeline.

**DuckDB** is an analytical SQL database that runs inside your process and
stores everything in a single file (``include/warehouse_waseem.duckdb``).
Java comparison (learning aid): it is to analytics what H2 or SQLite is to a
small Java application - same SQL, no server to start - but it is column based
and built for aggregation queries.

Key ideas implemented here
--------------------------
* **Idempotent load** - running the same load twice leaves the table exactly as
  it was after the first run.  We achieve this with a *delete-then-insert* on
  the business key, which behaves like an SQL ``MERGE``.
* **Append-only raw** - the ELT raw layer keeps every row ever received,
  tagged with ``_run_id`` and ``_loaded_at`` for audit.
* **Scorecards** - ``dq_results`` (quality) and ``pipeline_runs`` (run audit)
  grow one set of rows per run and are never overwritten.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import duckdb
import pandas as pd

from . import config

logger = logging.getLogger(__name__)


@contextmanager
def connect(db_path: str | Path | None = None, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection and always close it.

    Java comparison (learning aid): the same purpose as try-with-resources
    around a JDBC ``Connection``.
    """
    path = Path(db_path or config.WAREHOUSE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path), read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    found = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()
    return bool(found and found[0])


def row_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    if not table_exists(con, table):
        return 0
    return int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])


def replace_table(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    """Full reload: drop and recreate the table from ``df``.

    Used for the small dimension tables, where a full refresh is cheaper and
    simpler than a merge, and is naturally idempotent.
    """
    con.register("_tmp_replace", df)
    con.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM _tmp_replace')
    con.unregister("_tmp_replace")
    count = row_count(con, table)
    logger.info("[Warehouse] %s reloaded with %s rows", table, count)
    return count


def upsert_table(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame,
                 key_columns: Sequence[str]) -> dict[str, int]:
    """Insert new rows and overwrite existing ones, matched on ``key_columns``.

    This is the **idempotency** guarantee required by the client (acceptance
    criterion 5: "re-running never double-counts revenue") and the mechanism
    used for the incremental load (criterion 6).

    Implemented as delete-then-insert inside one transaction, which gives the
    same result as SQL ``MERGE`` on every DuckDB version.
    """
    con.register("_tmp_upsert", df)
    if not table_exists(con, table):
        con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM _tmp_upsert')
        inserted = row_count(con, table)
        con.unregister("_tmp_upsert")
        logger.info("[Warehouse] %s created with %s rows", table, inserted)
        return {"inserted": inserted, "updated": 0, "total": inserted}

    join_condition = " AND ".join(f't."{col}" = s."{col}"' for col in key_columns)
    matched = int(con.execute(
        f'SELECT count(*) FROM "{table}" t WHERE EXISTS '
        f'(SELECT 1 FROM _tmp_upsert s WHERE {join_condition})'
    ).fetchone()[0])

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f'DELETE FROM "{table}" t WHERE EXISTS '
            f'(SELECT 1 FROM _tmp_upsert s WHERE {join_condition})'
        )
        con.execute(f'INSERT INTO "{table}" SELECT * FROM _tmp_upsert')
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.unregister("_tmp_upsert")

    total = row_count(con, table)
    inserted = len(df) - matched
    logger.info("[Warehouse] %s upsert: %s updated, %s inserted, %s total",
                table, matched, inserted, total)
    return {"inserted": inserted, "updated": matched, "total": total}


def append_table(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    """Append rows without removing anything (raw layer and audit tables)."""
    con.register("_tmp_append", df)
    if table_exists(con, table):
        con.execute(f'INSERT INTO "{table}" BY NAME SELECT * FROM _tmp_append')
    else:
        con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM _tmp_append')
    con.unregister("_tmp_append")
    return row_count(con, table)


# ---------------------------------------------------------------------------
# Quality scorecard and run audit
# ---------------------------------------------------------------------------
DQ_RESULTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.DQ_RESULTS_TABLE} (
    run_id       VARCHAR,
    dag_id       VARCHAR,
    rule_id      VARCHAR,
    dataset      VARCHAR,
    check_name   VARCHAR,
    dimension    VARCHAR,
    severity     VARCHAR,
    rows_checked BIGINT,
    rows_failed  BIGINT,
    status       VARCHAR,
    detail       VARCHAR,
    checked_at   TIMESTAMP
)
"""

PIPELINE_RUNS_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.PIPELINE_RUNS_TABLE} (
    run_id           VARCHAR,
    dag_id           VARCHAR,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    duration_seconds DOUBLE,
    rows_extracted   BIGINT,
    rows_loaded      BIGINT,
    rows_quarantined BIGINT,
    status           VARCHAR,
    notes            VARCHAR
)
"""


def ensure_audit_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DQ_RESULTS_DDL)
    con.execute(PIPELINE_RUNS_DDL)


def write_dq_results(con: duckdb.DuckDBPyConnection, rows: Iterable[dict]) -> int:
    """Append one row per quality check executed in this run."""
    frame = pd.DataFrame(list(rows))
    if frame.empty:
        logger.warning("[DQ] no quality results to write")
        return 0
    ensure_audit_tables(con)
    frame["checked_at"] = pd.to_datetime(frame["checked_at"])
    if "detail" not in frame.columns:
        frame["detail"] = ""
    columns = ["run_id", "dag_id", "rule_id", "dataset", "check_name", "dimension",
               "severity", "rows_checked", "rows_failed", "status", "detail", "checked_at"]
    con.register("_tmp_dq", frame[columns])
    con.execute(f"INSERT INTO {config.DQ_RESULTS_TABLE} SELECT * FROM _tmp_dq")
    con.unregister("_tmp_dq")
    logger.info("[DQ] %s check results written to %s", len(frame), config.DQ_RESULTS_TABLE)
    return len(frame)


def write_pipeline_run(con: duckdb.DuckDBPyConnection, *, run_id: str, dag_id: str,
                       start_time: datetime, end_time: datetime, rows_extracted: int,
                       rows_loaded: int, rows_quarantined: int, status: str,
                       notes: str = "") -> None:
    """Write the run-audit row required by Part F (one row per DAG run)."""
    ensure_audit_tables(con)
    duration = (end_time - start_time).total_seconds()
    con.execute(
        f"INSERT INTO {config.PIPELINE_RUNS_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [run_id, dag_id, start_time, end_time, duration, int(rows_extracted),
         int(rows_loaded), int(rows_quarantined), status, notes],
    )
    logger.info("[Audit] run %s (%s) recorded as %s in %.1fs", run_id, dag_id, status, duration)


# ---------------------------------------------------------------------------
# SQL file execution (used by the ELT pipeline - Part B)
# ---------------------------------------------------------------------------
def read_sql(relative_path: str) -> str:
    """Read a ``.sql`` file from ``include/sql/``."""
    path = config.SQL_DIR / relative_path
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def run_sql_file(con: duckdb.DuckDBPyConnection, relative_path: str,
                 parameters: dict | None = None) -> None:
    """Execute a ``.sql`` file, after substituting ``{placeholders}``.

    Only simple, pipeline-controlled values are substituted (file paths,
    ``run_id``, thresholds).  No user input reaches the SQL text.
    """
    statement = read_sql(relative_path)
    if parameters:
        statement = statement.format(**parameters)
    logger.info("[SQL] executing %s", relative_path)
    con.execute(statement)


def query(con: duckdb.DuckDBPyConnection, sql: str, parameters: list | None = None) -> pd.DataFrame:
    """Run a SELECT and return the result as a pandas DataFrame."""
    return con.execute(sql, parameters or []).fetch_df()
