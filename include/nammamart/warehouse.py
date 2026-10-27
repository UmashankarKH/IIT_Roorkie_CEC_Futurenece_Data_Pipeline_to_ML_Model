"""DuckDB helpers shared by both pipelines: idempotent loads, dq_results, run audit."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import duckdb
import pandas as pd

from . import config as cfg
from . import transforms


def one(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None):
    """Run a query and return its first row, draining the result.

    duckdb 2.0 refuses any further work on a connection that still has an
    unfinished result, and fetchone() on a one-row result leaves it unfinished.
    Everything here used .fetchone() and it worked fine locally on duckdb 1.4.5 -
    it only broke inside Airflow, where the image had duckdb 2.0 because
    requirements.txt did not pin a version. fetchall() drains the result properly.
    """
    rows = con.execute(sql, params or []).fetchall()
    return rows[0] if rows else None


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None):
    row = one(con, sql, params)
    return row[0] if row else None


@contextmanager
def connect(path: Path | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    target = Path(path or cfg.WAREHOUSE)
    target.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(target))
    try:
        yield con
    finally:
        con.close()


def replace_table(con: duckdb.DuckDBPyConnection, name: str, df: pd.DataFrame) -> int:
    """Full replace. Idempotent by construction: re-running cannot double-count.

    The September exports are a complete snapshot, so a replace is the honest
    load pattern here. The increment file is handled by merge_orders() instead.
    """
    con.register("_incoming", df)
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _incoming")
    con.unregister("_incoming")
    return int(scalar(con, f"SELECT count(*) FROM {name}"))


def load_csv(con: duckdb.DuckDBPyConnection, name: str, path: str) -> int:
    """Load a stashed CSV straight into DuckDB and let DuckDB do the typing.

    The DAG tasks hand each other file paths, because Airflow 3 serialises XCom as
    JSON and a DataFrame cannot travel that way. Reading those files back with
    pandas and dtype=str meant every warehouse column ended up VARCHAR, and the
    incremental load then tried to subtract a string from a float. DuckDB's CSV
    sniffer gets the types right - DOUBLE for the amounts, TIMESTAMP for order_ts,
    BOOLEAN for the flags - so the tables match what the terminal run produces.
    """
    quoted = path.replace("'", "''")
    con.execute(f"""
        CREATE OR REPLACE TABLE {name} AS
        SELECT * FROM read_csv('{quoted}', header = true)
    """)
    return int(scalar(con, f"SELECT count(*) FROM {name}"))


def append_table(con: duckdb.DuckDBPyConnection, name: str, df: pd.DataFrame) -> int:
    con.register("_incoming", df)
    exists = scalar(con,
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [name])
    if exists:
        con.execute(f"INSERT INTO {name} SELECT * FROM _incoming")
    else:
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _incoming")
    con.unregister("_incoming")
    return int(scalar(con, f"SELECT count(*) FROM {name}"))


def merge_orders(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame,
                 key: str = "order_id") -> tuple[int, int]:
    """Upsert on the key: corrections overwrite, genuinely new rows are added.

    Returns (updated, inserted) so the run audit can report both.
    """
    con.register("_incoming", df)
    updated = int(scalar(con,
        f"SELECT count(*) FROM {table} t JOIN _incoming i USING ({key})"))
    con.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM _incoming)")
    con.execute(f"INSERT INTO {table} SELECT * FROM _incoming")
    con.unregister("_incoming")
    inserted = len(df) - updated
    return updated, inserted


def record_dq_results(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
            checked_at VARCHAR, detail VARCHAR
        )
    """)
    con.register("_dq", df)
    con.execute("""
        INSERT INTO dq_results
        SELECT run_id, dag_id, check_name, dimension, severity,
               rows_checked, rows_failed, status, checked_at, detail
        FROM _dq
    """)
    con.unregister("_dq")
    return len(df)


def record_run(con: duckdb.DuckDBPyConnection, run: dict) -> None:
    """One row per run, written by a task with trigger_rule='all_done'."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time VARCHAR, end_time VARCHAR,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT,
            status VARCHAR, notes VARCHAR
        )
    """)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
                [run["run_id"], run["dag_id"]])
    con.execute("""
        INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [run["run_id"], run["dag_id"], run["start_time"], run["end_time"],
          run["rows_extracted"], run["rows_loaded"], run["rows_quarantined"],
          run["status"], run.get("notes", "")])


def last_rows_loaded(con: duckdb.DuckDBPyConnection, dag_id: str) -> int | None:
    """The previous successful run's loaded row count, used as the volume baseline.

    Returns None on the first ever run, so the volume check can skip instead of
    inventing an expectation out of nothing.
    """
    exists = scalar(con,
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'pipeline_runs'")
    if not exists:
        return None
    row = one(con, """
        SELECT rows_loaded FROM pipeline_runs
        WHERE dag_id = ? AND status = 'SUCCESS' AND rows_loaded > 0
        ORDER BY end_time DESC LIMIT 1
    """, [dag_id])
    return int(row[0]) if row else None


def quarantine(df: pd.DataFrame, source: str, run_id: str) -> Path | None:
    """Failed rows go to a dated CSV, never back into the pipeline.

    PII is masked on the way out. A quarantine file is still an export of customer
    records, it lives on a filesystem rather than in the warehouse, and nothing
    deletes it - so it is the last place that should hold a readable phone number.
    """
    if df is None or df.empty:
        return None
    cfg.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    safe_run = run_id.replace(":", "-").replace("+", "_")
    path = cfg.QUARANTINE_DIR / f"{source}_failed_{safe_run}.csv"
    transforms.mask_pii_columns(df).to_csv(path, index=False)
    return path


def now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
