"""NammaMart ELT implementation: raw load in Python, transformations in SQL."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from nammamart_etl import DATA_DIR, SOURCE_FILES
from observability import append_dq_results, record_pipeline_run, volume_check


SQL_DIR = Path(__file__).resolve().parent / "sql"
DEFAULT_WAREHOUSE = Path(__file__).resolve().parent / "warehouse_samy_elt.duckdb"

RAW_TABLES = {
    "orders": "raw_orders",
    "payments": "raw_payments",
    "customers": "raw_customers",
    "products": "raw_products",
    "stores": "raw_stores",
}

STAGING_SQL_FILES = [
    "stg_customers.sql",
    "stg_products.sql",
    "stg_stores.sql",
    "stg_orders.sql",
    "stg_payments.sql",
]

# Raw tables holding unmasked personal data. They are written to a separate
# owner-only DuckDB file so the shared warehouse never contains raw PII;
# only the staging step attaches it, as ``pii``, to build masked columns.
PII_RAW_TABLES = {"raw_customers"}
PII_ALIAS = "pii"


def pii_warehouse_path(warehouse_path: str | Path) -> Path:
    """Return the restricted raw-PII file that sits next to a warehouse."""

    warehouse = Path(warehouse_path)
    return warehouse.with_name(f"{warehouse.stem}_pii{warehouse.suffix}")


def _restrict_permissions(path: Path) -> None:
    """Limit the raw-PII file to its owner (the Airflow service user)."""

    if path.exists():
        os.chmod(path, 0o600)


MART_SQL_FILES = [
    "mart_daily_store_kpis.sql",
    "mart_category_margin.sql",
    "mart_delivery_sla.sql",
    "mart_payment_reconciliation.sql",
    "mart_top_customers.sql",
]


def load_raw_tables(
    *,
    data_dir: str | Path = DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    run_id: str = "local-elt-run",
) -> None:
    """Load the five base files as text, adding only ingestion metadata."""

    source_dir = Path(data_dir)
    pii_path = pii_warehouse_path(warehouse_path)
    con = duckdb.connect(str(warehouse_path))
    pii_con = duckdb.connect(str(pii_path))
    loaded_at = datetime.now(timezone.utc).isoformat()
    try:
        for dataset_name, table_name in RAW_TABLES.items():
            # ``keep_default_na=False`` preserves empty source fields as empty
            # strings rather than silently converting them to pandas NaN.
            frame = pd.read_csv(
                source_dir / SOURCE_FILES[dataset_name],
                dtype=str,
                keep_default_na=False,
            )
            frame["_loaded_at"] = loaded_at
            frame["_source_file"] = SOURCE_FILES[dataset_name]
            frame["_run_id"] = run_id
            target = pii_con if table_name in PII_RAW_TABLES else con
            target.register("raw_frame", frame)
            target.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM raw_frame')
            target.unregister("raw_frame")
        for table_name in PII_RAW_TABLES:
            # Remove copies left in the shared warehouse by earlier versions.
            con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    finally:
        con.close()
        pii_con.close()
    _restrict_permissions(pii_path)


def append_incremental_orders(
    *,
    data_dir: str | Path = DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    run_id: str = "local-elt-increment",
) -> dict[str, int]:
    """Append the next-day order feed to raw storage without cleaning it."""

    source_dir = Path(data_dir)
    increment = pd.read_csv(
        source_dir / SOURCE_FILES["orders_increment"],
        dtype=str,
        keep_default_na=False,
    )
    con = duckdb.connect(str(warehouse_path))
    try:
        existing_ids = set(
            con.execute("SELECT DISTINCT order_id FROM raw_orders").fetchdf()["order_id"].astype(str)
        )
        increment_ids = set(increment["order_id"].astype(str))
        increment["_loaded_at"] = datetime.now(timezone.utc).isoformat()
        increment["_source_file"] = SOURCE_FILES["orders_increment"]
        increment["_run_id"] = run_id
        con.register("increment_frame", increment)
        con.execute("INSERT INTO raw_orders SELECT * FROM increment_frame")
        con.unregister("increment_frame")
    finally:
        con.close()
    return {
        "increment_rows_appended": len(increment),
        "increment_inserted": len(increment_ids - existing_ids),
        "increment_updated": len(increment_ids & existing_ids),
    }


DQ_CHECKS_SQL_FILE = "dq_checks_elt.sql"


def _connect_with_pii(warehouse_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Open the warehouse with the raw-PII file attached read-only as ``pii``."""

    con = duckdb.connect(str(warehouse_path))
    # Raw PII is attached only while SQL runs; staging and marts persist
    # masked columns in the shared warehouse.
    con.execute(f"ATTACH '{pii_warehouse_path(warehouse_path)}' AS {PII_ALIAS} (READ_ONLY)")
    return con


def run_sql_files(filenames: list[str], *, warehouse_path: str | Path = DEFAULT_WAREHOUSE) -> None:
    """Execute checked-in SQL files in order (one Airflow task per layer)."""

    con = _connect_with_pii(warehouse_path)
    try:
        for filename in filenames:
            con.execute((SQL_DIR / filename).read_text())
    finally:
        con.close()


def execute_sql_layers(*, warehouse_path: str | Path = DEFAULT_WAREHOUSE) -> None:
    """Run every staging and mart transformation from a checked-in SQL file."""

    # Staging runs first so marts always see fully created staging tables.
    run_sql_files(STAGING_SQL_FILES, warehouse_path=warehouse_path)
    run_sql_files(MART_SQL_FILES, warehouse_path=warehouse_path)


def record_dq_results(
    *,
    run_id: str,
    dag_id: str = "nammamart_elt_samy",
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
) -> pd.DataFrame:
    """Run the SQL scorecard and append it, with the volume check, to dq_results."""

    checked_at = datetime.now(timezone.utc).isoformat()
    con = _connect_with_pii(warehouse_path)
    try:
        checks = con.execute((SQL_DIR / DQ_CHECKS_SQL_FILE).read_text()).fetchdf()
    finally:
        con.close()
    checks.insert(0, "dag_id", dag_id)
    checks.insert(0, "run_id", run_id)
    checks["status"] = checks["rows_failed"].map(lambda failed: "PASS" if failed == 0 else "FAIL")
    checks["checked_at"] = checked_at
    current_orders = _count_current_orders(warehouse_path)
    volume = volume_check(warehouse_path, run_id=run_id, dag_id=dag_id, order_rows=current_orders, checked_at=checked_at)
    scorecard = pd.concat([checks, volume], ignore_index=True)
    append_dq_results(warehouse_path, scorecard)
    return scorecard


HISTORY_SQL_FILE = "dim_customer_history.sql"


def update_customer_history(
    *,
    run_id: str,
    effective_at: datetime | None = None,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
) -> dict[str, int]:
    """Apply today's stg_customers snapshot to the SCD Type 2 tier history."""

    effective_at = effective_at or datetime.now(timezone.utc)
    con = duckdb.connect(str(warehouse_path))
    try:
        con.execute("SET VARIABLE scd_effective_at = CAST(? AS TIMESTAMP)", [effective_at.replace(tzinfo=None)])
        con.execute("SET VARIABLE scd_run_id = ?", [run_id])
        con.execute((SQL_DIR / HISTORY_SQL_FILE).read_text())
        new, changed = con.execute(
            "SELECT count(*) FILTER (WHERE current_version IS NULL), count(*) FILTER (WHERE current_version IS NOT NULL) FROM scd_changes"
        ).fetchone()
        versions = con.execute("SELECT count(*) FROM dim_customer_history").fetchone()[0]
    finally:
        con.close()
    return {"new_customers": int(new), "tier_changes": int(changed), "history_rows": int(versions)}


def _count_current_orders(warehouse_path: str | Path) -> int:
    """Rows in the latest load of each order_id (the rows the checks cover)."""

    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        return con.execute(
            "SELECT count(*) FROM (SELECT 1 FROM raw_orders QUALIFY _loaded_at = max(_loaded_at) OVER (PARTITION BY order_id))"
        ).fetchone()[0]
    finally:
        con.close()


def count_raw_rows(warehouse_path: str | Path = DEFAULT_WAREHOUSE) -> int:
    """Total raw rows across the five base tables, including restricted PII."""

    total = 0
    for table_name in RAW_TABLES.values():
        path = pii_warehouse_path(warehouse_path) if table_name in PII_RAW_TABLES else warehouse_path
        con = duckdb.connect(str(path), read_only=True)
        try:
            total += con.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]
        finally:
            con.close()
    return total


def run_elt(
    *,
    data_dir: str | Path = DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    run_id: str = "local-elt-run",
    history_effective_at: datetime | None = None,
) -> dict[str, int | str]:
    """Run the raw, staging and mart layers and return table metrics."""

    load_raw_tables(data_dir=data_dir, warehouse_path=warehouse_path, run_id=run_id)
    execute_sql_layers(warehouse_path=warehouse_path)
    record_dq_results(run_id=run_id, warehouse_path=warehouse_path)
    history = update_customer_history(run_id=run_id, effective_at=history_effective_at, warehouse_path=warehouse_path)
    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        metrics = {
            "warehouse_path": str(warehouse_path),
            "raw_orders": con.execute("SELECT count(*) FROM raw_orders").fetchone()[0],
            "stg_orders": con.execute("SELECT count(*) FROM stg_orders").fetchone()[0],
            "passing_orders": con.execute("SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'").fetchone()[0],
            "mart_store_rows": con.execute("SELECT count(*) FROM mart_daily_store_kpis").fetchone()[0],
            "mart_category_rows": con.execute("SELECT count(*) FROM mart_category_margin").fetchone()[0],
            "reconciliation_rows": con.execute("SELECT count(*) FROM mart_payment_reconciliation").fetchone()[0],
            "customer_history": history,
        }
    finally:
        con.close()
    record_pipeline_run(
        warehouse_path,
        run_id=run_id,
        dag_id="nammamart_elt_samy",
        start_time=datetime.now(timezone.utc).isoformat(),
        end_time=datetime.now(timezone.utc).isoformat(),
        rows_extracted=count_raw_rows(warehouse_path),
        rows_loaded=int(metrics["passing_orders"]),
        rows_quarantined=int(metrics["stg_orders"] - metrics["passing_orders"]),
        status="SUCCESS",
    )
    return metrics


def run_incremental_elt(
    *,
    data_dir: str | Path = DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    run_id: str = "local-elt-increment",
) -> dict[str, int | str]:
    """Append the increment to raw and rebuild staging/marts with an upsert."""

    increment_metrics = append_incremental_orders(
        data_dir=data_dir,
        warehouse_path=warehouse_path,
        run_id=run_id,
    )
    execute_sql_layers(warehouse_path=warehouse_path)
    record_dq_results(run_id=run_id, warehouse_path=warehouse_path)
    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        metrics: dict[str, int | str] = {
            "warehouse_path": str(warehouse_path),
            **increment_metrics,
            "raw_orders": con.execute("SELECT count(*) FROM raw_orders").fetchone()[0],
            "stg_orders": con.execute("SELECT count(*) FROM stg_orders").fetchone()[0],
            "passing_orders": con.execute("SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'").fetchone()[0],
            "mart_store_rows": con.execute("SELECT count(*) FROM mart_daily_store_kpis").fetchone()[0],
            "reconciliation_rows": con.execute("SELECT count(*) FROM mart_payment_reconciliation").fetchone()[0],
        }
    finally:
        con.close()
    record_pipeline_run(
        warehouse_path,
        run_id=run_id,
        dag_id="nammamart_elt_samy",
        start_time=datetime.now(timezone.utc).isoformat(),
        end_time=datetime.now(timezone.utc).isoformat(),
        rows_extracted=int(metrics["increment_rows_appended"]),
        rows_loaded=int(metrics["passing_orders"]),
        rows_quarantined=int(metrics["stg_orders"] - metrics["passing_orders"]),
        status="SUCCESS",
    )
    return metrics
