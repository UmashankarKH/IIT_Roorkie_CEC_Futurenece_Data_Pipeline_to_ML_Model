"""
include/nammamart/elt_pipeline.py
---------------------------------
The ELT pipeline for NammaMart (assignment Part B).

**ELT** = Extract, Load, Transform.  Every row is loaded into the warehouse
exactly as received, and *all* cleaning happens afterwards in SQL.

Three layers, as required by the assignment:

=========  ==========================================================
Layer      Rule
=========  ==========================================================
raw        exact copy of each CSV, all columns text, plus
           ``_loaded_at`` / ``_source_file`` / ``_run_id``;
           append only, never cleaned, never deleted
staging    SQL only: cast, standardise, dedupe, mask PII, apply the
           Part D rules, add ``dq_status`` and ``failure_reason``
mart       SQL only: business-ready tables that answer the six
           business questions
=========  ==========================================================

This module contains **no cleaning logic at all** - it only decides which
``.sql`` file runs when.  That is the whole point of ELT.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import config, dq_rules, warehouse

logger = logging.getLogger(__name__)

DAG_ID = f"nammamart_elt_{config.STUDENT}"

RAW_SQL = {name: f"raw/raw_{name}.sql" for name in config.SOURCE_FILES}
STAGING_VIEW_SQL = {name: f"staging/stg_{name}_view.sql" for name in config.SOURCE_FILES}
STAGING_TABLE_SQL = {name: f"staging/stg_{name}.sql" for name in config.SOURCE_FILES}
# stg_payments must run after stg_orders: its amount rule reads the clean orders.
STAGING_ORDER = ("orders", "customers", "products", "stores", "payments")
MART_SQL = (
    "mart/mart_daily_store_kpis.sql",
    "mart/mart_category_margin.sql",
    "mart/mart_delivery_sla.sql",
    "mart/mart_payment_reconciliation.sql",
    "mart/mart_top_customers.sql",
)


# ---------------------------------------------------------------------------
# Layer 1 - RAW
# ---------------------------------------------------------------------------
def load_raw(con, dataset: str, run_id: str, data_dir: Path | None = None) -> int:
    """Append one CSV into its raw table, exactly as received."""
    base_dir = Path(data_dir or config.DATA_DIR)
    source_file = config.SOURCE_FILES[dataset]
    path = (base_dir / source_file).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Source file not found: {path}")

    before = warehouse.row_count(con, f"raw_{dataset}")
    warehouse.run_sql_file(con, RAW_SQL[dataset], {
        "file_path": str(path).replace("'", "''"),
        "source_file": source_file,
        "run_id": run_id,
    })
    after = warehouse.row_count(con, f"raw_{dataset}")
    logger.info("[ELT raw] raw_%s: +%s rows (total %s)", dataset, after - before, after)
    return after - before


def load_raw_increment(con, run_id: str, data_dir: Path | None = None) -> int:
    """Append the next-day orders file into ``raw_orders`` (Part B.3)."""
    base_dir = Path(data_dir or config.DATA_DIR)
    path = (base_dir / config.INCREMENT_FILE).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Increment file not found: {path}")

    before = warehouse.row_count(con, "raw_orders")
    warehouse.run_sql_file(con, RAW_SQL["orders"], {
        "file_path": str(path).replace("'", "''"),
        "source_file": config.INCREMENT_FILE,
        "run_id": run_id,
    })
    after = warehouse.row_count(con, "raw_orders")
    logger.info("[ELT raw] increment appended: +%s rows (raw_orders total %s)",
                after - before, after)
    return after - before


# ---------------------------------------------------------------------------
# Layer 2 - STAGING
# ---------------------------------------------------------------------------
def build_staging(con, full_refresh: bool = False) -> dict[str, int]:
    """Create the staging views and tables (SQL only).

    * the four master/payment tables are rebuilt in full - they are small and a
      full rebuild is naturally idempotent;
    * ``stg_orders`` is created empty and then filled by ``merge_orders`` so
      that every order row enters staging through one upsert path.
    """
    counts: dict[str, int] = {}
    warehouse.run_sql_file(con, "staging/stg_as_of_macro.sql", {"as_of": config.AS_OF})
    # The payments view reads stg_orders, so it is created later by
    # build_staging_payments() once the orders table exists.
    for dataset in ("orders", "customers", "products", "stores"):
        warehouse.run_sql_file(con, STAGING_VIEW_SQL[dataset])

    if full_refresh:
        con.execute("DROP TABLE IF EXISTS stg_orders")
    warehouse.run_sql_file(con, "staging/stg_orders_init.sql")

    for dataset in ("customers", "products", "stores"):
        warehouse.run_sql_file(con, STAGING_TABLE_SQL[dataset])
        counts[f"stg_{dataset}"] = warehouse.row_count(con, f"stg_{dataset}")
        logger.info("[ELT staging] stg_%s: %s rows", dataset, counts[f"stg_{dataset}"])
    return counts


def build_staging_payments(con) -> int:
    """Build ``stg_payments`` after ``stg_orders`` (it reads the clean orders)."""
    warehouse.run_sql_file(con, "staging/stg_as_of_macro.sql", {"as_of": config.AS_OF})
    warehouse.run_sql_file(con, STAGING_VIEW_SQL["payments"])
    warehouse.run_sql_file(con, STAGING_TABLE_SQL["payments"])
    rows = warehouse.row_count(con, "stg_payments")
    logger.info("[ELT staging] stg_payments: %s rows", rows)
    return rows


def merge_orders(con, run_id: str, source_file: str) -> dict[str, int]:
    """Upsert one orders file into ``stg_orders`` with SQL ``MERGE``.

    Returns how many rows were inserted and how many were updated, which is the
    evidence Part B.3 and Part F experiment 3 ask for.
    """
    # Count what the merge is about to do *before* running it: rows whose
    # order_id already exists will be updated, the rest will be inserted.
    source_rows = int(con.execute("""
        SELECT count(*) FROM v_stg_orders
        WHERE _run_id = ? AND _source_file = ? AND _version_rank = 1
    """, [run_id, source_file]).fetchone()[0])
    matched = int(con.execute("""
        SELECT count(*) FROM v_stg_orders s
        WHERE s._run_id = ? AND s._source_file = ? AND s._version_rank = 1
          AND EXISTS (SELECT 1 FROM stg_orders t WHERE t.order_id = s.order_id)
    """, [run_id, source_file]).fetchone()[0])

    statement = warehouse.read_sql("staging/stg_orders_merge.sql").format(
        run_id=run_id, source_file=source_file)
    con.execute(statement)

    result = {
        "source_file": source_file,
        "source_rows": source_rows,
        "inserted": source_rows - matched,
        "updated": matched,
        "total_rows": warehouse.row_count(con, "stg_orders"),
    }
    logger.info("[ELT staging] MERGE %s: %s inserted, %s updated, %s rows in stg_orders",
                source_file, result["inserted"], result["updated"], result["total_rows"])
    return result


def merge_orders_increment(con, run_id: str) -> dict[str, int]:
    """Upsert the next-day orders file into ``stg_orders`` (Part B.3)."""
    return merge_orders(con, run_id, config.INCREMENT_FILE)


# ---------------------------------------------------------------------------
# Layer 3 - MART
# ---------------------------------------------------------------------------
def build_mart(con) -> dict[str, int]:
    """Run every mart SQL file and report the resulting row counts."""
    counts: dict[str, int] = {}
    for sql_file in MART_SQL:
        warehouse.run_sql_file(con, sql_file)
        table = Path(sql_file).stem
        counts[table] = warehouse.row_count(con, table)
        logger.info("[ELT mart] %s: %s rows", table, counts[table])
    return counts


# ---------------------------------------------------------------------------
# Data quality (Part D) - evaluated in SQL, written to the shared dq_results
# ---------------------------------------------------------------------------
def publish_rule_catalogue(con) -> int:
    """Copy the Python rule definitions into DuckDB.

    The catalogue is the single place where a rule's dimension and severity are
    defined, so the ETL (Python) and ELT (SQL) scorecards always agree.
    """
    catalogue = dq_rules.rule_catalogue()
    warehouse.replace_table(con, "dq_rule_catalogue", catalogue)
    return len(catalogue)


def run_dq_checks(con, run_id: str, dag_id: str = DAG_ID) -> dict[str, int]:
    """Build the per-row failure list and insert this run's quality scorecard."""
    warehouse.ensure_audit_tables(con)
    publish_rule_catalogue(con)
    warehouse.run_sql_file(con, "dq/elt_rule_failures.sql", {"run_id": run_id})
    warehouse.run_sql_file(con, "dq/dq_results_elt.sql", {"run_id": run_id, "dag_id": dag_id})
    written = int(con.execute(
        "SELECT count(*) FROM dq_results WHERE run_id = ? AND dag_id = ?",
        [run_id, dag_id]).fetchone()[0])
    failures = int(con.execute(
        "SELECT count(*) FROM elt_rule_failures WHERE run_id = ?", [run_id]).fetchone()[0])
    logger.info("[ELT dq] %s checks recorded, %s individual rule failures", written, failures)
    return {"checks_recorded": written, "rule_failures": failures}


def quarantine_summary(con) -> pd.DataFrame:
    """Rows that staging rejected, by dataset and status (never deleted)."""
    return warehouse.query(con, """
        SELECT 'orders'    AS dataset, dq_status, count(*) AS rows FROM stg_orders    GROUP BY 2
        UNION ALL SELECT 'payments',  dq_status, count(*) FROM stg_payments  GROUP BY 2
        UNION ALL SELECT 'customers', dq_status, count(*) FROM stg_customers GROUP BY 2
        UNION ALL SELECT 'products',  dq_status, count(*) FROM stg_products  GROUP BY 2
        UNION ALL SELECT 'stores',    dq_status, count(*) FROM stg_stores    GROUP BY 2
        ORDER BY dataset, dq_status
    """)


def export_quarantine(con, run_id: str, quarantine_dir: Path | None = None) -> str | None:
    """Write the rejected staging rows to a CSV, mirroring the ETL quarantine file."""
    rejected = warehouse.query(con, """
        SELECT 'orders' AS dataset, order_id AS row_key, dq_status, failure_reason,
               _source_file, _run_id
        FROM stg_orders WHERE dq_status <> 'PASS'
        UNION ALL SELECT 'payments', payment_id, dq_status, failure_reason, _source_file, _run_id
        FROM stg_payments WHERE dq_status <> 'PASS'
        UNION ALL SELECT 'customers', customer_id, dq_status, failure_reason, _source_file, _run_id
        FROM stg_customers WHERE dq_status <> 'PASS'
        UNION ALL SELECT 'products', product_id, dq_status, failure_reason, _source_file, _run_id
        FROM stg_products WHERE dq_status <> 'PASS'
        UNION ALL SELECT 'stores', store_id, dq_status, failure_reason, _source_file, _run_id
        FROM stg_stores WHERE dq_status <> 'PASS'
        ORDER BY dataset, row_key
    """)
    if rejected.empty:
        return None
    target_dir = Path(quarantine_dir or config.QUARANTINE_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"elt_staging_rejected_{run_id}.csv"
    rejected.to_csv(path, index=False)
    logger.info("[ELT quarantine] %s rejected rows -> %s", len(rejected), path.name)
    return str(path)


# ---------------------------------------------------------------------------
# Reporting helper
# ---------------------------------------------------------------------------
def print_mart_preview(con) -> None:
    """Print the headline numbers so the Airflow log shows the 9 AM figures."""
    totals = warehouse.query(con, """
        SELECT count(*) AS delivered_orders,
               round(sum(net_revenue), 2)  AS net_revenue,
               round(sum(gross_margin), 2) AS gross_margin
        FROM mart_daily_store_kpis
    """)
    print("\n[ELT report] September totals from mart_daily_store_kpis")
    print(totals.to_string(index=False))
    print("\n[ELT report] Revenue and margin by category")
    print(warehouse.query(con, "SELECT * FROM mart_category_margin").to_string(index=False))


# ---------------------------------------------------------------------------
# Full run (used by scripts/run_elt_local.py and by the tests)
# ---------------------------------------------------------------------------
def run_elt(run_id: str | None = None, data_dir: Path | None = None,
            db_path: Path | None = None, include_increment: bool = False,
            quarantine_dir: Path | None = None) -> dict:
    """Execute the whole ELT pipeline in one process and return a summary."""
    start = datetime.now()
    run_id = run_id or f"manual__{start:%Y%m%dT%H%M%S}"
    config.ensure_directories()
    summary: dict = {"run_id": run_id, "dag_id": DAG_ID}

    try:
        with warehouse.connect(db_path) as con:
            warehouse.ensure_audit_tables(con)

            rows_extracted = 0
            for dataset in config.SOURCE_FILES:
                rows_extracted += load_raw(con, dataset, run_id, data_dir)

            increment_rows = 0
            if include_increment:
                increment_rows = load_raw_increment(con, run_id, data_dir)
                rows_extracted += increment_rows

            staging_counts = build_staging(con)
            merge_base = merge_orders(con, run_id, config.SOURCE_FILES["orders"])
            merge_result = {}
            if include_increment:
                merge_result = merge_orders_increment(con, run_id)
            staging_counts["stg_orders"] = warehouse.row_count(con, "stg_orders")
            staging_counts["stg_payments"] = build_staging_payments(con)

            dq_summary = run_dq_checks(con, run_id)
            quarantine_file = export_quarantine(con, run_id, quarantine_dir)
            mart_counts = build_mart(con)
            print_mart_preview(con)

            rows_loaded = int(con.execute(
                "SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'").fetchone()[0])
            rows_quarantined = int(con.execute("""
                SELECT (SELECT count(*) FROM stg_orders    WHERE dq_status <> 'PASS')
                     + (SELECT count(*) FROM stg_payments  WHERE dq_status <> 'PASS')
                     + (SELECT count(*) FROM stg_customers WHERE dq_status <> 'PASS')
                     + (SELECT count(*) FROM stg_products  WHERE dq_status <> 'PASS')
                     + (SELECT count(*) FROM stg_stores    WHERE dq_status <> 'PASS')
            """).fetchone()[0])

            end = datetime.now()
            warehouse.write_pipeline_run(
                con, run_id=run_id, dag_id=DAG_ID, start_time=start, end_time=end,
                rows_extracted=rows_extracted, rows_loaded=rows_loaded,
                rows_quarantined=rows_quarantined, status="SUCCESS",
                notes=f"increment={include_increment}")

            summary.update({
                "rows_extracted": rows_extracted,
                "increment_rows": increment_rows,
                "staging_counts": staging_counts,
                "merge_base": merge_base,
                "merge_increment": merge_result,
                "dq": dq_summary,
                "quarantine_file": quarantine_file,
                "mart_counts": mart_counts,
                "rows_loaded": rows_loaded,
                "rows_quarantined": rows_quarantined,
                "status": "SUCCESS",
                "duration_seconds": (end - start).total_seconds(),
            })
    except Exception as exc:
        end = datetime.now()
        with warehouse.connect(db_path) as con:
            warehouse.write_pipeline_run(
                con, run_id=run_id, dag_id=DAG_ID, start_time=start, end_time=end,
                rows_extracted=0, rows_loaded=0, rows_quarantined=0, status="FAILED",
                notes=f"{type(exc).__name__}: {exc}"[:400])
        logger.error("[ELT] run %s failed: %s", run_id, exc)
        raise

    return summary
