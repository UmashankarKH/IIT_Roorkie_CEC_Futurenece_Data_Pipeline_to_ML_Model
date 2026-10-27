"""Part B: the ELT pipeline. Load the files into raw as-is, then do every transform
in SQL inside DuckDB.

The Python here is only an orchestrator: it reads a .sql file, substitutes the run
details, and runs it. No pandas, no cleaning logic. That is the whole point of the
comparison in Part G - the ETL pipeline's rules live in Python and the ELT pipeline's
rules live in include/sql/, and the mart totals have to agree either way.

Layers:
    raw     -> include/sql/raw/        untouched text, append per source file
    staging -> include/sql/staging/    cast, standardise, dedupe, quality flags
    mart    -> include/sql/mart/       star schema and the reporting tables
"""

from __future__ import annotations

import logging
from pathlib import Path

from . import config as cfg
from . import warehouse as wh

log = logging.getLogger(__name__)

DAG_ID = f"nammamart_elt_{cfg.OWNER}"

RAW_TABLES = {
    "orders": "raw_orders",
    "payments": "raw_payments",
    "customers": "raw_customers",
    "products": "raw_products",
    "stores": "raw_stores",
}

STAGING_MODELS = ["v_stg_orders", "stg_orders", "stg_payments", "stg_customers",
                  "stg_products", "stg_stores"]

MART_MODELS = [
    "mart_dim_customer", "mart_dim_product", "mart_dim_store",
    "v_fact_orders", "mart_fact_orders",
    "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
    "mart_payment_reconciliation", "mart_top_customers",
]


def _read_sql(layer: str, name: str, **params: str) -> str:
    """Load a .sql file and substitute the placeholders.

    str.format is not used on purpose: the SQL contains regex literals like {2,}
    and format would choke on them. Plain replace is dull but it cannot misfire.
    """
    sql = (cfg.SQL_DIR / layer / f"{name}.sql").read_text()
    for key, value in params.items():
        sql = sql.replace("{" + key + "}", str(value))
    return sql


def _run_script(con, sql: str) -> None:
    # DuckDB's execute() takes multiple statements separated by semicolons.
    con.execute(sql)


# ----------------------------------------------------------------- raw layer

def load_raw(dataset: str, run_id: str, data_dir: Path | None = None,
             warehouse: Path | None = None) -> int:
    """One task per source file. The file lands in raw exactly as it arrived."""
    file_name = cfg.SOURCE_FILES[dataset]
    path = Path(data_dir or cfg.DATA_DIR) / file_name
    if not path.exists():
        raise FileNotFoundError(f"Source file missing: {path}")

    table = RAW_TABLES[dataset]
    sql = _read_sql("raw", "load_raw", table=table, path=path.as_posix(),
                    run_id=run_id, source_file=file_name)
    with wh.connect(warehouse) as con:
        _run_script(con, sql)
        rows = int(wh.scalar(con, f"SELECT count(*) FROM {table}"))
    log.info("load_raw %s: %s rows now in %s", dataset, rows, table)
    return rows


def load_raw_increment(run_id: str, data_dir: Path | None = None,
                       warehouse: Path | None = None) -> int:
    """The 1 Oct increment goes into the same raw table under its own _source_file."""
    path = Path(data_dir or cfg.DATA_DIR) / cfg.INCREMENT_FILE
    if not path.exists():
        log.info("no increment file at %s, nothing to load", path)
        return 0
    sql = _read_sql("raw", "load_raw", table="raw_orders", path=path.as_posix(),
                    run_id=run_id, source_file=cfg.INCREMENT_FILE)
    with wh.connect(warehouse) as con:
        _run_script(con, sql)
        rows = int(wh.scalar(
            con, "SELECT count(*) FROM raw_orders WHERE _source_file = ?",
            [cfg.INCREMENT_FILE]))
    log.info("load_raw_increment: %s increment rows in raw_orders", rows)
    return rows


# ------------------------------------------------------------- staging layer

def build_staging(warehouse: Path | None = None) -> dict[str, dict[str, int]]:
    """Run every staging model, then report how each one split."""
    summary: dict[str, dict[str, int]] = {}
    with wh.connect(warehouse) as con:
        # Macros first: the models call title_case() and the mask_* helpers.
        _run_script(con, _read_sql("staging", "macros"))
        for model in STAGING_MODELS:
            _run_script(con, _read_sql("staging", model))
            if model.startswith("v_"):
                continue        # a view, counting it would just re-run the projection
            passed, rejected = wh.one(con, f"""
                SELECT count_if(dq_status = 'PASSED'), count_if(dq_status = 'REJECTED')
                FROM {model}
            """)
            summary[model] = {"passed": int(passed), "rejected": int(rejected)}
    log.info("build_staging: %s", summary)
    return summary


def merge_staging_increment(run_id: str, warehouse: Path | None = None) -> dict[str, int]:
    """Part B3, staging half: the 160 increment rows are MERGEd into stg_orders.

    The rest of staging is untouched - the increment file only carries orders.
    """
    with wh.connect(warehouse) as con:
        _run_script(con, _read_sql("staging", "macros"))
        _run_script(con, _read_sql("staging", "v_stg_orders"))
        actions = con.execute(_read_sql("staging", "merge_stg_orders",
                                        run_id=run_id)).fetchall()
        total = int(wh.scalar(con, "SELECT count(*) FROM stg_orders"))

    updated = sum(1 for (action,) in actions if action == "UPDATE")
    inserted = sum(1 for (action,) in actions if action == "INSERT")
    log.info("merge_staging_increment: %s updated, %s inserted, stg_orders now %s rows",
             updated, inserted, total)
    return {"updated": updated, "inserted": inserted, "stg_rows": total}


def record_quality(run_id: str, warehouse: Path | None = None) -> dict[str, int]:
    """Part D evidence: the rule catalogue, the per-check results, the quarantine."""
    with wh.connect(warehouse) as con:
        _run_script(con, _read_sql("staging", "dq_rule_catalog"))
        _run_script(con, _read_sql("staging", "record_dq_results",
                                   run_id=run_id, dag_id=DAG_ID))
        _run_script(con, _read_sql("staging", "elt_quarantine", run_id=run_id))

        failed, warned = wh.one(con, """
            SELECT count_if(status = 'FAIL'), count_if(status = 'WARN')
            FROM dq_results WHERE run_id = ? AND dag_id = ?
        """, [run_id, DAG_ID])
        quarantined = int(wh.scalar(
            con, "SELECT count(*) FROM elt_quarantine WHERE run_id = ?", [run_id]))
        stray = int(wh.scalar(con, """
            SELECT count(*) FROM dq_results
            WHERE run_id = ? AND dag_id = ? AND dimension = 'UNCATALOGUED'
        """, [run_id, DAG_ID]))

        # Hand the quarantine to whoever has to fix the data, not just to the warehouse.
        cfg.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        safe_run = run_id.replace(":", "-").replace("+", "_")
        out = cfg.QUARANTINE_DIR / f"elt_quarantine_{safe_run}.csv"
        con.execute(f"""
            COPY (SELECT * FROM elt_quarantine WHERE run_id = '{run_id}')
            TO '{out.as_posix()}' (HEADER, DELIMITER ',')
        """)

    if stray:
        log.warning("%s staging rule(s) are not in dq_rule_catalog", stray)
    log.info("record_quality: %s checks failed, %s warned, %s rows quarantined -> %s",
             failed, warned, quarantined, out.name)
    return {"checks_failed": int(failed), "checks_warned": int(warned),
            "rows_quarantined": quarantined, "uncatalogued_rules": stray}


# ---------------------------------------------------------------- mart layer

def build_mart(warehouse: Path | None = None) -> dict[str, int]:
    counts: dict[str, int] = {}
    with wh.connect(warehouse) as con:
        for model in MART_MODELS:
            _run_script(con, _read_sql("mart", model))
            if model.startswith("v_"):
                continue        # a view, counting it would just re-run the join
            counts[model] = int(wh.scalar(con, f"SELECT count(*) FROM {model}"))
    log.info("build_mart: %s", counts)
    return counts


def merge_increment(run_id: str, warehouse: Path | None = None) -> dict[str, int]:
    """Part B3: MERGE the increment into the fact table instead of reloading it."""
    with wh.connect(warehouse) as con:
        new_rows = int(wh.scalar(con, """
            SELECT count(*) FROM stg_orders
            WHERE _run_id = ? AND dq_status = 'PASSED'
        """, [run_id]))
        if not new_rows:
            log.info("merge_increment: nothing staged for run %s", run_id)
            return {"updated": 0, "inserted": 0}

        before = int(wh.scalar(con, "SELECT count(*) FROM mart_fact_orders"))
        actions = con.execute(_read_sql("mart", "merge_orders_increment",
                                        run_id=run_id)).fetchall()
        after = int(wh.scalar(con, "SELECT count(*) FROM mart_fact_orders"))

    updated = sum(1 for (action,) in actions if action == "UPDATE")
    inserted = sum(1 for (action,) in actions if action == "INSERT")
    log.info("merge_increment: %s updated, %s inserted, fact went %s -> %s",
             updated, inserted, before, after)
    return {"updated": updated, "inserted": inserted, "fact_rows": after}


def rebuild_reports(warehouse: Path | None = None) -> dict[str, int]:
    """After a merge the aggregates are stale, so rebuild everything above the fact."""
    counts: dict[str, int] = {}
    downstream = [m for m in MART_MODELS
                  if m not in ("v_fact_orders", "mart_fact_orders")
                  and not m.startswith("mart_dim_")]
    with wh.connect(warehouse) as con:
        for model in downstream:
            _run_script(con, _read_sql("mart", model))
            counts[model] = int(wh.scalar(con, f"SELECT count(*) FROM {model}"))
    log.info("rebuild_reports: %s", counts)
    return counts


# --------------------------------------------------------- ETL vs ELT compare

def compare_with_etl(warehouse: Path | None = None) -> dict:
    """Part G: the two pipelines have to agree, and if they do not I want to know
    by how much rather than find out from the client."""
    with wh.connect(warehouse) as con:
        tables = {row[0] for row in con.execute(
            "SELECT table_name FROM information_schema.tables").fetchall()}
        if "etl_fact_orders" not in tables:
            log.warning("etl_fact_orders is missing, run the ETL DAG first")
            return {"compared": False}

        etl = wh.one(con, """
            SELECT count(*), round(sum(revenue_amount), 2), round(sum(margin_amount), 2)
            FROM etl_fact_orders
        """)
        elt = wh.one(con, """
            SELECT count(*), round(sum(revenue_amount), 2), round(sum(margin_amount), 2)
            FROM mart_fact_orders
        """)
        only_in_etl = int(wh.scalar(con, """
            SELECT count(*) FROM etl_fact_orders e
            WHERE NOT EXISTS (SELECT 1 FROM mart_fact_orders m WHERE m.order_id = e.order_id)
        """))
        only_in_elt = int(wh.scalar(con, """
            SELECT count(*) FROM mart_fact_orders m
            WHERE NOT EXISTS (SELECT 1 FROM etl_fact_orders e WHERE e.order_id = m.order_id)
        """))

    result = {
        "compared": True,
        "etl_rows": etl[0], "elt_rows": elt[0],
        "etl_revenue": etl[1], "elt_revenue": elt[1],
        "etl_margin": etl[2], "elt_margin": elt[2],
        "only_in_etl": only_in_etl, "only_in_elt": only_in_elt,
        "rows_match": etl[0] == elt[0],
        "revenue_match": abs((etl[1] or 0) - (elt[1] or 0)) <= 0.05,
    }
    log.info("compare_with_etl: %s", result)
    if not (result["rows_match"] and result["revenue_match"]):
        # Loud, but not fatal: the numbers are the finding, so let the DAG finish
        # and leave the evidence in the log instead of killing the run.
        log.error("ETL and ELT do not agree: %s", result)
    return result
