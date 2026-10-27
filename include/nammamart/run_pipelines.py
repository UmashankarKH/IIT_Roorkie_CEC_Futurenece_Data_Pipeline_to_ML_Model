"""Run either pipeline from a terminal, without Airflow.

This is how both pipelines were developed and how the numbers in SUBMISSION.md were
produced. The Airflow DAGs call exactly the same functions, so if something breaks in
the UI I can reproduce it here in two seconds instead of digging through task logs.

    python -m nammamart.run_pipelines --pipeline etl
    python -m nammamart.run_pipelines --pipeline elt --increment
    python -m nammamart.run_pipelines --pipeline both --increment --reset

--reset drops the raw/staging/mart tables first, which is the only honest way to test
"does a full load from scratch still give the same totals".
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from . import config as cfg
from . import elt, etl, quality, warehouse as wh

log = logging.getLogger("nammamart")

RESET_PREFIXES = ("raw_", "stg_", "mart_", "elt_", "v_")
RESET_TABLES = ("dq_rule_catalog", "dq_results", "pipeline_runs",
                "etl_fact_orders", "etl_dim_customer", "etl_dim_product", "etl_dim_store",
                "etl_payments", "etl_payment_reconciliation",
                "etl_daily_zone_revenue", "etl_category_margin")


def reset(warehouse=None) -> None:
    with wh.connect(warehouse) as con:
        # table_schema = 'main' keeps this away from DuckDB's own internal views.
        for (name,) in con.execute(
                "SELECT table_name FROM information_schema.views "
                "WHERE table_schema = 'main'").fetchall():
            if name.startswith(RESET_PREFIXES):
                con.execute(f"DROP VIEW IF EXISTS {name}")
        for (name,) in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'").fetchall():
            if name.startswith(RESET_PREFIXES) or name in RESET_TABLES:
                con.execute(f"DROP TABLE IF EXISTS {name}")
    log.info("reset: warehouse emptied")


def run_etl(run_id: str, increment: bool) -> dict:
    started = wh.now()
    extracted = 0
    validated: dict[str, dict] = {}

    for dataset in cfg.SOURCE_FILES:
        df = etl.extract(dataset)
        extracted += len(df)
        validated[dataset] = etl.validate(dataset, df, run_id)

    # Same order as the DAG: the per-file quality evidence is written before anything
    # downstream has a chance to fail (see Part F experiment 6).
    with wh.connect() as con:
        wh.record_dq_results(con, [row for v in validated.values() for row in v["dq"]])

    transformed = etl.transform(validated, run_id)
    counts = etl.load(transformed)

    quarantined = sum(len(v["failed"]) for v in validated.values()) + transformed["quarantined"]
    with wh.connect() as con:
        # Volume is judged against the last successful run, not this run's own numbers.
        baseline = wh.last_rows_loaded(con, etl.DAG_ID)
        wh.record_dq_results(con, list(transformed["dq"]) + quality.volume_and_freshness_checks(
            transformed["fact"], run_id, etl.DAG_ID, expected_rows=baseline))

    increment_result = etl.load_increment(run_id) if increment else {"updated": 0, "inserted": 0}
    # Same order as the DAG: reconcile after the increment, then report.
    mismatches = etl.load_reconciliation()
    reports = etl.build_reports()

    with wh.connect() as con:
        wh.record_run(con, {
            "run_id": run_id, "dag_id": etl.DAG_ID, "start_time": started,
            "end_time": wh.now(), "rows_extracted": extracted,
            "rows_loaded": counts["etl_fact_orders"] + increment_result.get("inserted", 0),
            "rows_quarantined": quarantined, "status": "SUCCESS",
            "notes": f"increment: {increment_result}",
        })
    return {"counts": counts, "increment": increment_result, "mismatches": mismatches,
            "quarantined": quarantined, "zones": len(reports["zone"])}


def run_elt(run_id: str, increment: bool) -> dict:
    started = wh.now()
    extracted = 0
    for dataset in elt.RAW_TABLES:
        extracted += elt.load_raw(dataset, run_id)

    elt.build_staging()
    dq = elt.record_quality(run_id)
    counts = elt.build_mart()

    increment_result = {"updated": 0, "inserted": 0}
    if increment:
        # The increment gets its own run_id, because the MERGE picks its rows out of
        # staging by _run_id and the two loads must not be confused with each other.
        increment_run = f"{run_id}_increment"
        elt.load_raw_increment(increment_run)
        elt.merge_staging_increment(increment_run)
        elt.record_quality(increment_run)
        increment_result = elt.merge_increment(increment_run)
        elt.rebuild_reports()

    comparison = elt.compare_with_etl()

    with wh.connect() as con:
        wh.record_run(con, {
            "run_id": run_id, "dag_id": elt.DAG_ID, "start_time": started,
            "end_time": wh.now(), "rows_extracted": extracted,
            "rows_loaded": increment_result.get("fact_rows", counts["mart_fact_orders"]),
            "rows_quarantined": dq["rows_quarantined"], "status": "SUCCESS",
            "notes": f"increment: {increment_result} | vs etl: {comparison}",
        })
    return {"counts": counts, "dq": dq, "increment": increment_result,
            "comparison": comparison}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NammaMart pipelines locally")
    parser.add_argument("--pipeline", choices=["etl", "elt", "both"], default="both")
    parser.add_argument("--increment", action="store_true",
                        help="also apply the 1 Oct increment file")
    parser.add_argument("--reset", action="store_true",
                        help="drop the warehouse tables first")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run_id = args.run_id or f"manual_{datetime.now():%Y%m%dT%H%M%S}"

    if args.reset:
        reset()

    if args.pipeline in ("etl", "both"):
        log.info("=== ETL (%s) ===", run_id)
        log.info("etl result: %s", run_etl(run_id, args.increment))

    if args.pipeline in ("elt", "both"):
        log.info("=== ELT (%s) ===", run_id)
        log.info("elt result: %s", run_elt(run_id, args.increment))


if __name__ == "__main__":
    main()
