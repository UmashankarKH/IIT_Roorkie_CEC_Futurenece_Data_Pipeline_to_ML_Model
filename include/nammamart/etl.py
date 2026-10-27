"""Part A: the ETL pipeline. Extract -> Validate -> Quarantine -> Transform -> Load.

Each function is one Airflow task. They are plain functions so the whole pipeline
can also be run from a terminal (see include/nammamart/run_pipelines.py), which is
how the logic was developed and tested before Airflow was involved.

The rule that matters: nothing reaches a reporting table until it has passed every
Critical check. Bad rows are written to quarantine with the reason attached.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from . import config as cfg
from . import quality, transforms as tf, warehouse as wh

log = logging.getLogger(__name__)

DAG_ID = f"nammamart_etl_{cfg.OWNER}"

# Airflow 3 serialises XCom as JSON, so a DataFrame cannot travel between tasks.
# Each task writes its result to a CSV here and passes the path instead. The files
# are also handy evidence: "what did extract actually read" is one cat away.
TMP_DIR = cfg.INCLUDE_DIR / "tmp" / cfg.OWNER


def stash(df: pd.DataFrame, name: str, run_id: str) -> str:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    safe_run = run_id.replace(":", "-").replace("+", "_").replace("/", "-")
    path = TMP_DIR / f"{safe_run}__{name}.csv"
    df.to_csv(path, index=False)
    return str(path)


def unstash(path: str | None) -> pd.DataFrame:
    """Read a stashed file back as text, the same way extract reads a source file."""
    if not path:
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def extract(dataset: str, data_dir: Path | None = None) -> pd.DataFrame:
    """One task per file. Everything is read as text; casting is a later decision."""
    path = Path(data_dir or cfg.DATA_DIR) / cfg.SOURCE_FILES[dataset]
    if not path.exists():
        # Fail loudly: a missing export is an upstream incident, not something to paper over.
        raise FileNotFoundError(f"Source file missing: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    log.info("extract %s: %s rows from %s", dataset, len(df), path.name)
    return df


def validate(dataset: str, df: pd.DataFrame, run_id: str) -> dict:
    passed, failed, results = quality.run_checks(dataset, df, run_id, DAG_ID)
    path = wh.quarantine(failed, dataset, run_id)
    log.info("validate %s: %s passed, %s quarantined%s",
             dataset, len(passed), len(failed), f" -> {path.name}" if path else "")
    return {"passed": passed, "failed": failed, "dq": results,
            "quarantine_file": str(path) if path else None}


def extract_to_stash(dataset: str, run_id: str) -> str:
    """DAG wrapper for Extract: read the file, stash it, hand on the path."""
    return stash(extract(dataset), f"{dataset}_extracted", run_id)


def validate_to_stash(dataset: str, raw_path: str, run_id: str) -> dict:
    """DAG wrapper for Validate + Quarantine."""
    result = validate(dataset, unstash(raw_path), run_id)
    return {
        "dataset": dataset,
        "passed": stash(result["passed"], f"{dataset}_passed", run_id),
        "failed": stash(result["failed"], f"{dataset}_failed", run_id)
                  if len(result["failed"]) else None,
        "quarantine_file": result["quarantine_file"],
        "dq": result["dq"],
        "rows_passed": len(result["passed"]),
        "rows_failed": len(result["failed"]),
    }


def transform(validated: dict[str, dict], run_id: str) -> dict:
    """Standardise, dedupe, mask, then join into a star schema."""
    customers = tf.deduplicate(tf.standardise_customers(validated["customers"]["passed"]),
                               "customer_id")
    products = tf.deduplicate(tf.standardise_products(validated["products"]["passed"]),
                              "product_id")
    stores = tf.deduplicate(tf.standardise_stores(validated["stores"]["passed"]), "store_id")
    payments = tf.deduplicate(tf.standardise_payments(validated["payments"]["passed"]),
                              "payment_id")

    orders = tf.deduplicate(tf.standardise_orders(validated["orders"]["passed"]), "order_id")

    # Key existence is judged against the FULL master files, including rows that failed
    # quality, so quarantining a product does not silently delete its orders' revenue.
    def all_keys(dataset: str) -> pd.DataFrame:
        parts = [validated[dataset]["passed"], validated[dataset]["failed"]]
        return pd.concat([p for p in parts if len(p)], ignore_index=True)

    orders, orphans, ref_results = quality.referential_checks(
        orders, all_keys("customers"), all_keys("products"), all_keys("stores"),
        run_id, DAG_ID)
    orphan_file = wh.quarantine(orphans, "orders_referential", run_id)

    fact = tf.build_fact_orders(orders, customers, products, stores)
    reconciliation = tf.reconcile_payments(fact, payments)

    log.info("transform: %s fact rows, %s orphans quarantined, %s reconciliation rows",
             len(fact), len(orphans), len(reconciliation))
    return {
        "fact": fact,
        "dim_customer": customers,
        "dim_product": products,
        "dim_store": stores,
        "payments": payments,
        "reconciliation": reconciliation,
        "dq": ref_results,
        "quarantined": len(orphans),
        "quarantine_file": str(orphan_file) if orphan_file else None,
    }


def record_validation_dq(validated_paths: dict[str, dict],
                         warehouse: Path | None = None) -> int:
    """Write the per-file check results to dq_results as soon as they exist.

    This used to happen at the end of the run, after transform. Experiment 6 showed
    why that is wrong: the schema change made transform fail, and the dq_results rows
    explaining what had gone wrong were never written at all. Quality evidence has to
    be durable before the next thing has a chance to break.
    """
    rows = [row for item in validated_paths.values() for row in item["dq"]]
    with wh.connect(warehouse) as con:
        return wh.record_dq_results(con, rows)


def record_transform_dq(transformed_paths: dict, run_id: str,
                        warehouse: Path | None = None) -> int:
    """The cross-file checks: referential integrity, plus batch volume and freshness."""
    rows = list(transformed_paths.get("dq", []))
    with wh.connect(warehouse) as con:
        # Volume is judged against the last successful run, read from pipeline_runs.
        baseline = wh.last_rows_loaded(con, DAG_ID)
        rows += quality.volume_and_freshness_checks(
            unstash(transformed_paths["fact"]), run_id, DAG_ID, expected_rows=baseline)
        return wh.record_dq_results(con, rows)


def transform_from_stash(validated_paths: dict[str, dict], run_id: str) -> dict:
    """DAG wrapper: read the stashed validate output, transform, stash the results.

    Only paths and plain dicts cross the task boundary, which is what XCom can carry.
    """
    validated = {
        dataset: {"passed": unstash(paths.get("passed")),
                  "failed": unstash(paths.get("failed")),
                  "dq": paths.get("dq", [])}
        for dataset, paths in validated_paths.items()
    }
    out = transform(validated, run_id)
    return {
        "fact": stash(out["fact"], "fact_orders", run_id),
        "dim_customer": stash(out["dim_customer"], "dim_customer", run_id),
        "dim_product": stash(out["dim_product"], "dim_product", run_id),
        "dim_store": stash(out["dim_store"], "dim_store", run_id),
        "payments": stash(out["payments"], "payments", run_id),
        "reconciliation": stash(out["reconciliation"], "reconciliation", run_id),
        "dq": out["dq"],
        "quarantined": out["quarantined"],
        "fact_rows": len(out["fact"]),
    }


def load_star(paths: dict, warehouse: Path | None = None) -> dict:
    """DAG wrapper for Load: the star schema only, reconciliation is its own task.

    The stashed CSVs are handed to DuckDB rather than re-read with pandas, so the
    warehouse columns keep their types. See warehouse.load_csv for why that matters.
    """
    tables = {
        "etl_fact_orders": paths["fact"],
        "etl_dim_customer": paths["dim_customer"],
        "etl_dim_product": paths["dim_product"],
        "etl_dim_store": paths["dim_store"],
        "etl_payments": paths["payments"],
    }
    counts = {}
    with wh.connect(warehouse) as con:
        for name, path in tables.items():
            counts[name] = wh.load_csv(con, name, path)
    log.info("load_star: %s", counts)
    return counts


def load_reconciliation(warehouse: Path | None = None) -> int:
    """Part A6: one row per order/payment mismatch, in its own table and task.

    It reads the fact table back out of the warehouse rather than using the stashed
    transform output, so it has to run AFTER the incremental load. Built from the
    stash it ignored the 150 increment orders and finance would have been handed a
    reconciliation that was already a day out of date.
    """
    with wh.connect(warehouse) as con:
        fact = con.execute("SELECT * FROM etl_fact_orders").df()
        payments = con.execute("SELECT * FROM etl_payments").df()
        recon = tf.reconcile_payments(fact, payments)
        rows = wh.replace_table(con, "etl_payment_reconciliation", recon)
    log.info("etl_payment_reconciliation: %s mismatches", rows)
    return rows


def load(transformed: dict, warehouse: Path | None = None) -> dict:
    """Idempotent load: CREATE OR REPLACE, so a second run cannot double-count."""
    counts: dict[str, int] = {}
    with wh.connect(warehouse) as con:
        counts["etl_fact_orders"] = wh.replace_table(con, "etl_fact_orders", transformed["fact"])
        counts["etl_dim_customer"] = wh.replace_table(con, "etl_dim_customer",
                                                      transformed["dim_customer"])
        counts["etl_dim_product"] = wh.replace_table(con, "etl_dim_product",
                                                     transformed["dim_product"])
        counts["etl_dim_store"] = wh.replace_table(con, "etl_dim_store", transformed["dim_store"])
        counts["etl_payments"] = wh.replace_table(con, "etl_payments", transformed["payments"])
        if transformed.get("reconciliation") is not None:
            counts["etl_payment_reconciliation"] = wh.replace_table(
                con, "etl_payment_reconciliation", transformed["reconciliation"])
    log.info("load: %s", counts)
    return counts


def load_increment(run_id: str, data_dir: Path | None = None,
                   warehouse: Path | None = None) -> dict:
    """Part B3 for the ETL side: new orders appended, corrections overwritten.

    The increment's checks are recorded under "<run_id>_increment". Sharing the
    run_id put two sets of orders_* rows in dq_results for the same run, one for the
    full load and one for the increment, and there was no way to tell them apart.
    """
    run_id = f"{run_id}_increment"
    path = Path(data_dir or cfg.DATA_DIR) / cfg.INCREMENT_FILE
    if not path.exists():
        log.info("no increment file at %s, nothing to do", path)
        return {"updated": 0, "inserted": 0}

    raw = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    passed, failed, results = quality.run_checks("orders", raw, run_id, DAG_ID)
    wh.quarantine(failed, "orders_increment", run_id)

    # The key check has to see the FULL master files, same as transform() does. The
    # dimension tables only hold the master rows that passed quality, and checking
    # against those threw away 11 perfectly good increment orders the first time.
    master_keys = {}
    for dataset, key in (("customers", "customer_id"), ("products", "product_id"),
                         ("stores", "store_id")):
        master_path = Path(data_dir or cfg.DATA_DIR) / cfg.SOURCE_FILES[dataset]
        master_keys[dataset] = pd.read_csv(master_path, dtype=str, usecols=[key])

    with wh.connect(warehouse) as con:
        wh.record_dq_results(con, results)
        customers = con.execute("SELECT * FROM etl_dim_customer").df()
        products = con.execute("SELECT * FROM etl_dim_product").df()
        stores = con.execute("SELECT * FROM etl_dim_store").df()

        orders = tf.deduplicate(tf.standardise_orders(passed), "order_id")
        orders, orphans, ref_results = quality.referential_checks(
            orders, master_keys["customers"], master_keys["products"],
            master_keys["stores"], run_id, DAG_ID)
        wh.quarantine(orphans, "orders_increment_referential", run_id)
        wh.record_dq_results(con, ref_results)

        fact = tf.build_fact_orders(orders, customers, products, stores)
        updated, inserted = wh.merge_orders(con, "etl_fact_orders", fact)

    log.info("increment: %s updated, %s inserted", updated, inserted)
    return {"updated": updated, "inserted": inserted, "quarantined": len(failed) + len(orphans)}


def build_reports(warehouse: Path | None = None) -> dict[str, pd.DataFrame]:
    """Final task: the two tables the COO actually looks at, printed into the log."""
    with wh.connect(warehouse) as con:
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, zone, store_name,
                   count(*) AS delivered_orders,
                   round(sum(revenue_amount), 2) AS net_revenue,
                   round(avg(delivery_minutes), 1) AS avg_delivery_minutes,
                   round(100.0 * avg(CASE WHEN within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min
            FROM etl_fact_orders
            WHERE is_revenue
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT category,
                   count(*) AS delivered_orders,
                   round(sum(revenue_amount), 2) AS net_revenue,
                   round(sum(margin_amount), 2) AS gross_margin,
                   round(100.0 * sum(margin_amount) / nullif(sum(revenue_amount), 0), 1) AS margin_pct
            FROM etl_fact_orders
            WHERE is_revenue
            GROUP BY 1
            ORDER BY net_revenue DESC
        """)
        zone = con.execute("""
            SELECT zone, round(sum(net_revenue), 2) AS net_revenue,
                   sum(delivered_orders) AS orders
            FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY net_revenue DESC
        """).df()
        category = con.execute("SELECT * FROM etl_category_margin").df()

    log.info("Revenue by zone:\n%s", zone.to_string(index=False))
    log.info("Margin by category:\n%s", category.to_string(index=False))
    return {"zone": zone, "category": category}
