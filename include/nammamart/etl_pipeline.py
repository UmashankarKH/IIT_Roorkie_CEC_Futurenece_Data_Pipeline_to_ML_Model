"""
include/nammamart/etl_pipeline.py
---------------------------------
The ETL pipeline for NammaMart (assignment Part A).

**ETL** = Extract, Transform, Load.  Data is cleaned *before* it reaches the
warehouse, so the finance tables only ever contain rows that passed every
critical check.

Flow implemented here::

    extract (5 files, in parallel)
        -> validate  (Part D rules, Critical vs Warning)
        -> quarantine (rejected rows + failure_reason)
        -> transform (standardise, dedupe, mask PII, join, derive)
        -> load      (star schema in DuckDB, idempotent upsert)
        -> reconcile (orders vs payments)
        -> report    (zone revenue, category margin)

Every function is plain Python so it can be called from an Airflow task, from
``scripts/run_etl_local.py`` or from a unit test.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from . import config, dq_rules, masking, transforms as tf, warehouse

logger = logging.getLogger(__name__)

DAG_ID = f"nammamart_etl_{config.STUDENT}"
STAGING_ROOT = config.INCLUDE_DIR / "staging" / config.STUDENT

# Order matters: masters are validated first so the orders rules can check that
# customer_id / product_id / store_id really exist in the *clean* master data.
VALIDATION_ORDER = ("stores", "products", "customers", "orders", "payments")


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def read_source_csv(path: str | Path) -> pd.DataFrame:
    """Read a CSV with every column as text.

    Reading as text keeps the row exactly as the source system sent it; the
    quality rules decide later whether a value can be converted to a number or
    a timestamp.  A missing file raises ``FileNotFoundError`` on purpose so the
    Airflow task fails loudly (Part F experiment 4).
    """
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Source file not found: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    logger.info("[Extract] %s -> %s rows, %s columns", path.name, len(df), len(df.columns))
    return df


def extract(dataset: str, data_dir: Path | None = None, include_increment: bool = False) -> pd.DataFrame:
    """Extract one source file. ``orders`` may optionally include the next-day file."""
    base_dir = Path(data_dir or config.DATA_DIR)
    frame = read_source_csv(base_dir / config.SOURCE_FILES[dataset])
    if dataset == "orders" and include_increment:
        increment_file = base_dir / config.INCREMENT_FILE
        increment = read_source_csv(increment_file)
        increment["_source_file"] = config.INCREMENT_FILE
        frame["_source_file"] = config.SOURCE_FILES["orders"]
        # An order_id that appears in a later file is a *correction*, not a
        # duplicate: the newer row replaces the older one. Duplicates *inside*
        # one file are left untouched so the uniqueness rule still reports them.
        corrected = set(tf.clean_text(increment["order_id"]).dropna())
        superseded = tf.clean_text(frame["order_id"]).isin(corrected)
        frame = pd.concat([frame.loc[~superseded], increment], ignore_index=True)
        logger.info("[Extract] orders + increment -> %s rows (%s corrections applied, "
                    "%s new orders)", len(frame), int(superseded.sum()),
                    len(increment) - int(superseded.sum()))
    return frame


def staging_dir(run_id: str) -> Path:
    path = STAGING_ROOT / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_staging(run_id: str, name: str, df: pd.DataFrame) -> str:
    path = staging_dir(run_id) / f"{name}.csv"
    df.to_csv(path, index=False)
    return str(path)


def read_staging(run_id: str, name: str) -> pd.DataFrame:
    path = staging_dir(run_id) / f"{name}.csv"
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------
def build_context(frames: dict[str, pd.DataFrame], run_id: str, dag_id: str = DAG_ID,
                  now: pd.Timestamp | None = None) -> dq_rules.ValidationContext:
    """Create the reference sets the consistency rules need."""
    return dq_rules.ValidationContext(
        run_id=run_id,
        dag_id=dag_id,
        now=now or pd.Timestamp.now(),
        expected_counts=dict(config.EXPECTED_ROW_COUNTS),
    )


def validate_all(frames: dict[str, pd.DataFrame], run_id: str, dag_id: str = DAG_ID,
                 now: pd.Timestamp | None = None) -> dict[str, dq_rules.ValidationResult]:
    """Validate every dataset.

    Foreign-key rules are checked against the **source** master keys, so an
    order is rejected only when its ``product_id`` / ``store_id`` /
    ``customer_id`` genuinely does not exist.  A master row that fails an
    unrelated quality rule (for example a blank category) is quarantined from
    the dimension, but it must not silently delete real revenue.
    """
    ctx = build_context(frames, run_id, dag_id, now)

    def source_keys(dataset: str, column: str) -> set[str]:
        if dataset not in frames or column not in frames[dataset].columns:
            return set()
        return set(tf.clean_text(frames[dataset][column]).dropna())

    ctx.valid_store_ids = source_keys("stores", "store_id")
    ctx.valid_product_ids = source_keys("products", "product_id")
    ctx.valid_customer_ids = source_keys("customers", "customer_id")
    ctx.valid_order_ids = source_keys("orders", "order_id")

    results: dict[str, dq_rules.ValidationResult] = {}
    for dataset in VALIDATION_ORDER:
        if dataset not in frames:
            continue
        if dataset == "payments":
            # The amount rule can only be judged against orders that passed
            # validation; mismatches for rejected orders surface in the
            # reconciliation table instead.
            clean_orders = results["orders"].passed if "orders" in results else pd.DataFrame()
            if not clean_orders.empty:
                amounts = clean_orders.assign(
                    _key=tf.clean_text(clean_orders["order_id"]),
                    _amount=tf.to_number(clean_orders["order_amount"]),
                ).dropna(subset=["_key"])
                ctx.order_amounts = dict(zip(amounts["_key"], amounts["_amount"]))
        results[dataset] = dq_rules.validate(dataset, frames[dataset], ctx)

    return results


def collect_dq_rows(results: dict[str, dq_rules.ValidationResult]) -> list[dict]:
    rows: list[dict] = []
    for result in results.values():
        rows.extend(result.dq_rows)
    return rows


# ---------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------
def quarantine(results: dict[str, dq_rules.ValidationResult], run_id: str,
               quarantine_dir: Path | None = None) -> dict[str, str]:
    """Write rejected rows to ``include/quarantine/<student>/`` with the reason.

    Rows are never silently dropped (client acceptance criterion 4): every
    rejected row keeps all its original columns plus ``failure_reason``,
    ``run_id`` and ``source_file``.

    Personal data is masked before the file is written, because quarantine
    files live in the repository and the client's rule is that no unmasked
    customer data may leave ``include/data/``.  The untouched original is still
    available in the source file and in the ELT raw layer.
    """
    target_dir = Path(quarantine_dir or config.QUARANTINE_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}

    for dataset, result in results.items():
        if result.failed.empty:
            logger.info("[Quarantine] %s: no rejected rows", dataset)
            continue
        path = target_dir / f"{dataset}_failed_{run_id}.csv"
        masking.mask_pii_in_place(result.failed).to_csv(path, index=False)
        written[dataset] = str(path)
        logger.info("[Quarantine] %s: %s rows -> %s", dataset, len(result.failed), path.name)
    return written


def quarantine_frame(results: dict[str, dq_rules.ValidationResult]) -> pd.DataFrame:
    """All rejected rows in one narrow frame, for the ``etl_quarantine`` table."""
    rows = []
    for dataset, result in results.items():
        if result.failed.empty:
            continue
        key_column = config.PRIMARY_KEYS[dataset]
        safe_rows = masking.mask_pii_in_place(result.failed)
        frame = pd.DataFrame({
            "run_id": result.failed["run_id"],
            "dag_id": result.failed["dag_id"],
            "dataset": dataset,
            "source_file": result.failed["source_file"],
            "row_key": result.failed[key_column].astype("string"),
            "failure_reason": result.failed["failure_reason"].astype("string"),
            # the record as received, with personal data masked
            "raw_record": safe_rows.drop(
                columns=["failure_reason", "dq_status", "source_file", "run_id", "dag_id",
                         "quarantined_at", "dq_warnings"], errors="ignore"
            ).astype("string").apply(lambda r: r.to_json(), axis=1),
            "quarantined_at": result.failed["quarantined_at"],
        })
        rows.append(frame)
    if not rows:
        return pd.DataFrame(columns=["run_id", "dag_id", "dataset", "source_file", "row_key",
                                     "failure_reason", "raw_record", "quarantined_at"])
    return pd.concat(rows, ignore_index=True)


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    """Clean, deduplicate and mask the customer dimension."""
    df = customers.copy()
    df["customer_id"] = tf.clean_text(df["customer_id"])
    df = tf.deduplicate(df, "customer_id", order_by="signup_date")
    df["loyalty_tier"] = tf.standardise_upper(df["loyalty_tier"]).fillna("BRONZE")
    df["city"] = tf.standardise_title(df["city"]).fillna("Unknown")
    df["area"] = tf.standardise_title(df["area"])
    df["is_active"] = tf.standardise_yes_no(df["is_active"]).fillna("N")
    df["signup_date"] = tf.to_timestamp(df["signup_date"]).dt.date
    df = masking.mask_customer_frame(df)
    columns = ["customer_id", "customer_name_masked", "email_masked", "phone_masked", "city",
               "area", "pincode", "signup_date", "loyalty_tier", "is_active"]
    return df[[c for c in columns if c in df.columns]].reset_index(drop=True)


def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    df = products.copy()
    df["product_id"] = tf.clean_text(df["product_id"])
    df = tf.deduplicate(df, "product_id", order_by="launch_date")
    df["category"] = tf.standardise_title(df["category"]).fillna("Unknown")
    df["sub_category"] = tf.standardise_title(df["sub_category"])
    df["brand"] = tf.clean_text(df["brand"])
    df["mrp"] = tf.to_number(df["mrp"])
    df["cost_price"] = tf.to_number(df["cost_price"])
    df["is_perishable"] = tf.standardise_yes_no(df["is_perishable"])
    df["launch_date"] = tf.to_timestamp(df["launch_date"]).dt.date
    columns = ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
               "cost_price", "unit", "is_perishable", "launch_date"]
    return df[[c for c in columns if c in df.columns]].reset_index(drop=True)


def build_dim_store(stores: pd.DataFrame) -> pd.DataFrame:
    df = stores.copy()
    df["store_id"] = tf.clean_text(df["store_id"])
    df = tf.deduplicate(df, "store_id")
    df["zone"] = tf.standardise_title(df["zone"]).fillna("Unknown")
    df["store_name"] = tf.clean_text(df["store_name"])
    df["area"] = tf.standardise_title(df["area"])
    df["capacity_orders_per_day"] = tf.to_number(df["capacity_orders_per_day"])
    df["open_date"] = tf.to_timestamp(df["open_date"]).dt.date
    # The store manager's name is personal data, so it is masked here too.
    df["manager_name_masked"] = df["manager_name"].map(masking.mask_name)
    df = df.drop(columns=["manager_name"])
    columns = ["store_id", "store_name", "zone", "area", "pincode", "open_date",
               "manager_name_masked", "capacity_orders_per_day"]
    return df[[c for c in columns if c in df.columns]].reset_index(drop=True)


def build_fact_orders(orders: pd.DataFrame, dim_product: pd.DataFrame,
                      dim_store: pd.DataFrame, dim_customer: pd.DataFrame,
                      run_id: str) -> pd.DataFrame:
    """Join the clean orders to the dimensions and derive the reporting columns."""
    df = orders.copy()
    for column in ("order_id", "customer_id", "store_id", "product_id"):
        df[column] = tf.clean_text(df[column])
    df["order_status"] = tf.standardise_upper(df["order_status"])
    df["payment_mode"] = tf.standardise_payment_mode(df["payment_mode"])
    df["channel"] = tf.standardise_title(df["channel"])
    df["quantity"] = tf.to_number(df["quantity"])
    df["unit_price"] = tf.to_number(df["unit_price"])
    df["discount_pct"] = tf.to_number(df["discount_pct"]).fillna(0)
    df = tf.deduplicate(df, "order_id", order_by="order_ts")
    df = tf.add_order_derivations(df)

    df = df.merge(
        dim_product[["product_id", "category", "sub_category", "cost_price"]],
        on="product_id", how="left", validate="many_to_one")
    df = df.merge(
        dim_store[["store_id", "store_name", "zone"]],
        on="store_id", how="left", validate="many_to_one")
    df = df.merge(
        dim_customer[["customer_id", "loyalty_tier"]],
        on="customer_id", how="left", validate="many_to_one")
    df = tf.add_gross_margin(df)

    df["run_id"] = run_id
    df["loaded_at"] = pd.Timestamp.now()
    columns = ["order_id", "order_ts", "order_date", "customer_id", "store_id", "store_name",
               "zone", "product_id", "category", "sub_category", "quantity", "unit_price",
               "discount_pct", "order_amount", "cost_price", "cost_of_goods", "net_revenue",
               "gross_margin", "payment_mode", "order_status", "channel", "delivery_minutes",
               "within_15_min", "is_revenue", "loyalty_tier", "dq_warnings", "run_id", "loaded_at"]
    return df[[c for c in columns if c in df.columns]].reset_index(drop=True)


def add_unknown_members(dimension: pd.DataFrame, key: str, referenced_keys: Iterable[str],
                        defaults: dict) -> pd.DataFrame:
    """Add a placeholder row for keys used by orders but missing from the dimension.

    This is the standard warehouse **unknown member** pattern.  A master row
    that was quarantined (for example product ``P0109`` with ``mrp = 0``) must
    not make a real delivered order disappear from revenue, so the fact keeps
    its row and joins to a clearly labelled placeholder.
    """
    present = set(dimension[key].dropna().astype(str))
    missing = sorted({str(k) for k in referenced_keys if k and str(k) != "nan"} - present)
    if not missing:
        return dimension
    placeholder = pd.DataFrame([{key: value, **defaults} for value in missing])
    logger.warning("[Transform] %s: added %s unknown member row(s): %s",
                   key, len(missing), ", ".join(missing[:5]))
    return pd.concat([dimension, placeholder], ignore_index=True)


def transform(clean: dict[str, pd.DataFrame], run_id: str) -> dict[str, pd.DataFrame]:
    """Build the star schema from the validated (clean) frames.

    ``clean`` maps a dataset name to the rows that passed validation, so this
    function can be called from the DAG, from the local runner or from a test
    without needing the full validation objects.
    """
    dim_customer = build_dim_customer(clean["customers"])
    dim_product = build_dim_product(clean["products"])
    dim_store = build_dim_store(clean["stores"])

    clean_orders = clean["orders"]
    dim_product = add_unknown_members(
        dim_product, "product_id", tf.clean_text(clean_orders["product_id"]).dropna(),
        {"product_name": "UNKNOWN (master row quarantined)", "category": "Unknown",
         "sub_category": "Unknown", "brand": None, "mrp": None, "cost_price": 0.0,
         "unit": None, "is_perishable": None, "launch_date": None})
    dim_store = add_unknown_members(
        dim_store, "store_id", tf.clean_text(clean_orders["store_id"]).dropna(),
        {"store_name": "UNKNOWN (master row quarantined)", "zone": "Unknown", "area": None,
         "pincode": None, "open_date": None, "manager_name_masked": None,
         "capacity_orders_per_day": None})
    dim_customer = add_unknown_members(
        dim_customer, "customer_id", tf.clean_text(clean_orders["customer_id"]).dropna(),
        {"customer_name_masked": "UNKNOWN", "email_masked": None, "phone_masked": None,
         "city": "Unknown", "area": None, "pincode": None, "signup_date": None,
         "loyalty_tier": "BRONZE", "is_active": "N"})

    fact_orders = build_fact_orders(clean_orders, dim_product, dim_store, dim_customer, run_id)
    logger.info("[Transform] fact=%s dim_customer=%s dim_product=%s dim_store=%s",
                len(fact_orders), len(dim_customer), len(dim_product), len(dim_store))
    return {
        "etl_fact_orders": fact_orders,
        "etl_dim_customer": dim_customer,
        "etl_dim_product": dim_product,
        "etl_dim_store": dim_store,
    }


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
STAGE_PREFIX = "etl_stage_"
STAR_TABLES = ("etl_fact_orders", "etl_dim_customer", "etl_dim_product", "etl_dim_store")


def stage_transformed(con, tables: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Park the transformed tables in the warehouse between two Airflow tasks.

    Passing a whole DataFrame through XCom would be slow and fragile, and a CSV
    hand-off would lose the column types.  Writing to ``etl_stage_*`` tables
    keeps the types and makes the intermediate result inspectable with SQL.
    """
    counts = {}
    for name, frame in tables.items():
        counts[name] = warehouse.replace_table(con, f"{STAGE_PREFIX}{name}", frame)
    return counts


def read_staged(con) -> dict[str, pd.DataFrame]:
    """Read the tables written by :func:`stage_transformed`."""
    return {name: warehouse.query(con, f'SELECT * FROM "{STAGE_PREFIX}{name}"')
            for name in STAR_TABLES}


def load(con, tables: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Load the star schema.

    * dimensions - full replace (small, naturally idempotent)
    * fact       - upsert on ``order_id`` so a re-run or an incremental file
      never double-counts revenue
    """
    counts: dict[str, int] = {}
    for name in ("etl_dim_customer", "etl_dim_product", "etl_dim_store"):
        counts[name] = warehouse.replace_table(con, name, tables[name])
    fact_result = warehouse.upsert_table(con, "etl_fact_orders", tables["etl_fact_orders"],
                                         key_columns=["order_id"])
    counts["etl_fact_orders"] = fact_result["total"]
    counts["etl_fact_orders_inserted"] = fact_result["inserted"]
    counts["etl_fact_orders_updated"] = fact_result["updated"]
    return counts


# ---------------------------------------------------------------------------
# Reconcile (acceptance criterion 7)
# ---------------------------------------------------------------------------
def reconcile_payments(fact_orders: pd.DataFrame, payments_raw: pd.DataFrame,
                       run_id: str) -> pd.DataFrame:
    """Compare clean orders with the payment gateway export.

    One row per mismatch, with the reason finance needs:

    * ``MISSING_PAYMENT``         - delivered order with no successful payment
    * ``AMOUNT_MISMATCH``         - paid amount differs from the order amount
    * ``ORPHAN_PAYMENT``          - payment whose order is not in the fact table
    * ``PAYMENT_FOR_NON_REVENUE`` - successful payment against a cancelled or returned order
    * ``DUPLICATE_PAYMENT``       - more than one successful payment for one order
    * ``PAYMENT_MODE_MISMATCH``   - gateway mode differs from the order mode
    """
    payments = payments_raw.copy()
    payments["order_id"] = tf.clean_text(payments["order_id"])
    payments["payment_id"] = tf.clean_text(payments["payment_id"])
    payments["payment_status"] = tf.standardise_upper(payments["payment_status"])
    payments["payment_mode"] = tf.standardise_payment_mode(payments["payment_mode"])
    payments["amount_paid"] = tf.to_number(payments["amount_paid"])

    orders = fact_orders[["order_id", "order_amount", "order_status", "payment_mode",
                          "net_revenue"]].copy()
    successful = payments[payments["payment_status"] == "SUCCESS"].copy()

    rows: list[pd.DataFrame] = []

    # 1. delivered orders without a successful payment
    paid_ids = set(successful["order_id"].dropna())
    missing = orders[(orders["order_status"] == config.REVENUE_STATUS)
                     & ~orders["order_id"].isin(paid_ids)]
    if not missing.empty:
        rows.append(pd.DataFrame({
            "order_id": missing["order_id"], "payment_id": pd.NA,
            "order_amount": missing["order_amount"], "amount_paid": 0.0,
            "difference": missing["order_amount"].round(2),
            "order_status": missing["order_status"], "payment_status": pd.NA,
            "mismatch_reason": "MISSING_PAYMENT",
        }))

    merged = successful.merge(orders, on="order_id", how="left", suffixes=("_pay", "_ord"))

    # 2. payments with no matching clean order
    orphan = merged[merged["order_amount"].isna()]
    if not orphan.empty:
        rows.append(pd.DataFrame({
            "order_id": orphan["order_id"], "payment_id": orphan["payment_id"],
            "order_amount": pd.NA, "amount_paid": orphan["amount_paid"],
            "difference": orphan["amount_paid"].round(2),
            "order_status": pd.NA, "payment_status": orphan["payment_status"],
            "mismatch_reason": "ORPHAN_PAYMENT",
        }))

    matched = merged[merged["order_amount"].notna()].copy()
    matched["difference"] = (matched["amount_paid"] - matched["order_amount"]).round(2)

    # 3. amount mismatches
    amount_bad = matched[matched["difference"].abs() > config.AMOUNT_TOLERANCE]
    if not amount_bad.empty:
        rows.append(pd.DataFrame({
            "order_id": amount_bad["order_id"], "payment_id": amount_bad["payment_id"],
            "order_amount": amount_bad["order_amount"], "amount_paid": amount_bad["amount_paid"],
            "difference": amount_bad["difference"], "order_status": amount_bad["order_status"],
            "payment_status": amount_bad["payment_status"],
            "mismatch_reason": "AMOUNT_MISMATCH",
        }))

    # 4. successful payments against orders that are not revenue
    non_revenue = matched[matched["order_status"] != config.REVENUE_STATUS]
    if not non_revenue.empty:
        rows.append(pd.DataFrame({
            "order_id": non_revenue["order_id"], "payment_id": non_revenue["payment_id"],
            "order_amount": non_revenue["order_amount"], "amount_paid": non_revenue["amount_paid"],
            "difference": non_revenue["difference"], "order_status": non_revenue["order_status"],
            "payment_status": non_revenue["payment_status"],
            "mismatch_reason": "PAYMENT_FOR_NON_REVENUE",
        }))

    # 5. the same payment sent twice, or one order paid by two different payments
    duplicate_ids = set(payments.loc[payments["payment_id"].duplicated(keep=False)
                                     & payments["payment_id"].notna(), "payment_id"])
    unique_successful = successful.drop_duplicates(subset=["payment_id"])
    repeat_orders = set(unique_successful.loc[
        unique_successful.duplicated(subset=["order_id"], keep=False), "order_id"].dropna())
    duplicates = unique_successful[unique_successful["payment_id"].isin(duplicate_ids)
                                   | unique_successful["order_id"].isin(repeat_orders)]
    if not duplicates.empty:
        duplicates = duplicates.merge(orders, on="order_id", how="left")
        rows.append(pd.DataFrame({
            "order_id": duplicates["order_id"], "payment_id": duplicates["payment_id"],
            "order_amount": duplicates["order_amount"], "amount_paid": duplicates["amount_paid"],
            "difference": (duplicates["amount_paid"] - duplicates["order_amount"]).round(2),
            "order_status": duplicates["order_status"],
            "payment_status": duplicates["payment_status"],
            "mismatch_reason": "DUPLICATE_PAYMENT",
        }))

    # 6. payment mode disagreement
    mode_bad = matched[matched["payment_mode_pay"].notna()
                       & matched["payment_mode_ord"].notna()
                       & (matched["payment_mode_pay"] != matched["payment_mode_ord"])]
    if not mode_bad.empty:
        rows.append(pd.DataFrame({
            "order_id": mode_bad["order_id"], "payment_id": mode_bad["payment_id"],
            "order_amount": mode_bad["order_amount"], "amount_paid": mode_bad["amount_paid"],
            "difference": mode_bad["difference"], "order_status": mode_bad["order_status"],
            "payment_status": mode_bad["payment_status"],
            "mismatch_reason": "PAYMENT_MODE_MISMATCH",
        }))

    if not rows:
        result = pd.DataFrame(columns=["order_id", "payment_id", "order_amount", "amount_paid",
                                       "difference", "order_status", "payment_status",
                                       "mismatch_reason"])
    else:
        result = pd.concat(rows, ignore_index=True)

    result["run_id"] = run_id
    result["reconciled_at"] = pd.Timestamp.now()
    logger.info("[Reconcile] %s mismatches found", len(result))
    return result


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def build_reports(con) -> dict[str, pd.DataFrame]:
    """Create the two summary tables the morning review needs and print them."""
    daily_zone_revenue = warehouse.query(con, """
        SELECT order_date,
               zone,
               store_id,
               store_name,
               count(*)                       AS orders,
               round(sum(net_revenue), 2)     AS net_revenue,
               round(sum(gross_margin), 2)    AS gross_margin
        FROM etl_fact_orders
        WHERE is_revenue
        GROUP BY order_date, zone, store_id, store_name
        ORDER BY order_date, zone, store_id
    """)
    category_margin = warehouse.query(con, """
        SELECT category,
               count(*)                                        AS delivered_orders,
               round(sum(net_revenue), 2)                      AS net_revenue,
               round(sum(cost_of_goods), 2)                    AS cost_of_goods,
               round(sum(gross_margin), 2)                     AS gross_margin,
               round(100 * sum(gross_margin) / nullif(sum(net_revenue), 0), 2) AS margin_pct
        FROM etl_fact_orders
        WHERE is_revenue
        GROUP BY category
        ORDER BY net_revenue DESC
    """)
    warehouse.replace_table(con, "etl_daily_zone_revenue", daily_zone_revenue)
    warehouse.replace_table(con, "etl_category_margin", category_margin)

    print("\n[Report] Daily revenue by zone and store (top 10 rows)")
    print(daily_zone_revenue.head(10).to_string(index=False))
    print("\n[Report] Revenue and gross margin by category")
    print(category_margin.to_string(index=False))
    return {"etl_daily_zone_revenue": daily_zone_revenue, "etl_category_margin": category_margin}


# ---------------------------------------------------------------------------
# Full run (used by scripts/run_etl_local.py and by the tests)
# ---------------------------------------------------------------------------
def run_etl(run_id: str | None = None, data_dir: Path | None = None,
            db_path: Path | None = None, include_increment: bool = False,
            quarantine_dir: Path | None = None, now: pd.Timestamp | None = None) -> dict:
    """Execute the whole ETL pipeline in one process and return a summary."""
    start = datetime.now()
    run_id = run_id or f"manual__{start:%Y%m%dT%H%M%S}"
    config.ensure_directories()
    summary: dict = {"run_id": run_id, "dag_id": DAG_ID}
    status = "SUCCESS"

    try:
        frames = {name: extract(name, data_dir, include_increment=include_increment)
                  for name in config.SOURCE_FILES}
        rows_extracted = sum(len(df) for df in frames.values())

        results = validate_all(frames, run_id, DAG_ID, now=now)
        quarantine_files = quarantine(results, run_id, quarantine_dir)
        tables = transform({name: result.passed for name, result in results.items()}, run_id)

        with warehouse.connect(db_path) as con:
            warehouse.ensure_audit_tables(con)
            counts = load(con, tables)
            warehouse.write_dq_results(con, collect_dq_rows(results))

            quarantined = quarantine_frame(results)
            if not quarantined.empty:
                warehouse.append_table(con, config.QUARANTINE_TABLE, quarantined)

            reconciliation = reconcile_payments(tables["etl_fact_orders"], frames["payments"], run_id)
            warehouse.replace_table(con, "etl_payment_reconciliation", reconciliation)
            reports = build_reports(con)

            rows_quarantined = sum(len(r.failed) for r in results.values())
            end = datetime.now()
            warehouse.write_pipeline_run(
                con, run_id=run_id, dag_id=DAG_ID, start_time=start, end_time=end,
                rows_extracted=rows_extracted, rows_loaded=counts["etl_fact_orders"],
                rows_quarantined=rows_quarantined, status=status,
                notes=f"increment={include_increment}")

            summary.update({
                "rows_extracted": rows_extracted,
                "rows_quarantined": rows_quarantined,
                "counts": counts,
                "quarantine_files": quarantine_files,
                "reconciliation_rows": len(reconciliation),
                "zone_rows": len(reports["etl_daily_zone_revenue"]),
                "status": status,
                "duration_seconds": (end - start).total_seconds(),
            })
    except Exception as exc:  # the run audit must record failures too
        end = datetime.now()
        with warehouse.connect(db_path) as con:
            warehouse.write_pipeline_run(
                con, run_id=run_id, dag_id=DAG_ID, start_time=start, end_time=end,
                rows_extracted=0, rows_loaded=0, rows_quarantined=0, status="FAILED",
                notes=f"{type(exc).__name__}: {exc}"[:400])
        logger.error("[ETL] run %s failed: %s", run_id, exc)
        raise

    return summary
