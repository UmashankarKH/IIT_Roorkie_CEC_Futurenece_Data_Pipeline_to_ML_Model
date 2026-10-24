"""
NammaMart ETL implementation shared by the Airflow DAG and local notebook.

The module keeps orchestration in the DAG and keeps business logic here. That
separation lets us test the complete ETL flow locally before running it inside
Docker/Airflow.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from data_quality import run_quality_checks, standardise_values
from masking import mask_email, mask_phone
from observability import append_dq_results, record_pipeline_run, volume_check


DATA_DIR = Path(__file__).resolve().parent / "data" / "nammamart"
DEFAULT_WAREHOUSE = Path(__file__).resolve().parent / "warehouse_samy.duckdb"
DEFAULT_QUARANTINE_DIR = Path(__file__).resolve().parent / "quarantine" / "samy"

SOURCE_FILES = {
    "orders": "orders.csv",
    "orders_increment": "orders_2026-10-01_increment.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}


def load_source_data(data_dir: str | Path = DATA_DIR) -> dict[str, pd.DataFrame]:
    """Read all six source files without changing the source values."""

    source_dir = Path(data_dir)
    # Reading is isolated in one function so the DAG and notebook use exactly
    # the same source-file mapping and cannot accidentally load different data.
    return {
        name: pd.read_csv(source_dir / filename)
        for name, filename in SOURCE_FILES.items()
    }


def load_source_file(dataset_name: str, data_dir: str | Path = DATA_DIR) -> pd.DataFrame:
    """Read one source file so each Airflow extract task fails independently."""

    return pd.read_csv(Path(data_dir) / SOURCE_FILES[dataset_name])


def apply_increment(data: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], int, int]:
    """Upsert the next-day feed into ``orders``; return (data, inserted, updated).

    Every base row whose order_id appears in the increment is replaced, so
    corrected orders are not misclassified as duplicates. Duplicates *within*
    orders.csv are left in place so the uniqueness check still quarantines
    them, the same way stg_orders.sql only merges across loads.
    """

    original_order_ids = set(data["orders"]["order_id"].astype(str))
    increment = data["orders_increment"].copy()
    increment_ids = set(increment["order_id"].astype(str))
    base = data["orders"][~data["orders"]["order_id"].astype(str).isin(increment_ids)]
    merged = dict(data)
    merged["orders"] = pd.concat([base, increment], ignore_index=True)
    return merged, len(increment_ids - original_order_ids), len(original_order_ids & increment_ids)


def _source_file_name(dataset_name: str) -> str:
    """Convert an internal dataset name into the name used in issue logs."""

    return SOURCE_FILES[dataset_name]


def validate_data(
    data: dict[str, pd.DataFrame],
    *,
    run_id: str,
    dag_id: str = "nammamart_etl_samy",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run Step 4 checks and return the issue log plus the DQ scorecard."""

    # The shared validator records run metadata so the same scorecard can be
    # written to DuckDB by the Airflow version of this pipeline.
    return run_quality_checks(data, run_id=run_id, dag_id=dag_id)


def quarantine_critical_rows(
    data: dict[str, pd.DataFrame],
    issues: pd.DataFrame,
    *,
    run_id: str,
    quarantine_dir: str | Path = DEFAULT_QUARANTINE_DIR,
) -> list[Path]:
    """Write critical failures with one combined ``failure_reason`` per row."""

    output_dir = Path(quarantine_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    critical = issues[issues["severity"] == "Critical"].copy()
    written_files: list[Path] = []

    if critical.empty:
        return written_files

    # Combine multiple failed checks for the same source row into one reason,
    # which makes the quarantine file useful to an operations user.
    reason_by_row = (
        critical.groupby(["file", "row_key"], dropna=False)["problem"]
        .apply(lambda values: "; ".join(dict.fromkeys(values)))
        .reset_index(name="failure_reason")
    )

    for dataset_name, frame in data.items():
        file_name = _source_file_name(dataset_name)
        file_reasons = reason_by_row[reason_by_row["file"] == file_name]
        if file_reasons.empty:
            continue

        # Convert keys to strings for a stable join even when pandas inferred
        # one source column as numeric in a malformed input file.
        source = frame.copy()
        source["_join_key"] = source["order_id" if "order_id" in source.columns else (
            "payment_id" if "payment_id" in source.columns else (
                "customer_id" if "customer_id" in source.columns else (
                    "product_id" if "product_id" in source.columns else "store_id"
                )
            )
        )].astype(str)
        file_reasons = file_reasons.copy()
        file_reasons["_join_key"] = file_reasons["row_key"].astype(str)
        failed_rows = source.merge(file_reasons[["_join_key", "failure_reason"]], on="_join_key", how="inner")
        failed_rows = failed_rows.drop(columns=["_join_key"])
        # Quarantine files are read by operations stewards, so contact details
        # are masked; customer_id still traces each row back to the source.
        if "email" in failed_rows.columns:
            failed_rows["email"] = failed_rows["email"].map(mask_email)
        if "phone" in failed_rows.columns:
            failed_rows["phone"] = failed_rows["phone"].map(mask_phone)
        path = output_dir / f"{Path(file_name).stem}_failed_{run_id}.csv"
        failed_rows.to_csv(path, index=False)
        written_files.append(path)

    return written_files


def _critical_keys(issues: pd.DataFrame, file_name: str, *, ignore_duplicates: bool = False) -> set[str]:
    """Return source keys that must not enter the clean reporting table."""

    selected = issues[(issues["file"] == file_name) & (issues["severity"] == "Critical")]
    if ignore_duplicates:
        # Duplicate dimension keys are resolved by keeping the first row, so
        # the key itself stays valid; only its extra copies are quarantined.
        selected = selected[~selected["problem"].str.startswith("Duplicate")]
    return set(selected["row_key"].astype(str))


def _remove_critical_rows(frame: pd.DataFrame, key: str, blocked_keys: set[str]) -> pd.DataFrame:
    """Filter critical records while preserving the source DataFrame columns."""

    return frame[~frame[key].astype(str).isin(blocked_keys)].copy()


def add_dimension_failures(data: dict[str, pd.DataFrame], issues: pd.DataFrame) -> pd.DataFrame:
    """Flag orders whose customer, product or store failed a Critical check.

    These orders cannot join to a valid dimension row, so they are rejected.
    Recording them as Critical order issues sends them to quarantine with a
    reason instead of letting the star-schema inner join drop them silently
    (the ELT equivalent is "Unknown or failed <key>" in stg_orders).
    """

    orders = data["orders"]
    rows = []
    for file_name, key in (("customers.csv", "customer_id"), ("products.csv", "product_id"), ("stores.csv", "store_id")):
        blocked = _critical_keys(issues, file_name, ignore_duplicates=True)
        for order_id in orders.loc[orders[key].astype(str).isin(blocked), "order_id"]:
            rows.append({
                "file": "orders.csv",
                "row_key": order_id,
                "column": key,
                "problem": f"{key} failed quality checks in {file_name}",
                "quality_dimension": "Consistency",
                "proposed_rule": f"orders may only reference a {key} that passes its own checks",
                "severity": "Critical",
            })
    if not rows:
        return issues
    return pd.concat([issues, pd.DataFrame(rows, columns=issues.columns)], ignore_index=True)


def rejected_order_count(issues: pd.DataFrame) -> int:
    """Distinct orders rejected by a Critical rule (same measure as ELT FAIL rows)."""

    critical_orders = issues[(issues["file"] == "orders.csv") & (issues["severity"] == "Critical")]
    return critical_orders["row_key"].astype(str).nunique()


def build_clean_tables(data: dict[str, pd.DataFrame], issues: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Transform valid data into the ETL star-schema inputs."""

    # Standardise casing/spelling first so the fact table carries the same
    # values that the validator checked (``upi`` -> ``UPI``).
    data = standardise_values(data)

    # Critical order failures are excluded from finance facts. Warning-level
    # PII format issues remain loadable and are protected during masking.
    orders = _remove_critical_rows(data["orders"], "order_id", _critical_keys(issues, "orders.csv"))

    # Dimension duplicates are reduced to one deterministic row so joins do
    # not multiply revenue. Their original failures remain in quarantine/logs.
    customers = data["customers"].drop_duplicates("customer_id", keep="first").copy()
    products = data["products"].drop_duplicates("product_id", keep="first").copy()
    stores = data["stores"].drop_duplicates("store_id", keep="first").copy()

    # Remove dimensions with critical completeness/accuracy issues before the
    # join; any order depending on them will fail the inner join and stay out
    # of the finance fact table.
    customers = _remove_critical_rows(customers, "customer_id", _critical_keys(issues, "customers.csv", ignore_duplicates=True))
    products = _remove_critical_rows(products, "product_id", _critical_keys(issues, "products.csv", ignore_duplicates=True))
    stores = _remove_critical_rows(stores, "store_id", _critical_keys(issues, "stores.csv", ignore_duplicates=True))

    customers["email_masked"] = customers["email"].map(mask_email)
    customers["phone_masked"] = customers["phone"].map(mask_phone)

    # Inner joins enforce referential integrity in the final star schema.
    # Raw email/phone stay out of the fact table; only masked values travel.
    fact = orders.merge(customers.drop(columns=["email", "phone"]), on="customer_id", how="inner", suffixes=("", "_customer"))
    fact = fact.merge(products, on="product_id", how="inner", suffixes=("", "_product"))
    fact = fact.merge(stores, on="store_id", how="inner", suffixes=("", "_store"))

    fact["order_ts"] = pd.to_datetime(fact["order_ts"], errors="coerce")
    fact["order_date"] = fact["order_ts"].dt.date.astype(str)
    fact["net_revenue"] = fact["order_amount"].where(fact["order_status"] == "DELIVERED", 0.0)
    fact["gross_margin"] = fact["net_revenue"] - (fact["quantity"] * fact["cost_price"])
    fact["within_15_min"] = (fact["order_status"] == "DELIVERED") & (fact["delivery_minutes"] <= 15)

    customer_dim = customers[["customer_id", "full_name", "email_masked", "phone_masked", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"]].copy()
    product_dim = products[["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"]].copy()
    store_dim = stores[["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"]].copy()

    return {
        "fact_orders": fact,
        "dim_customer": customer_dim,
        "dim_product": product_dim,
        "dim_store": store_dim,
    }


def build_payment_reconciliation(
    clean_tables: dict[str, pd.DataFrame],
    payments: pd.DataFrame,
) -> pd.DataFrame:
    """Create one reconciliation row for every payment/order mismatch."""

    fact = clean_tables["fact_orders"]
    payments = standardise_values({"payments": payments})["payments"]
    order_amounts = fact[["order_id", "order_amount", "payment_mode", "order_status"]].drop_duplicates("order_id")
    payments = payments.drop(columns=["_payment_ts"], errors="ignore")
    joined = order_amounts.merge(payments, on="order_id", how="outer", indicator=True, suffixes=("_order", "_payment"))
    mismatch = joined[\
        (joined["_merge"] != "both")
        | ((joined["payment_status"] == "SUCCESS") & ((joined["amount_paid"] - joined["order_amount"]).abs() > 0.01))
        | (joined["payment_mode_order"].notna() & joined["payment_mode_payment"].notna() & (joined["payment_mode_order"] != joined["payment_mode_payment"]))
    ].copy()

    def reason(row: pd.Series) -> str:
        if row["_merge"] == "left_only":
            return "Missing payment for order"
        if row["_merge"] == "right_only":
            return "Payment references an order outside the clean orders"
        if row["payment_status"] == "SUCCESS" and abs(row["amount_paid"] - row["order_amount"]) > 0.01:
            return "Successful payment amount mismatch"
        return "Payment mode mismatch"

    columns = ["order_id", "payment_id", "reason", "order_amount", "amount_paid", "order_payment_mode", "payment_payment_mode", "payment_status"]
    if mismatch.empty:
        return pd.DataFrame(columns=columns)
    mismatch["reason"] = mismatch.apply(reason, axis=1)
    mismatch =mismatch.rename(columns={"payment_mode_order": "order_payment_mode", "payment_mode_payment": "payment_payment_mode"})
    return mismatch[columns]


def build_reports(fact_orders: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build the first two business reports from the clean fact table."""

    daily_zone = (
        fact_orders[fact_orders["order_status"] == "DELIVERED"]
        .groupby(["order_date", "zone", "store_id"], as_index=False)["net_revenue"]
        .sum()
        .rename(columns={"net_revenue": "daily_net_revenue"})
    )
    category_margin = (
        fact_orders[fact_orders["order_status"] == "DELIVERED"]
        .groupby("category", as_index=False)
        .agg(revenue=("net_revenue", "sum"), gross_margin=("gross_margin", "sum"))
    )
    return {
        "daily_zone_revenue": daily_zone,
        "category_margin": category_margin,
    }


def write_tables(frames: dict[str, pd.DataFrame], *, warehouse_path: str | Path = DEFAULT_WAREHOUSE) -> None:
    """Replace each named derived table; used by every ETL load step."""

    warehouse = Path(warehouse_path)
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    try:
        for table_name, frame in frames.items():
            # Replacing derived tables makes a rerun deterministic and avoids
            # double-counting revenue when the same source is processed twice.
            con.register("current_frame", frame)
            con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM current_frame')
            con.unregister("current_frame")
    finally:
        con.close()


HISTORY_TABLE = "etl_dim_customer_history"
HISTORY_COLUMNS = ["customer_id", "loyalty_tier", "version", "valid_from", "valid_to", "is_current", "run_id"]
OPEN_VALID_TO = pd.Timestamp("9999-12-31")


def update_customer_history(
    dim_customer: pd.DataFrame,
    *,
    run_id: str,
    effective_at: datetime | None = None,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
) -> dict[str, int]:
    """Apply today's customer snapshot to the SCD Type 2 loyalty-tier history.

    A customer whose tier differs from their open version gets that version
    closed (valid_to = effective_at, is_current = False) and a new version
    opened; a new customer gets version 1. An unchanged snapshot changes
    nothing, so reruns and retries are idempotent.
    """

    effective_at = pd.Timestamp(effective_at or datetime.now(timezone.utc)).tz_localize(None)
    con = duckdb.connect(str(warehouse_path))
    try:
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [HISTORY_TABLE]
        ).fetchone()[0]
        history = con.execute(f'SELECT * FROM "{HISTORY_TABLE}"').fetchdf() if exists else pd.DataFrame(columns=HISTORY_COLUMNS)
    finally:
        con.close()

    snapshot = dim_customer[["customer_id", "loyalty_tier"]]
    current = history[history["is_current"].astype(bool)][["customer_id", "loyalty_tier", "version"]]
    compared = snapshot.merge(current, on="customer_id", how="left", suffixes=("", "_current"))
    changes = compared[compared["version"].isna() | (compared["loyalty_tier"] != compared["loyalty_tier_current"])]
    changed_ids = set(changes.loc[changes["version"].notna(), "customer_id"])

    closing = history["is_current"].astype(bool) & history["customer_id"].isin(changed_ids)
    history.loc[closing, "valid_to"] = effective_at
    history.loc[closing, "is_current"] = False
    opened = pd.DataFrame({
        "customer_id": changes["customer_id"],
        "loyalty_tier": changes["loyalty_tier"],
        "version": changes["version"].fillna(0).astype(int) + 1,
        "valid_from": effective_at,
        "valid_to": OPEN_VALID_TO,
        "is_current": True,
        "run_id": run_id,
    })
    history = pd.concat([history, opened], ignore_index=True) if len(history) else opened
    history = history.astype({"version": int, "is_current": bool})
    write_tables({HISTORY_TABLE: history[HISTORY_COLUMNS]}, warehouse_path=warehouse_path)
    return {"new_customers": int(changes["version"].isna().sum()), "tier_changes": len(changed_ids), "history_rows": len(history)}


def record_dq_results(
    dq_results: pd.DataFrame,
    *,
    order_rows: int,
    run_id: str,
    dag_id: str,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
) -> pd.DataFrame:
    """Add the run-over-run volume check and append the run's scorecard."""

    checked_at = str(dq_results["checked_at"].iloc[0]) if len(dq_results) else datetime.now(timezone.utc).isoformat()
    volume = volume_check(warehouse_path, run_id=run_id, dag_id=dag_id, order_rows=order_rows, checked_at=checked_at)
    scorecard = pd.concat([dq_results, volume], ignore_index=True)
    append_dq_results(warehouse_path, scorecard)
    return scorecard


def write_warehouse(
    clean_tables: dict[str, pd.DataFrame],
    reconciliation: pd.DataFrame,
    reports: dict[str, pd.DataFrame],
    dq_results: pd.DataFrame,
    *,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
) -> None:
    """Write the ETL outputs idempotently and append the run's scorecard."""

    write_tables(
        {
            "etl_fact_orders": clean_tables["fact_orders"],
            "etl_dim_customer": clean_tables["dim_customer"],
            "etl_dim_product": clean_tables["dim_product"],
            "etl_dim_store": clean_tables["dim_store"],
            "etl_payment_reconciliation": reconciliation,
            "etl_daily_zone_revenue": reports["daily_zone_revenue"],
            "etl_category_margin": reports["category_margin"],
        },
        warehouse_path=warehouse_path,
    )
    # dq_results is appended (not replaced) so it is a scorecard over time.
    append_dq_results(warehouse_path, dq_results)


def run_etl(
    *,
    data_dir: str | Path = DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    quarantine_dir: str | Path = DEFAULT_QUARANTINE_DIR,
    run_id: str | None = None,
    dag_id: str = "nammamart_etl_samy",
    include_increment: bool = False,
    history_effective_at: datetime | None = None,
) -> dict[str, Any]:
    """Run the ETL flow, optionally applying the next-day increment feed."""

    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    start_time = datetime.now(timezone.utc).isoformat()
    data = load_source_data(data_dir)
    # Count only the files this run uses: the increment only when applied.
    rows_extracted = sum(len(frame) for name, frame in data.items() if include_increment or name != "orders_increment")
    increment_inserted = 0
    increment_updated = 0
    if include_increment:
        data, increment_inserted, increment_updated = apply_increment(data)

    issues, dq_results = validate_data(data, run_id=run_id, dag_id=dag_id)
    issues = add_dimension_failures(data, issues)
    quarantine_files = quarantine_critical_rows(data, issues, run_id=run_id, quarantine_dir=quarantine_dir)
    clean_tables = build_clean_tables(data, issues)
    reconciliation = build_payment_reconciliation(clean_tables, data["payments"])
    reports = build_reports(clean_tables["fact_orders"])
    dq_results = pd.concat([dq_results, volume_check(warehouse_path, run_id=run_id, dag_id=dag_id, order_rows=len(data["orders"]), checked_at=str(dq_results["checked_at"].iloc[0]))], ignore_index=True)
    write_warehouse(clean_tables, reconciliation, reports, dq_results, warehouse_path=warehouse_path)
    history = update_customer_history(clean_tables["dim_customer"], run_id=run_id, effective_at=history_effective_at, warehouse_path=warehouse_path)
    end_time = datetime.now(timezone.utc).isoformat()
    record_pipeline_run(
        warehouse_path,
        run_id=run_id,
        dag_id=dag_id,
        start_time=start_time,
        end_time=end_time,
        rows_extracted=rows_extracted,
        rows_loaded=len(clean_tables["fact_orders"]),
        rows_quarantined=rejected_order_count(issues),
        status="SUCCESS",
    )

    return {
        "run_id": run_id,
        "rows_extracted": rows_extracted,
        "rows_loaded": len(clean_tables["fact_orders"]),
        "rows_quarantined": rejected_order_count(issues),
        "issue_rows": len(issues),
        "quarantine_files": [str(path) for path in quarantine_files],
        "warehouse_path": str(warehouse_path),
        "reconciliation_rows": len(reconciliation),
        "reports": reports,
        "increment_inserted": increment_inserted,
        "increment_updated": increment_updated,
        "customer_history": history,
    }
