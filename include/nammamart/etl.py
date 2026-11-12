"""Reusable CSV and Parquet output helpers for NammaMart ETL."""

from pathlib import Path
import re

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"
OUTPUT_DIR = PROJECT_ROOT / "include" / "output" / "nammamart"


def read_source_csv(
    filename: str, source_dir: Path | None = None
) -> pd.DataFrame:
    """Read an input CSV without modifying the source file.

    ``source_dir`` lets Airflow tasks process the immutable, run-scoped copies
    created by the parallel extraction tasks.
    """
    path = (source_dir or SOURCE_DIR) / filename

    if not path.is_file():
        raise FileNotFoundError(f"Source CSV not found: {path}")

    return pd.read_csv(path, dtype="string", keep_default_na=True)


def _mask_email_value(value) -> str | None:
    if pd.isna(value) or not str(value).strip():
        return None
    text = str(value).strip()
    if "*" in text:
        return text
    if "@" not in text:
        return "***"
    local, domain = text.split("@", 1)
    return f"{local[:2]}***@{domain}" if local else f"***@{domain}"


def _mask_phone_value(value) -> str | None:
    if pd.isna(value) or not str(value).strip():
        return None
    text = str(value).strip()
    if "*" in text:
        return text
    digits = "".join(char for char in text if char.isdigit())
    if len(digits) < 4:
        return "*" * max(len(digits), 4)
    return f"{digits[:2]}{'*' * max(len(digits) - 4, 4)}{digits[-2:]}"


def write_dataset(df: pd.DataFrame, dataset_name: str) -> dict[str, str]:
    """Write a DataFrame as CSV and Parquet; return their paths."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / f"{dataset_name}.csv"
    parquet_path = OUTPUT_DIR / f"{dataset_name}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    return {
        "csv": str(csv_path),
        "parquet": str(parquet_path),
    }



def _split_conflicting_keys(
    df: pd.DataFrame, key: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Quarantine missing keys and conflicting business-key records."""

    key_values = df[key].astype("string").str.strip()
    missing_key = key_values.isna() | key_values.eq("")
    keyed = df.loc[~missing_key]

    duplicate_rows = keyed.loc[
        keyed.duplicated(subset=[key], keep=False)
    ]
    conflicting_keys = []

    for value, group in duplicate_rows.groupby(key, sort=False):
        # Exact repeats are not conflicts; differing records are.
        if len(group.drop_duplicates()) > 1:
            conflicting_keys.append(value)

    conflicts = df.loc[key_values.isin(conflicting_keys)].copy()
    conflicts["quarantine_reason"] = "conflicting_business_key"

    missing = df.loc[missing_key].copy()
    missing["quarantine_reason"] = "missing_business_key"

    clean = df.loc[
        ~missing_key & ~key_values.isin(conflicting_keys)
    ].copy()

    quarantine = pd.concat(
        [conflicts, missing],
        ignore_index=True,
    )

    return clean, quarantine



def transform_customers(
    source_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean customers and quarantine critical timestamp/key failures."""
    df = read_source_csv("customers.csv", source_dir).drop_duplicates().copy()

    clean, key_quarantine = _split_conflicting_keys(df, "customer_id")

    signup_ts = pd.to_datetime(clean["signup_date"], errors="coerce", format="mixed")
    now = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)

    if signup_ts.dt.tz is not None:
        signup_ts = signup_ts.dt.tz_localize(None)

    invalid_signup = signup_ts.isna()
    future_signup = signup_ts.notna() & (signup_ts > now)

    timestamp_quarantine = clean.loc[invalid_signup | future_signup].copy()
    timestamp_quarantine["quarantine_reason"] = "invalid_signup_date"
    timestamp_quarantine.loc[future_signup.loc[timestamp_quarantine.index], "quarantine_reason"] = (
        "future_customer_signup_date"
    )

    clean = clean.loc[~(invalid_signup | future_signup)].copy()

    clean["city"] = clean["city"].fillna("UNKNOWN")
    clean["city"] = clean["city"].astype("string").str.strip().replace("", "UNKNOWN")

    quarantine = pd.concat(
        [key_quarantine, timestamp_quarantine],
        ignore_index=True,
        sort=False,
    )

    # Mask customer contact fields in curated ETL outputs. Raw values remain
    # only in the original source file and the access-restricted quarantine.
    if "email" in clean.columns:
        clean["email"] = clean["email"].map(_mask_email_value)
    if "phone" in clean.columns:
        clean["phone"] = clean["phone"].map(_mask_phone_value)

    return clean, quarantine



def transform_products(
    source_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean products and quarantine key conflicts and invalid pricing."""
    from include.nammamart.quality import product_pricing_issues

    df = read_source_csv("products.csv", source_dir).drop_duplicates().copy()

    clean, key_quarantine = _split_conflicting_keys(df, "product_id")

    clean["category"] = clean["category"].fillna("UNKNOWN")
    clean["category"] = clean["category"].str.strip().replace("", "UNKNOWN")

    pricing_issues = product_pricing_issues(clean)

    if pricing_issues.empty:
        return clean, key_quarantine

    # Group issue descriptions by product so one product can retain
    # multiple quarantine reasons.
    reasons = (
        pricing_issues.groupby("row_key")["problem"]
        .agg(lambda values: "; ".join(dict.fromkeys(values)))
    )

    invalid_mask = clean["product_id"].isin(reasons.index)
    invalid_products = clean.loc[invalid_mask].copy()

    invalid_products["quarantine_reason"] = (
        invalid_products["product_id"]
        .map(reasons)
        .map(lambda value: value.replace(
            "MRP must be a valid number greater than zero",
            "invalid_mrp",
        ).replace(
            "Cost price must not exceed MRP",
            "cost_price_exceeds_mrp",
        ))
    )

    clean = clean.loc[~invalid_mask].copy()

    quarantine = pd.concat(
        [key_quarantine, invalid_products],
        ignore_index=True,
        sort=False,
    )

    return clean, quarantine



def normalize_order_values(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize approved order aliases and return unsupported records separately."""
    clean = df.copy()

    # Preserve source values for auditability.
    clean["source_order_status"] = clean["order_status"]
    clean["source_payment_mode"] = clean["payment_mode"]

    status_aliases = {
        "delivered": "DELIVERED",
        "cancelled": "CANCELLED",
        "returned": "RETURNED",
    }
    mode_aliases = {
        "upi": "UPI",
        "card": "Card",
        "cash on delivery": "Cash on Delivery",
        "cod": "Cash on Delivery",
        "wallet": "Wallet",
    }

    status_text = clean["order_status"].astype("string").str.strip()
    mode_text = clean["payment_mode"].astype("string").str.strip()

    clean["order_status"] = status_text.map(
        lambda value: (
            status_aliases.get(value.lower(), value)
            if pd.notna(value) else value
        )
    )
    clean["payment_mode"] = mode_text.map(
        lambda value: (
            mode_aliases.get(value.lower(), value)
            if pd.notna(value) else value
        )
    )

    valid_statuses = {"DELIVERED", "CANCELLED", "RETURNED"}
    valid_modes = {"UPI", "Card", "Cash on Delivery", "Wallet"}

    invalid_mask = (
        ~clean["order_status"].isin(valid_statuses)
        | ~clean["payment_mode"].isin(valid_modes)
    )

    invalid = clean.loc[invalid_mask].copy()
    invalid_reasons = []

    for _, row in invalid.iterrows():
        reasons = []
        if row["order_status"] not in valid_statuses:
            reasons.append("invalid_order_status")
        if row["payment_mode"] not in valid_modes:
            reasons.append("invalid_payment_mode")
        invalid_reasons.append("; ".join(reasons))

    invalid["quarantine_reason"] = invalid_reasons
    clean = clean.loc[~invalid_mask].copy()

    return clean, invalid


def transform_orders(
    source_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deduplicate base orders and upsert the incremental order file."""
    base = read_source_csv("orders.csv", source_dir)
    incremental = read_source_csv("orders_2026-10-01_increment.csv", source_dir)

    base["source_file"] = "orders.csv"
    incremental["source_file"] = "orders_2026-10-01_increment.csv"

    # Remove exact repeated rows from the base data.
    base = base.drop_duplicates().copy()

    # Incremental records are authoritative for the order_status field.
    # Reject missing/blank order IDs into quarantine.
    missing_base = (
        base["order_id"].isna() | base["order_id"].str.strip().eq("")
    )
    missing_incremental = (
        incremental["order_id"].isna()
        | incremental["order_id"].str.strip().eq("")
    )

    quarantine = pd.concat(
        [base.loc[missing_base], incremental.loc[missing_incremental]],
        ignore_index=True,
    )

    base = base.loc[~missing_base].copy()
    incremental = incremental.loc[~missing_incremental].copy()

    # A repeated order ID with different records is ambiguous.
    base, base_conflicts = _split_conflicting_keys(base, "order_id")
    incremental, incremental_conflicts = _split_conflicting_keys(
        incremental, "order_id"
    )
    quarantine = pd.concat(
        [quarantine, base_conflicts, incremental_conflicts],
        ignore_index=True,
    )

    # Incremental order IDs update order_status only.
    # New incremental IDs are inserted as new orders.
    base_by_id = base.set_index("order_id", drop=False)
    incremental_by_id = incremental.set_index("order_id", drop=False)

    overlap = base_by_id.index.intersection(incremental_by_id.index)
    new_ids = incremental_by_id.index.difference(base_by_id.index)

    if len(overlap):
        base_by_id.loc[overlap, "order_status"] = (
            incremental_by_id.loc[overlap, "order_status"]
        )

    combined = pd.concat(
        [base_by_id, incremental_by_id.loc[new_ids]],
        ignore_index=True,
    )

    # Re-check the key after merging.
    duplicate_ids = combined["order_id"].duplicated(keep=False)
    if duplicate_ids.any():
        raise ValueError(
            "Order upsert produced duplicate order IDs: "
            f"{combined.loc[duplicate_ids, 'order_id'].tolist()}"
        )

    # Normalize the final upserted order values.
    # Invalid statuses or payment modes are quarantined, not curated.
    normalized, invalid_values = normalize_order_values(combined)

    quarantine = pd.concat(
        [quarantine, invalid_values],
        ignore_index=True,
    )

    return normalized, quarantine


#def transform_payments(
#    valid_order_ids: set[str],
#) -> tuple[pd.DataFrame, pd.DataFrame]:
#    """Remove exact payment replays and quarantine invalid order references."""
#    df = read_source_csv("payments.csv")
#
#    # Remove exact repeated payment rows.
#    df = df.drop_duplicates().copy()
#
#    missing_id = df["payment_id"].isna() | df["payment_id"].str.strip().eq("")
#    duplicate_id = df["payment_id"].duplicated(keep=False) & ~missing_id
#
#    # Conflicting records for one payment ID must not be selected arbitrarily.
#    conflicting_ids = []
#    for payment_id, group in df.loc[duplicate_id].groupby("payment_id"):
#        if len(group.drop_duplicates()) > 1:
#            conflicting_ids.append(payment_id)
#
#    conflicts = df.loc[df["payment_id"].isin(conflicting_ids)].copy()
#    missing_keys = df.loc[missing_id].copy()
#
#    df = df.loc[~df["payment_id"].isin(conflicting_ids) & ~missing_id].copy()
#
#    # Payment records without a matching curated order are quarantined.
#    order_ref = df["order_id"].astype("string").str.strip()
#    orphan_mask = order_ref.isna() | order_ref.eq("") | ~order_ref.isin(valid_order_ids)
#    orphans = df.loc[orphan_mask].copy()
#    clean = df.loc[~orphan_mask].copy()

#   quarantine = pd.concat(
#        [conflicts, missing_keys, orphans], ignore_index=True
#    )

#    return clean, quarantine

def transform_payments(
    valid_order_ids: set[str],
    order_amounts: pd.DataFrame | None = None,
    source_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deduplicate payments and quarantine invalid references or amounts."""
    df = read_source_csv("payments.csv", source_dir).drop_duplicates().copy()
    quarantine_parts = []

    # Identify missing payment IDs.
    payment_id = df["payment_id"].astype("string").str.strip()
    missing_id = payment_id.isna() | payment_id.eq("")

    missing_payments = df.loc[missing_id].copy()
    if not missing_payments.empty:
        missing_payments["quarantine_reason"] = "missing_payment_id"
        quarantine_parts.append(missing_payments)

    # Identify conflicting records sharing a payment ID.
    keyed = df.loc[~missing_id]
    duplicate_mask = keyed.duplicated("payment_id", keep=False)
    duplicate_rows = keyed.loc[duplicate_mask]

    conflicting_ids = []
    for value, group in duplicate_rows.groupby("payment_id", sort=False):
        if len(group.drop_duplicates()) > 1:
            conflicting_ids.append(value)

    conflicts = df.loc[payment_id.isin(conflicting_ids)].copy()
    if not conflicts.empty:
        conflicts["quarantine_reason"] = "conflicting_payment_id"
        quarantine_parts.append(conflicts)

    # Continue with usable, non-conflicting payment IDs.
    df = df.loc[
        ~missing_id & ~payment_id.isin(conflicting_ids)
    ].copy()

    # Validate order references.
    order_ref = df["order_id"].astype("string").str.strip()
    missing_order = order_ref.isna() | order_ref.eq("")

    valid_ids = {
        str(value).strip()
        for value in valid_order_ids
        if pd.notna(value) and str(value).strip()
    }

    orphan_order = ~missing_order & ~order_ref.isin(valid_ids)

    missing_order_rows = df.loc[missing_order].copy()
    if not missing_order_rows.empty:
        missing_order_rows["quarantine_reason"] = "missing_order_id"
        quarantine_parts.append(missing_order_rows)

    orphan_rows = df.loc[orphan_order].copy()
    if not orphan_rows.empty:
        orphan_rows["quarantine_reason"] = "orphan_order_id"
        quarantine_parts.append(orphan_rows)

    clean = df.loc[~missing_order & ~orphan_order].copy()

    # Reconcile successful payments when order amounts are supplied.
    if order_amounts is not None and not clean.empty:
        amounts = order_amounts[["order_id", "order_amount"]].copy()
        amounts["order_id"] = amounts["order_id"].astype("string").str.strip()
        amounts["order_amount"] = pd.to_numeric(
            amounts["order_amount"], errors="coerce"
        ).round(2)

        clean["amount_paid"] = pd.to_numeric(
            clean["amount_paid"], errors="coerce"
        ).round(2)

        clean = clean.merge(
            amounts,
            on="order_id",
            how="left",
            validate="many_to_one",
        )

        successful = (
            clean["payment_status"].astype("string").str.strip().str.upper()
            .eq("SUCCESS")
        )

        mismatch = successful & (
            clean["order_amount"].isna()
            | clean["amount_paid"].isna()
            | clean["amount_paid"].ne(clean["order_amount"])
        )

        amount_quarantine = clean.loc[mismatch].copy()
        if not amount_quarantine.empty:
            amount_quarantine = amount_quarantine.drop(
                columns=["order_amount"]
            )
            amount_quarantine["amount_paid"] = (
                amount_quarantine["amount_paid"].astype("string")
            )
            amount_quarantine["quarantine_reason"] = "payment_amount_mismatch"
            quarantine_parts.append(amount_quarantine)

        clean = clean.loc[~mismatch].drop(columns=["order_amount"])

    quarantine = (
        pd.concat(quarantine_parts, ignore_index=True)
        if quarantine_parts
        else pd.DataFrame(columns=[*df.columns, "quarantine_reason"])
    )
        # Keep payment amount types consistent for Parquet serialization.
    if "amount_paid" in quarantine.columns:
        quarantine["amount_paid"] = (
            quarantine["amount_paid"].astype("string")
        )

    return clean, quarantine


def transform_stores(
    source_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean stores and quarantine conflicting keys or invalid capacity."""
    df = read_source_csv("stores.csv", source_dir).drop_duplicates().copy()

    clean, quarantine = _split_conflicting_keys(df, "store_id")

    # Match ELT: missing, invalid, or non-positive capacity is Critical.
    capacity = pd.to_numeric(
        clean["capacity_orders_per_day"], errors="coerce"
    )
    invalid_capacity = capacity.isna() | capacity.le(0)

    capacity_quarantine = clean.loc[invalid_capacity].copy()
    if not capacity_quarantine.empty:
        capacity_quarantine["quarantine_reason"] = (
            "invalid_capacity_orders_per_day"
        )

    clean = clean.loc[~invalid_capacity].copy()

    clean["zone"] = clean["zone"].fillna("UNKNOWN")
    clean["zone"] = clean["zone"].str.strip().replace("", "UNKNOWN")

    quarantine = pd.concat(
        [quarantine, capacity_quarantine],
        ignore_index=True,
    )

    return clean, quarantine




def validate_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    stores: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Quarantine orders with missing fields, invalid timestamps, or orphan references."""
    df = orders.copy()
    reasons = pd.Series("", index=df.index, dtype="string")

    required_fields = [
        "order_id", "order_ts", "customer_id", "product_id", "store_id"
    ]

    # Validate required fields.
    for column in required_fields:
        values = df[column].astype("string").str.strip()
        missing = df[column].isna() | values.eq("")

        if column == "order_ts":
            missing = missing | values.str.upper().eq("N/A")

        reasons.loc[missing] = reasons.loc[missing].apply(
            lambda current: f"{current}; missing_{column}".strip("; ")
        )

    # Reject timestamps that are present but cannot be parsed.
    timestamp_text = df["order_ts"].astype("string").str.strip()
    timestamp_values = pd.to_datetime(
        timestamp_text,
        errors="coerce",
        format="mixed",
    )
    invalid_timestamp = (
        df["order_ts"].notna()
        & timestamp_text.ne("")
        & timestamp_text.str.upper().ne("N/A")
        & timestamp_values.isna()
    )
    reasons.loc[invalid_timestamp] = reasons.loc[invalid_timestamp].apply(
        lambda current: f"{current}; invalid_order_ts".strip("; ")
    )
        # Quarantine orders with future timestamps.
    now = pd.Timestamp.now(tz="UTC")

    comparable_timestamps = timestamp_values.copy()

    if comparable_timestamps.dt.tz is None:
        comparable_timestamps = comparable_timestamps.dt.tz_localize("UTC")
    else:
        comparable_timestamps = comparable_timestamps.dt.tz_convert("UTC")

    future_timestamp = (
        comparable_timestamps.notna()
        & (comparable_timestamps > now)
    )

    reasons.loc[future_timestamp] = reasons.loc[future_timestamp].apply(
        lambda current: f"{current}; future_order_ts".strip("; ")
    )
    # Validate foreign-key references.
    references = [
        ("customer_id", customers, "customer_id"),
        ("product_id", products, "product_id"),
        ("store_id", stores, "store_id"),
    ]

    for child_key, parent, parent_key in references:
        key = df[child_key].astype("string").str.strip()
        valid_parent_keys = (
            parent[parent_key].dropna().astype("string").str.strip()
        )
        orphan = key.notna() & key.ne("") & ~key.isin(valid_parent_keys)
        reasons.loc[orphan] = reasons.loc[orphan].apply(
            lambda current: f"{current}; orphan_{child_key}".strip("; ")
        )

    # Validate critical order accuracy rules.
    from include.nammamart.quality import order_accuracy_issues

    accuracy_issues = order_accuracy_issues(df)

    if not accuracy_issues.empty:
        reason_mapping = {
            "Quantity must be greater than zero": "invalid_quantity",
            "Discount must be between 0 and 100": "invalid_discount_pct",
            "Order amount does not match the required formula":
                "order_amount_mismatch",
        }

        accuracy_issues = accuracy_issues.copy()
        accuracy_issues["row_key"] = (
            accuracy_issues["row_key"].astype("string")
        )
        accuracy_issues["reason"] = accuracy_issues["problem"].map(
            reason_mapping
        )

        accuracy_reasons = (
            accuracy_issues.groupby("row_key")["reason"]
            .agg(lambda values: "; ".join(dict.fromkeys(values)))
        )

        order_keys = df["order_id"].astype("string")
        mapped_reasons = order_keys.map(accuracy_reasons)
        has_accuracy_issue = mapped_reasons.notna()

        reasons.loc[has_accuracy_issue] = [
            f"{current}; {accuracy_reason}".strip("; ")
            for current, accuracy_reason in zip(
                reasons.loc[has_accuracy_issue],
                mapped_reasons.loc[has_accuracy_issue],
            )
        ]
        # Validate critical order delivery-time rules.
    from include.nammamart.quality import order_delivery_issues

    delivery_issues = order_delivery_issues(df)

    if not delivery_issues.empty:
        reason_mapping = {
            "Delivered order must have a valid delivery time":
                "missing_delivery_minutes",
            "Delivery time must be less than 120 minutes":
                "delivery_time_over_120",
            "Delivery time must be greater than zero":
                "invalid_delivery_minutes",
        }

        delivery_issues = delivery_issues.copy()
        delivery_issues["row_key"] = (
            delivery_issues["row_key"].astype("string")
        )
        delivery_issues["reason"] = delivery_issues["problem"].map(
            reason_mapping
        )

        delivery_reasons = (
            delivery_issues.groupby("row_key")["reason"]
            .agg(lambda values: "; ".join(dict.fromkeys(values)))
        )

        order_keys = df["order_id"].astype("string")
        mapped_reasons = order_keys.map(delivery_reasons)
        has_delivery_issue = mapped_reasons.notna()

        reasons.loc[has_delivery_issue] = [
            f"{current}; {delivery_reason}".strip("; ")
            for current, delivery_reason in zip(
                reasons.loc[has_delivery_issue],
                mapped_reasons.loc[has_delivery_issue],
            )
        ]
    quarantine_mask = reasons.ne("")
    quarantined = df.loc[quarantine_mask].copy()
    quarantined["quarantine_reason"] = reasons.loc[quarantine_mask]

    curated = df.loc[~quarantine_mask].copy()
    return curated, quarantined

def run_etl(
    run_id: str | None = None,
    source_dir: Path | None = None,
) -> dict[str, dict[str, str]]:
    """Run all transformations and persist curated and quarantine outputs.

    If ``source_dir`` is supplied, all input comes from the immutable copies
    produced by the DAG extraction tasks.
    """
    customers, customers_q = transform_customers(source_dir)
    products, products_q = transform_products(source_dir)
    stores, stores_q = transform_stores(source_dir)

    orders, orders_initial_q = transform_orders(source_dir)
    orders, orders_validation_q = validate_orders(
        orders, customers, products, stores
    )

    # Payments must reference orders that survive validation.
    payments, payments_q = transform_payments(
        set(orders["order_id"].astype(str)),
        orders[["order_id", "order_amount"]],
        source_dir=source_dir,
    )

    if run_id is None:
        from include.nammamart.dq_results import new_run_id
        run_id = new_run_id()

    orders_quarantine = pd.concat(
        [orders_initial_q, orders_validation_q], ignore_index=True, sort=False
    )
    outputs = {
        "customers": write_dataset(customers, "customers"),
        "customers_quarantine": write_dataset(customers_q, "customers_quarantine"),
        "products": write_dataset(products, "products"),
        "products_quarantine": write_dataset(products_q, "products_quarantine"),
        "stores": write_dataset(stores, "stores"),
        "stores_quarantine": write_dataset(stores_q, "stores_quarantine"),
        "orders": write_dataset(orders, "orders"),
        "orders_quarantine": write_dataset(orders_quarantine, "orders_quarantine"),
        "payments": write_dataset(payments, "payments"),
        "payments_quarantine": write_dataset(payments_q, "payments_quarantine"),
    }

    # Assignment-required run-scoped quarantine CSVs with a standard reason field.
    quarantine_dir = PROJECT_ROOT / "include" / "quarantine" / "gowtham"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(run_id)).strip("_.")
    quarantine_frames = {
        "customers": customers_q,
        "products": products_q,
        "stores": stores_q,
        "orders": orders_quarantine,
        "payments": payments_q,
    }
    for dataset_name, failed_rows in quarantine_frames.items():
        failed = failed_rows.copy()
        if "quarantine_reason" in failed.columns:
            failed = failed.rename(columns={"quarantine_reason": "failure_reason"})
        elif "failure_reason" not in failed.columns:
            failed["failure_reason"] = "unspecified_validation_failure"
        failed.to_csv(
            quarantine_dir / f"{dataset_name}_failed_{safe_run_id}.csv",
            index=False,
        )

    build_dq_results(run_id=run_id, source_dir=source_dir)
    return outputs


def build_dq_results(
    run_id: str | None = None, source_dir: Path | None = None
) -> pd.DataFrame:
    """Write standardized run-level data-quality results for ETL."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from include.nammamart.dq_results import (
        make_dq_result,
        new_run_id,
        write_dq_results,
    )
    from include.nammamart.quality import (
        customer_validity_issues,
        duplicate_key_rows,
        missing_key_rows,
        order_accuracy_issues,
        orphan_rows,
        payment_reconciliation_issues,
        product_pricing_issues,
        order_delivery_issues,
        timestamp_parsing_issues,
    )

    datasets = {
        name: pd.read_parquet(OUTPUT_DIR / f"{name}.parquet")
        for name in ["customers", "products", "stores", "orders", "payments"]
    }
    keys = {
        "customers": "customer_id",
        "products": "product_id",
        "stores": "store_id",
        "orders": "order_id",
        "payments": "payment_id",
    }

    run_id = run_id or new_run_id()
    checked_at = datetime.now(ZoneInfo("Asia/Kolkata"))
    results = []

    def record(check_name, dimension, severity, checked, failed):
        results.append(
            make_dq_result(
                run_id=run_id,
                dag_id="nammamart_etl_pipeline",
                check_name=check_name,
                dimension=dimension,
                severity=severity,
                rows_checked=int(checked),
                rows_failed=int(failed),
                checked_at=checked_at,
            )
        )

    # Business-key completeness and uniqueness.
    for name, df in datasets.items():
        key = keys[name]
        record(
            f"{name}_business_key_completeness",
            "Completeness", "Critical", len(df),
            len(missing_key_rows(df, key)),
        )
        record(
            f"{name}_business_key_uniqueness",
            "Uniqueness", "Critical", len(df),
            len(duplicate_key_rows(df, key)),
        )

    # Customer contact validity and loyalty-tier consistency.
    customers = datasets["customers"]
    customer_source = read_source_csv("customers.csv", source_dir)
    customer_issues = customer_validity_issues(customer_source)

    # Contact checks use source values before masking; published curated data
    # contains masked values by design.
    email_values = customer_source["email"].astype("string").str.strip()
    missing_emails = email_values.isna() | email_values.eq("")
    record(
        "customer_email_completeness",
        "Completeness",
        "Warning",
        len(customer_source),
        int(missing_emails.sum()),
    )

    customer_checks = [
        ("email", "customer_email_format", "Validity"),
        ("phone", "customer_phone_format", "Validity"),
        ("pincode", "customer_pincode_format", "Validity"),
        ("loyalty_tier", "customer_loyalty_tier_casing", "Consistency"),
    ]

    for column, check_name, dimension in customer_checks:
        failed = customer_issues.loc[
            customer_issues["column"].eq(column), "row_key"
        ].nunique()

        record(
            check_name,
            dimension,
            "Warning",
            len(customer_source),
            failed,
        )
        # Timestamp parsing validity.
    customers = datasets["customers"]
    orders = datasets["orders"]
    now = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)
    order_timestamp_issues = timestamp_parsing_issues(
        orders, "order_ts", "order_id", "Critical"
    )
    record(
        "order_timestamp_parsing",
        "Validity",
        "Critical",
        len(orders),
        order_timestamp_issues["row_key"].nunique(),
    )

    signup_timestamp_issues = timestamp_parsing_issues(
        customers, "signup_date", "customer_id", "Warning"
    )
    # record(
    #     "customer_signup_date_parsing",
    #     "Validity",
    #     "Warning",
    #     len(customers),
    #     signup_timestamp_issues["row_key"].nunique(),
    # )
    signup_ts = pd.to_datetime(
    customers["signup_date"],
    errors="coerce",
    format="mixed",
)
    if signup_ts.dt.tz is not None:
     signup_ts = signup_ts.dt.tz_localize(None)
    record(
        "future_customer_signup_date",
        "Timeliness",
        "Critical",
        len(customers),
        int((signup_ts > now).sum()),
        )
    # Delivery-time completeness and accuracy.
    delivery_issues = order_delivery_issues(orders)

    delivery_checks = [
        (
            "delivered_order_delivery_time_completeness",
            "Completeness",
            "Delivered order must have a valid delivery time",
        ),
        (
            "delivery_time_under_120_minutes",
            "Accuracy",
            "Delivery time must be less than 120 minutes",
        ),
        (
            "delivery_time_greater_than_zero",
            "Accuracy",
            "Delivery time must be greater than zero",
        ),
    ]

    for check_name, dimension, problem in delivery_checks:
        failed = delivery_issues.loc[
            delivery_issues["problem"].eq(problem), "row_key"
        ].nunique()
        record(
            check_name,
            dimension,
            "Critical",
            len(orders),
            failed,
        )
    orders = datasets["orders"]

    # Order foreign-key consistency against curated dimensions.
    foreign_key_checks = [
        ("customer_id", "customers", "customer_id", "orders_customer_reference"),
        ("product_id", "products", "product_id", "orders_product_reference"),
        ("store_id", "stores", "store_id", "orders_store_reference"),
    ]

    for child_key, parent_name, parent_key, check_name in foreign_key_checks:
        missing = missing_key_rows(orders, child_key)
        orphans = orphan_rows(
            orders, child_key, datasets[parent_name], parent_key
        )
        failed_ids = set(missing["order_id"].dropna()) | set(
            orphans["order_id"].dropna()
        )
        record(
            check_name,
            "Consistency",
            "Critical",
            len(orders),
            len(failed_ids),
        )

    valid_statuses = {"DELIVERED", "CANCELLED", "RETURNED"}
    valid_modes = {"UPI", "Card", "Cash on Delivery", "Wallet"}
    status = orders["order_status"].astype("string").str.strip()
    mode = orders["payment_mode"].astype("string").str.strip()
    invalid = ~status.isin(valid_statuses) | ~mode.isin(valid_modes)
    record(
        "order_status_and_payment_mode_validity",
        "Validity", "Critical", len(orders), invalid.sum(),
    )

    order_issues = order_accuracy_issues(orders)
    record(
        "order_quantity_discount_and_amount_accuracy",
        "Accuracy", "Critical", len(orders),
        order_issues["row_key"].nunique(),
    )

    products = datasets["products"]
    product_issues = product_pricing_issues(products)
    record(
        "product_pricing_accuracy",
        "Accuracy", "Critical", len(products),
        product_issues["row_key"].nunique(),
    )


    # Reconcile curated payments against curated orders.
    # Quarantined source records remain available for auditing but must
    # not cause curated-data validation checks to fail.
    orders = datasets["orders"]
    payments = datasets["payments"]

    reconciliation = payment_reconciliation_issues(payments, orders)

    successful = (
        payments["payment_status"]
        .astype("string")
        .str.strip()
        .str.upper()
        .eq("SUCCESS")
    )
    successful_payments = payments.loc[successful].copy()

    order_keys = set(
        orders["order_id"].dropna().astype("string").str.strip()
    )
    payment_order_keys = (
        successful_payments["order_id"].astype("string").str.strip()
    )

    has_order_reference = (
        successful_payments["order_id"].notna()
        & payment_order_keys.ne("")
        & payment_order_keys.isin(order_keys)
    )

    reference_issues = successful_payments.loc[~has_order_reference]

    amount_issues = reconciliation.loc[
        reconciliation["problem"].str.startswith(
            "Successful payment amount", na=False
        )
    ]

    record(
        "successful_payment_amount_reconciliation",
        "Consistency",
        "Critical",
        int(has_order_reference.sum()),
        int(amount_issues["row_key"].nunique()),
    )
    record(
        "successful_payment_order_reference",
        "Consistency",
        "Critical",
        len(successful_payments),
        int(reference_issues["payment_id"].nunique()),
    )


    # Timeliness checks.
    now = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)
    order_ts = pd.to_datetime(orders["order_ts"], errors="coerce", format="mixed")
    if order_ts.dt.tz is not None:
        order_ts = order_ts.dt.tz_localize(None)
    record(
        "future_order_timestamp",
        "Timeliness", "Critical", len(orders), (order_ts > now).sum(),
    )


    latest = order_ts.max()
    is_stale = pd.notna(latest) and latest < now - pd.Timedelta(days=1)
    record(
        "latest_order_freshness",
        "Timeliness", "Warning", 1, int(is_stale),
    )

    frame = write_dq_results(results, str(OUTPUT_DIR / "dq_results.csv"))

    from include.nammamart.audit import persist_dq_results
    persist_dq_results(results)

    return frame


def build_quality_report() -> pd.DataFrame:
    """Build a summary of curated outputs and their quarantine counts."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    datasets = [
        "customers",
        "products",
        "stores",
        "orders",
        "payments",
    ]

    report_rows = []

    for name in datasets:
        curated_path = OUTPUT_DIR / f"{name}.parquet"
        quarantine_path = OUTPUT_DIR / f"{name}_quarantine.parquet"

        curated = pd.read_parquet(curated_path)
        quarantine = pd.read_parquet(quarantine_path)

        key = {
            "customers": "customer_id",
            "products": "product_id",
            "stores": "store_id",
            "orders": "order_id",
            "payments": "payment_id",
        }[name]

        report_rows.append(
            {
                "dataset": name,
                "curated_rows": len(curated),
                "quarantined_rows": len(quarantine),
                "total_processed_rows": len(curated) + len(quarantine),
                "business_key": key,
                "business_key_unique": curated[key].is_unique,
                "missing_business_keys": int(curated[key].isna().sum()),
                "curated_parquet": str(curated_path),
                "quarantine_parquet": str(quarantine_path),
            }
        )

    report = pd.DataFrame(report_rows)
    report.to_csv(OUTPUT_DIR / "quality_report.csv", index=False)
    report.to_parquet(
        OUTPUT_DIR / "quality_report.parquet",
        index=False,
        engine="pyarrow",
    )

    return report
