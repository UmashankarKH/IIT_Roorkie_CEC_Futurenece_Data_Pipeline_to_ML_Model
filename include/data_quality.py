"""
Reusable data-quality checks for the NammaMart ETL and ELT pipelines.

The helper functions in this module intentionally do not clean, change, or
write source data. They only evaluate a DataFrame and return two outputs:

1. A row-level issue log that can be sent to quarantine.
2. A check-level scorecard that can be written to the ``dq_results`` table.

Keeping validation outside the DAG makes the rules easier to test locally and
ensures that the ETL and ELT implementations use the same business rules.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


VALID_ORDER_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
VALID_LOYALTY_TIERS = {"Bronze", "Silver", "Gold", "Platinum"}

# The latest source export (orders_2026-10-01_increment.csv) was produced on
# 1 October 2026, so no order can be dated after the end of that day. Pinning
# the cut-off keeps results reproducible: comparing with "now" would let a
# planted future order (ORD001693, dated 21 Oct 2026) pass once that date is
# reached. In production this is the Airflow run's data_interval_end.
DATA_AS_OF = pd.Timestamp("2026-10-02 00:00:00")
FRESHNESS_LIMIT = pd.Timedelta(days=1)

ISSUE_COLUMNS = [
    "file",
    "row_key",
    "column",
    "problem",
    "quality_dimension",
    "proposed_rule",
    "severity",
]

DQ_COLUMNS = [
    "run_id",
    "dag_id",
    "check_name",
    "dimension",
    "severity",
    "rows_checked",
    "rows_failed",
    "status",
    "checked_at",
]


PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cod": "Cash on Delivery",
    "cashh": "Cash on Delivery",
    "cash on delivery": "Cash on Delivery",
    "wallet": "Wallet",
}


def standardise_payment_mode(values: pd.Series) -> pd.Series:
    """Trim and map payment-mode spellings exactly like the ELT staging SQL."""

    trimmed = values.astype("string").str.strip()
    return trimmed.str.lower().map(PAYMENT_MODE_MAP).fillna(trimmed)


def standardise_values(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Return copies with trimmed/cased categorical values (``upi`` -> ``UPI``).

    Validation runs on the standardised values so a casing problem is fixed
    rather than quarantined, matching ``stg_orders.sql`` and ``stg_payments.sql``.
    """

    result = dict(data)
    for name in ("orders", "orders_increment"):
        if name in result:
            frame = result[name].copy()
            frame["payment_mode"] = standardise_payment_mode(frame["payment_mode"])
            frame["order_status"] = frame["order_status"].astype("string").str.strip().str.upper()
            result[name] = frame
    if "customers" in result:
        frame = result["customers"].copy()
        frame["loyalty_tier"] = frame["loyalty_tier"].astype("string").str.strip().str.title()
        result["customers"] = frame
    if "payments" in result:
        frame = result["payments"].copy()
        frame["payment_mode"] = standardise_payment_mode(frame["payment_mode"])
        frame["payment_status"] = frame["payment_status"].astype("string").str.strip().str.upper()
        result["payments"] = frame
    return result


def _safe_mask(mask: Any, index: pd.Index) -> pd.Series:
    """Return a boolean mask aligned to the input DataFrame index."""

    # ``fillna(False)`` prevents missing values from being interpreted as
    # unknown boolean values when a check is applied to incomplete data.
    return pd.Series(mask, index=index).fillna(False).astype(bool)


def _record_check(
    checks: list[dict[str, Any]],
    *,
    run_id: str,
    dag_id: str,
    check_name: str,
    dimension: str,
    severity: str,
    rows_checked: int,
    rows_failed: int,
    checked_at: str,
) -> None:
    """Append one check-level result using the schema required by the assignment."""

    checks.append(
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "check_name": check_name,
            "dimension": dimension,
            "severity": severity,
            "rows_checked": rows_checked,
            "rows_failed": rows_failed,
            "status": "PASS" if rows_failed == 0 else "FAIL",
            "checked_at": checked_at,
        }
    )


def _check(
    *,
    frame: pd.DataFrame,
    file_name: str,
    row_key: str,
    column: str,
    mask: Any,
    problem: str,
    dimension: str,
    proposed_rule: str,
    severity: str,
    run_id: str,
    dag_id: str,
    checked_at: str,
    issues: list[dict[str, Any]],
    checks: list[dict[str, Any]],
) -> None:
    """Record both row-level failures and the aggregate scorecard result."""

    failed = _safe_mask(mask, frame.index)
    failed_indices = frame.index[failed]

    # Each failed row keeps its business key and reason so the ETL pipeline
    # can quarantine it without silently dropping the source record.
    for index in failed_indices:
        issues.append(
            {
                "file": file_name,
                "row_key": frame.loc[index, row_key],
                "column": column,
                "problem": problem,
                "quality_dimension": dimension,
                "proposed_rule": proposed_rule,
                "severity": severity,
            }
        )

    _record_check(
        checks,
        run_id=run_id,
        dag_id=dag_id,
        check_name=f"{file_name}:{column}:{problem}",
        dimension=dimension,
        severity=severity,
        rows_checked=len(frame),
        rows_failed=int(failed.sum()),
        checked_at=checked_at,
    )


def _prepare_result(issues: list[dict[str, Any]], checks: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return consistently shaped DataFrames, including when no rows fail."""

    issue_frame = pd.DataFrame(issues, columns=ISSUE_COLUMNS)
    check_frame = pd.DataFrame(checks, columns=DQ_COLUMNS)
    return issue_frame, check_frame


def run_quality_checks(
    data: dict[str, pd.DataFrame],
    *,
    run_id: str,
    dag_id: str,
    checked_at: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the shared NammaMart rules and return ``(issues, dq_results)``.

    Parameters
    ----------
    data:
        Mapping containing ``orders``, ``orders_increment``, ``payments``,
        ``customers``, ``products`` and ``stores`` DataFrames.
    run_id, dag_id:
        Identifiers stored with every scorecard row for auditability.
    checked_at:
        Optional ISO timestamp. UTC is used when it is omitted.
    """

    checked_at = checked_at or datetime.now(timezone.utc).isoformat()
    issues: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    # Casing is checked on the values as received; everything else is checked
    # after standardisation so a casing problem is logged, not quarantined.
    raw_customers = data["customers"].copy()
    data = standardise_values(data)

    orders = data["orders"].copy()
    customers = data["customers"].copy()
    products = data["products"].copy()
    stores = data["stores"].copy()
    payments = data["payments"].copy()

    # Parse timestamps once so both validation and downstream transformations
    # use the same interpretation of invalid or future dates.
    orders["_order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce")
    payments["_payment_ts"] = pd.to_datetime(payments["payment_ts"], errors="coerce")
    customers["_signup_date"] = pd.to_datetime(customers["signup_date"], errors="coerce")
    products["_launch_date"] = pd.to_datetime(products["launch_date"], errors="coerce")
    stores["_open_date"] = pd.to_datetime(stores["open_date"], errors="coerce")

    # ------------------------------ Orders ---------------------------------
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="customer_id", mask=orders["customer_id"].isna(), problem="Missing customer_id", dimension="Completeness", proposed_rule="customer_id must be present", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="order_id", mask=orders["order_id"].duplicated(keep=False), problem="Duplicate order_id", dimension="Uniqueness", proposed_rule="order_id must be unique", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="order_status", mask=~orders["order_status"].isin(VALID_ORDER_STATUSES), problem="Invalid order status", dimension="Validity", proposed_rule="status must be DELIVERED, CANCELLED or RETURNED", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="payment_mode", mask=~orders["payment_mode"].isin(VALID_PAYMENT_MODES), problem="Invalid payment mode", dimension="Validity", proposed_rule="payment mode must use the approved values", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="quantity", mask=orders["quantity"] <= 0, problem="Quantity is not positive", dimension="Accuracy", proposed_rule="quantity must be greater than zero", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    expected_amount = (orders["quantity"] * orders["unit_price"] * (1 - orders["discount_pct"] / 100)).round(2)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="order_amount", mask=(orders["order_amount"] - expected_amount).abs() > 0.01, problem="Amount does not match pricing formula", dimension="Accuracy", proposed_rule="order_amount must equal quantity × unit_price × discount", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="delivery_minutes", mask=(orders["order_status"] == "DELIVERED") & orders["delivery_minutes"].isna(), problem="Delivered order has no delivery time", dimension="Completeness", proposed_rule="DELIVERED orders require delivery_minutes", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="delivery_minutes", mask=orders["delivery_minutes"].fillna(0) > 120, problem="Delivery time exceeds 120 minutes", dimension="Accuracy", proposed_rule="delivery_minutes should be at most 120", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="discount_pct", mask=~orders["discount_pct"].between(0, 100), problem="Discount outside 0-100", dimension="Validity", proposed_rule="discount_pct must be between 0 and 100", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="order_ts", mask=orders["_order_ts"].isna(), problem="order_ts is not a valid timestamp", dimension="Validity", proposed_rule="order_ts must parse as a timestamp", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="order_ts", mask=orders["_order_ts"] > min(pd.Timestamp.now(), DATA_AS_OF), problem="Future-dated order", dimension="Timeliness", proposed_rule="order_ts must not be later than the source export (2026-10-01)", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="customer_id", mask=~orders["customer_id"].isin(set(customers["customer_id"])), problem="Unknown customer_id", dimension="Consistency", proposed_rule="orders.customer_id must exist in customers", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="product_id", mask=~orders["product_id"].isin(set(products["product_id"])), problem="Unknown product_id", dimension="Consistency", proposed_rule="orders.product_id must exist in products", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=orders, file_name="orders.csv", row_key="order_id", column="store_id", mask=~orders["store_id"].isin(set(stores["store_id"])), problem="Unknown store_id", dimension="Consistency", proposed_rule="orders.store_id must exist in stores", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)

    # Dataset-level freshness: one Warning row in the scorecard (no row is
    # quarantined) when the newest order is more than a day old at run time.
    cutoff = min(pd.Timestamp.now(), DATA_AS_OF)
    latest_order = orders.loc[orders["_order_ts"] <= cutoff, "_order_ts"].max()
    stale = pd.isna(latest_order) or pd.Timestamp.now() - latest_order > FRESHNESS_LIMIT
    _record_check(checks, run_id=run_id, dag_id=dag_id, check_name="orders.csv:order_ts:Latest order older than 1 day", dimension="Timeliness", severity="Warning", rows_checked=len(orders), rows_failed=int(stale), checked_at=checked_at)

    # ----------------------------- Payments --------------------------------
    _check(frame=payments, file_name="payments.csv", row_key="payment_id", column="payment_id", mask=payments["payment_id"].duplicated(keep=False), problem="Duplicate payment_id", dimension="Uniqueness", proposed_rule="payment_id must be unique", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=payments, file_name="payments.csv", row_key="payment_id", column="gateway_ref", mask=payments["gateway_ref"].isna(), problem="Missing gateway reference", dimension="Completeness", proposed_rule="gateway_ref is required for payment auditability", severity="Warning", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=payments, file_name="payments.csv", row_key="payment_id", column="amount_paid", mask=payments["amount_paid"] <= 0, problem="Payment amount is not positive", dimension="Accuracy", proposed_rule="amount_paid must be greater than zero", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=payments, file_name="payments.csv", row_key="payment_id", column="order_id", mask=~payments["order_id"].isin(set(orders["order_id"])), problem="Unknown order_id", dimension="Consistency", proposed_rule="payments.order_id must exist in orders", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    payment_check = payments.merge(orders[["order_id", "order_amount"]], on="order_id", how="left")
    payment_mismatch = (payment_check["payment_status"] == "SUCCESS") & payment_check["order_amount"].notna() & ((payment_check["amount_paid"] - payment_check["order_amount"]).abs() > 0.01)
    _check(frame=payment_check, file_name="payments.csv", row_key="payment_id", column="amount_paid", mask=payment_mismatch, problem="Successful payment does not equal order amount", dimension="Consistency", proposed_rule="SUCCESS payment must equal order_amount", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)

    # ----------------------------- Customers --------------------------------
    _check(frame=customers, file_name="customers.csv", row_key="customer_id", column="customer_id", mask=customers["customer_id"].duplicated(keep=False), problem="Duplicate customer_id", dimension="Uniqueness", proposed_rule="customer_id must be unique", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=customers, file_name="customers.csv", row_key="customer_id", column="email", mask=~customers["email"].fillna("").str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), problem="Invalid email format", dimension="Validity", proposed_rule="email should follow a valid email format", severity="Warning", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=customers, file_name="customers.csv", row_key="customer_id", column="phone", mask=~customers["phone"].fillna("").astype(str).str.fullmatch(r"\d{10}"), problem="Phone is not a 10-digit number", dimension="Validity", proposed_rule="phone should contain exactly 10 digits", severity="Warning", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=raw_customers, file_name="customers.csv", row_key="customer_id", column="loyalty_tier", mask=~raw_customers["loyalty_tier"].isin(VALID_LOYALTY_TIERS), problem="Loyalty tier casing is not standard", dimension="Consistency", proposed_rule="loyalty_tier must be Bronze, Silver, Gold or Platinum (one casing)", severity="Warning", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=customers, file_name="customers.csv", row_key="customer_id", column="signup_date", mask=customers["_signup_date"] > min(pd.Timestamp.now(), DATA_AS_OF), problem="Future signup_date", dimension="Timeliness", proposed_rule="signup_date must not be later than the source export", severity="Warning", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=customers, file_name="customers.csv", row_key="customer_id", column="pincode", mask=~customers["pincode"].fillna("").astype(str).str.fullmatch(r"\d{6}"), problem="Pincode is not six digits", dimension="Validity", proposed_rule="pincode should contain exactly 6 digits", severity="Warning", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)

    # ------------------------------ Products --------------------------------
    _check(frame=products, file_name="products.csv", row_key="product_id", column="product_id", mask=products["product_id"].duplicated(keep=False), problem="Duplicate product_id", dimension="Uniqueness", proposed_rule="product_id must be unique", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=products, file_name="products.csv", row_key="product_id", column="category", mask=products["category"].isna(), problem="Missing category", dimension="Completeness", proposed_rule="category is required for category reporting", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=products, file_name="products.csv", row_key="product_id", column="mrp", mask=products["mrp"] <= 0, problem="MRP is not positive", dimension="Accuracy", proposed_rule="mrp must be greater than zero", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=products, file_name="products.csv", row_key="product_id", column="cost_price", mask=products["cost_price"] > products["mrp"], problem="Cost price exceeds MRP", dimension="Accuracy", proposed_rule="cost_price must not exceed mrp", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)

    # -------------------------------- Stores --------------------------------
    _check(frame=stores, file_name="stores.csv", row_key="store_id", column="store_id", mask=stores["store_id"].duplicated(keep=False), problem="Duplicate store_id", dimension="Uniqueness", proposed_rule="store_id must be unique", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=stores, file_name="stores.csv", row_key="store_id", column="zone", mask=stores["zone"].isna(), problem="Missing zone", dimension="Completeness", proposed_rule="zone is required for zone reporting", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=stores, file_name="stores.csv", row_key="store_id", column="pincode", mask=~stores["pincode"].fillna("").astype(str).str.fullmatch(r"\d{6}"), problem="Pincode is not six digits", dimension="Validity", proposed_rule="pincode should contain exactly 6 digits", severity="Warning", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=stores, file_name="stores.csv", row_key="store_id", column="capacity_orders_per_day", mask=stores["capacity_orders_per_day"] <= 0, problem="Store capacity is not positive", dimension="Accuracy", proposed_rule="capacity must be greater than zero", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)

    # Store capacity (bonus): orders per store per day must not exceed the
    # store's declared daily capacity. Stores whose capacity is itself invalid
    # are excluded here because "Store capacity is not positive" flags them.
    dated = orders[orders["_order_ts"].notna()].assign(order_day=orders["_order_ts"].dt.date)
    store_days = dated.groupby(["store_id", "order_day"]).size().rename("daily_orders").reset_index()
    capacity = stores.drop_duplicates("store_id")[["store_id", "capacity_orders_per_day"]]
    store_days = store_days.merge(capacity, on="store_id", how="inner")
    store_days = store_days[store_days["capacity_orders_per_day"] > 0].reset_index(drop=True)
    store_days["store_day"] = store_days["store_id"] + "@" + store_days["order_day"].astype(str)
    _check(frame=store_days, file_name="stores.csv", row_key="store_day", column="capacity_orders_per_day", mask=store_days["daily_orders"] > store_days["capacity_orders_per_day"], problem="Daily orders exceed store capacity", dimension="Accuracy", proposed_rule="orders per store per day must not exceed capacity_orders_per_day", severity="Warning", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)

    # The increment feed is checked for the rules that determine whether a
    # corrected or newly arrived order can safely enter the warehouse.
    increment = data["orders_increment"].copy()
    increment["_order_ts"] = pd.to_datetime(increment["order_ts"], errors="coerce")
    increment_amount = (increment["quantity"] * increment["unit_price"] * (1 - increment["discount_pct"] / 100)).round(2)
    _check(frame=increment, file_name="orders_2026-10-01_increment.csv", row_key="order_id", column="order_amount", mask=(increment["order_amount"] - increment_amount).abs() > 0.01, problem="Amount does not match pricing formula", dimension="Accuracy", proposed_rule="increment order_amount must equal the pricing formula", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)
    _check(frame=increment, file_name="orders_2026-10-01_increment.csv", row_key="order_id", column="order_status", mask=~increment["order_status"].isin(VALID_ORDER_STATUSES), problem="Invalid order status", dimension="Validity", proposed_rule="increment status must use the approved values", severity="Critical", run_id=run_id, dag_id=dag_id, checked_at=checked_at, issues=issues, checks=checks)

    return _prepare_result(issues, checks)
