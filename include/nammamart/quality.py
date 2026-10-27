"""Part D: the data quality rules, in Python, for the ETL pipeline.

Every rule names its dimension and a severity:
  Critical -> the row is quarantined and never reaches a reporting table
  Warning  -> the row loads, but the breach is recorded in dq_results

The same rule set is expressed again in SQL for the ELT pipeline
(include/sql/staging/*.sql), so both pipelines agree on what "clean" means.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

import pandas as pd

from . import config as cfg

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")


@dataclass
class Check:
    """One quality rule. `mask` returns True for rows that FAIL the rule."""
    name: str
    dimension: str
    severity: str           # Critical | Warning
    mask: Callable[[pd.DataFrame], pd.Series]
    reason: str


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", format="mixed")


def _blank(series: pd.Series) -> pd.Series:
    return series.isna() | (series.astype(str).str.strip() == "")


def _window_end() -> pd.Timestamp:
    return pd.Timestamp(cfg.DATA_WINDOW_END)


# --------------------------------------------------------------- rule tables

def order_checks() -> list[Check]:
    def amount_mismatch(df: pd.DataFrame) -> pd.Series:
        expected = (_num(df["quantity"]) * _num(df["unit_price"])
                    * (1 - _num(df["discount_pct"]) / 100)).round(2)
        actual = _num(df["order_amount"])
        return expected.notna() & actual.notna() & ((actual - expected).abs() > cfg.AMOUNT_TOLERANCE)

    return [
        Check("orders_order_id_present", "Completeness", "Critical",
              lambda df: _blank(df["order_id"]), "order_id is missing"),
        Check("orders_customer_id_present", "Completeness", "Critical",
              lambda df: _blank(df["customer_id"]), "customer_id is missing"),
        Check("orders_delivered_has_delivery_time", "Completeness", "Critical",
              lambda df: (df["order_status"].astype(str).str.upper() == "DELIVERED")
              & _blank(df["delivery_minutes"]),
              "DELIVERED order has no delivery_minutes"),
        Check("orders_order_id_unique", "Uniqueness", "Critical",
              lambda df: df.duplicated("order_id", keep="last") & ~_blank(df["order_id"]),
              "duplicate order_id, later row kept"),
        Check("orders_status_allowed", "Validity", "Critical",
              lambda df: ~df["order_status"].astype(str).str.strip().str.upper()
              .isin([s.upper() for s in cfg.VALID_ORDER_STATUS]),
              f"order_status not in {list(cfg.VALID_ORDER_STATUS)}"),
        Check("orders_timestamp_parses", "Validity", "Critical",
              lambda df: _ts(df["order_ts"]).isna(), "order_ts does not parse"),
        Check("orders_discount_in_range", "Validity", "Critical",
              lambda df: _num(df["discount_pct"]).notna()
              & ~_num(df["discount_pct"]).between(0, 100),
              "discount_pct outside 0-100"),
        Check("orders_payment_mode_mappable", "Validity", "Critical",
              lambda df: ~df["payment_mode"].astype(str).str.strip().str.lower()
              .isin(list(cfg.PAYMENT_MODE_MAP)), "payment_mode cannot be mapped"),
        Check("orders_quantity_positive", "Accuracy", "Critical",
              lambda df: _num(df["quantity"]).isna() | (_num(df["quantity"]) <= 0),
              "quantity must be greater than zero"),
        Check("orders_amount_matches_formula", "Accuracy", "Critical", amount_mismatch,
              "order_amount does not match quantity x unit_price x (1 - discount)"),
        Check("orders_delivery_minutes_believable", "Accuracy", "Critical",
              lambda df: _num(df["delivery_minutes"]).notna()
              & (_num(df["delivery_minutes"]) > cfg.DELIVERY_MAX_MINUTES),
              f"delivery_minutes above {cfg.DELIVERY_MAX_MINUTES}"),
        Check("orders_not_future_dated", "Timeliness", "Critical",
              lambda df: _ts(df["order_ts"]).notna() & (_ts(df["order_ts"]) > _window_end()),
              "order_ts is in the future"),
        Check("orders_payment_mode_casing", "Consistency", "Warning",
              lambda df: df["payment_mode"].notna()
              & ~df["payment_mode"].isin(cfg.VALID_PAYMENT_MODE)
              & df["payment_mode"].astype(str).str.strip().str.lower().isin(list(cfg.PAYMENT_MODE_MAP)),
              "payment_mode needed standardising"),
        Check("orders_delivery_time_within_band", "Accuracy", "Warning",
              lambda df: _num(df["delivery_minutes"]).notna()
              & (_num(df["delivery_minutes"]) > 35)
              & (_num(df["delivery_minutes"]) <= cfg.DELIVERY_MAX_MINUTES),
              "delivery took longer than the normal 8-35 minute band"),
    ]


def payment_checks() -> list[Check]:
    return [
        Check("payments_payment_id_present", "Completeness", "Critical",
              lambda df: _blank(df["payment_id"]), "payment_id is missing"),
        Check("payments_payment_id_unique", "Uniqueness", "Critical",
              lambda df: df.duplicated("payment_id", keep="last") & ~_blank(df["payment_id"]),
              "duplicate payment_id, later row kept"),
        Check("payments_order_id_present", "Completeness", "Critical",
              lambda df: _blank(df["order_id"]), "payment has no order_id"),
        Check("payments_timestamp_parses", "Validity", "Critical",
              lambda df: _ts(df["payment_ts"]).isna(), "payment_ts does not parse"),
        Check("payments_amount_not_negative", "Accuracy", "Critical",
              lambda df: _num(df["amount_paid"]).isna() | (_num(df["amount_paid"]) < 0),
              "amount_paid is negative or unreadable"),
        Check("payments_gateway_ref_present", "Completeness", "Warning",
              lambda df: _blank(df["gateway_ref"]), "no gateway reference recorded"),
    ]


def customer_checks() -> list[Check]:
    return [
        Check("customers_customer_id_present", "Completeness", "Critical",
              lambda df: _blank(df["customer_id"]), "customer_id is missing"),
        Check("customers_customer_id_unique", "Uniqueness", "Critical",
              lambda df: df.duplicated("customer_id", keep="last") & ~_blank(df["customer_id"]),
              "duplicate customer_id, later row kept"),
        Check("customers_signup_not_future", "Timeliness", "Critical",
              lambda df: _ts(df["signup_date"]).notna() & (_ts(df["signup_date"]) > _window_end()),
              "signup_date is in the future"),
        Check("customers_email_format", "Validity", "Warning",
              lambda df: ~df["email"].fillna("").str.match(EMAIL_RE),
              "email is not a valid address"),
        Check("customers_phone_ten_digits", "Validity", "Warning",
              lambda df: df["phone"].fillna("").str.replace(r"\D", "", regex=True).str.len() != 10,
              "phone is not 10 digits"),
        Check("customers_pincode_six_digits", "Validity", "Warning",
              lambda df: df["pincode"].fillna("").str.replace(r"\D", "", regex=True).str.len() != 6,
              "pincode is not 6 digits"),
        Check("customers_loyalty_tier_casing", "Consistency", "Warning",
              lambda df: df["loyalty_tier"].notna()
              & ~df["loyalty_tier"].isin([t.title() for t in cfg.LOYALTY_TIERS]),
              "loyalty_tier casing needed standardising"),
        Check("customers_city_present", "Completeness", "Warning",
              lambda df: _blank(df["city"]), "city is missing"),
    ]


def product_checks() -> list[Check]:
    return [
        Check("products_product_id_present", "Completeness", "Critical",
              lambda df: _blank(df["product_id"]), "product_id is missing"),
        Check("products_product_id_unique", "Uniqueness", "Critical",
              lambda df: df.duplicated("product_id", keep="last") & ~_blank(df["product_id"]),
              "duplicate product_id, later row kept"),
        Check("products_mrp_positive", "Accuracy", "Critical",
              lambda df: _num(df["mrp"]).isna() | (_num(df["mrp"]) <= 0),
              "mrp must be greater than zero"),
        Check("products_cost_not_above_mrp", "Accuracy", "Critical",
              lambda df: _num(df["cost_price"]).notna() & _num(df["mrp"]).notna()
              & (_num(df["cost_price"]) > _num(df["mrp"])),
              "cost_price is higher than mrp"),
        Check("products_category_present", "Completeness", "Warning",
              lambda df: _blank(df["category"]), "category is missing"),
    ]


def store_checks() -> list[Check]:
    return [
        Check("stores_store_id_present", "Completeness", "Critical",
              lambda df: _blank(df["store_id"]), "store_id is missing"),
        Check("stores_store_id_unique", "Uniqueness", "Critical",
              lambda df: df.duplicated("store_id", keep="last") & ~_blank(df["store_id"]),
              "duplicate store_id, later row kept"),
        Check("stores_capacity_positive", "Accuracy", "Warning",
              lambda df: _num(df["capacity_orders_per_day"]).isna()
              | (_num(df["capacity_orders_per_day"]) <= 0),
              "capacity_orders_per_day is zero or missing"),
        Check("stores_zone_present", "Completeness", "Warning",
              lambda df: _blank(df["zone"]), "zone is missing"),
    ]


CHECKS = {
    "orders": order_checks,
    "payments": payment_checks,
    "customers": customer_checks,
    "products": product_checks,
    "stores": store_checks,
}


# ------------------------------------------------------------------- runner

def run_checks(dataset: str, df: pd.DataFrame, run_id: str, dag_id: str
               ) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    """Split a dataframe into passed and failed rows and record every check.

    Returns (passed, failed_with_reasons, dq_results_rows).
    """
    checks = CHECKS[dataset]()
    results: list[dict] = []
    reasons = pd.Series([""] * len(df), index=df.index)
    critical_fail = pd.Series(False, index=df.index)
    checked_at = datetime.utcnow().isoformat(timespec="seconds")

    for check in checks:
        try:
            failed = check.mask(df).fillna(False)
        except KeyError as exc:
            # A missing column is itself a finding: record it rather than crashing the task.
            results.append({
                "run_id": run_id, "dag_id": dag_id, "check_name": check.name,
                "dimension": check.dimension, "severity": check.severity,
                "rows_checked": len(df), "rows_failed": -1, "status": "ERROR",
                "checked_at": checked_at, "detail": f"missing column {exc}",
            })
            continue

        count = int(failed.sum())
        results.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": check.name,
            "dimension": check.dimension, "severity": check.severity,
            "rows_checked": len(df), "rows_failed": count,
            "status": "PASS" if count == 0 else ("FAIL" if check.severity == "Critical" else "WARN"),
            "checked_at": checked_at, "detail": check.reason if count else "",
        })

        if count:
            tag = f"{check.severity}: {check.reason}"
            reasons.loc[failed] = reasons.loc[failed].where(
                reasons.loc[failed] == "", reasons.loc[failed] + " | ") + tag
            if check.severity == "Critical":
                critical_fail |= failed

    passed = df.loc[~critical_fail].copy()
    failed_rows = df.loc[critical_fail].copy()
    if len(failed_rows):
        failed_rows["failure_reason"] = reasons.loc[critical_fail]
        failed_rows["quarantined_at"] = checked_at
        failed_rows["run_id"] = run_id
    else:
        failed_rows = failed_rows.assign(failure_reason=[], quarantined_at=[], run_id=[])

    # Warnings that did not quarantine the row are still worth carrying forward.
    warn_only = reasons.loc[~critical_fail]
    passed["dq_warnings"] = warn_only.reindex(passed.index).fillna("")
    return passed, failed_rows, results


def referential_checks(orders: pd.DataFrame, customers: pd.DataFrame,
                       products: pd.DataFrame, stores: pd.DataFrame,
                       run_id: str, dag_id: str) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    """Consistency across files: an order must point at keys that exist.

    The key sets come from the FULL master files, not just the rows that passed
    quality. A product with a bad cost price is still a real product, so its
    orders keep their revenue; only orders pointing at a key that exists nowhere
    are quarantined. Without this, quarantining 6 products would silently delete
    about 100 orders worth of revenue.
    """
    checked_at = datetime.utcnow().isoformat(timespec="seconds")
    results: list[dict] = []
    reasons = pd.Series([""] * len(orders), index=orders.index)
    bad = pd.Series(False, index=orders.index)

    for column, parent, parent_key, label in (
        ("customer_id", customers, "customer_id", "customers"),
        ("product_id", products, "product_id", "products"),
        ("store_id", stores, "store_id", "stores"),
    ):
        known = set(parent[parent_key].dropna())
        failed = orders[column].notna() & ~orders[column].isin(known)
        count = int(failed.sum())
        results.append({
            "run_id": run_id, "dag_id": dag_id,
            "check_name": f"orders_{column}_exists_in_{label}",
            "dimension": "Consistency", "severity": "Critical",
            "rows_checked": len(orders), "rows_failed": count,
            "status": "PASS" if count == 0 else "FAIL", "checked_at": checked_at,
            "detail": f"{column} not found in {label}" if count else "",
        })
        if count:
            tag = f"Critical: {column} does not exist in {label}"
            reasons.loc[failed] = reasons.loc[failed].where(
                reasons.loc[failed] == "", reasons.loc[failed] + " | ") + tag
            bad |= failed

    passed = orders.loc[~bad].copy()
    failed_rows = orders.loc[bad].copy()
    if len(failed_rows):
        failed_rows["failure_reason"] = reasons.loc[bad]
        failed_rows["quarantined_at"] = checked_at
        failed_rows["run_id"] = run_id
    return passed, failed_rows, results


def volume_and_freshness_checks(orders: pd.DataFrame, run_id: str, dag_id: str,
                                expected_rows: int | None = None) -> list[dict]:
    """Observability checks that look at the batch as a whole, not row by row.

    expected_rows is the previous successful run's loaded row count, read from
    pipeline_runs. Comparing against the last run is the only baseline that means
    anything; my first attempt compared the fact row count against the total rows
    read across all five files, which drops by about half on a perfectly healthy run.
    On the very first run there is no baseline, so the check is skipped rather than
    reported as a pass it did not earn.
    """
    checked_at = datetime.utcnow().isoformat(timespec="seconds")
    results: list[dict] = []

    if expected_rows:
        drop = 1 - (len(orders) / expected_rows)
        breached = drop > 0.9
        status, detail = (("WARN", f"{len(orders)} rows vs {expected_rows} last run "
                                   f"({drop:.0%} drop)") if breached
                          else ("PASS", f"{len(orders)} rows vs {expected_rows} last run"))
    else:
        # Reported rather than silently skipped: the ELT side does the same, and a
        # missing row looks identical to a check nobody ever wrote.
        status, detail, breached = "SKIP", "no previous run to compare against", False

    results.append({
        "run_id": run_id, "dag_id": dag_id, "check_name": "orders_volume_not_collapsed",
        "dimension": "Volume", "severity": "Warning",
        "rows_checked": len(orders), "rows_failed": 1 if breached else 0,
        "status": status, "checked_at": checked_at, "detail": detail,
    })

    latest = _ts(orders["order_ts"]).max()
    # Calendar days, not elapsed hours. Comparing timestamps made this check return 1
    # where the SQL version returned 2 for the same data, because the newest order is
    # at 23:50 on 30 September. Two implementations of one rule have to agree.
    stale_days = ((_window_end().date() - latest.date()).days
                  if pd.notna(latest) else 999)
    results.append({
        "run_id": run_id, "dag_id": dag_id, "check_name": "orders_freshness",
        "dimension": "Timeliness", "severity": "Warning",
        "rows_checked": len(orders), "rows_failed": 1 if stale_days > 1 else 0,
        "status": "WARN" if stale_days > 1 else "PASS", "checked_at": checked_at,
        "detail": f"latest order is {stale_days} days old" if stale_days > 1 else "",
    })
    return results
