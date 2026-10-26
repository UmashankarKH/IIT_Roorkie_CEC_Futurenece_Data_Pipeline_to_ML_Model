"""Data quality validation for NammaMart ETL (pandas)."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd

from nammamart_config import VALID_ORDER_STATUS, VALID_PAYMENT_MODES
from nammamart_transforms import (
    expected_order_amount,
    is_valid_email,
    is_valid_phone,
    is_valid_pincode,
    parse_float,
    parse_timestamp,
    standardize_loyalty_tier,
    standardize_order_status,
    standardize_payment_mode,
)


def _dq_record(
    check_name: str,
    dimension: str,
    severity: str,
    rows_checked: int,
    rows_failed: int,
) -> dict:
    status = "PASS" if rows_failed == 0 else "FAIL"
    return {
        "check_name": check_name,
        "dimension": dimension,
        "severity": severity,
        "rows_checked": rows_checked,
        "rows_failed": rows_failed,
        "status": status,
        "checked_at": datetime.utcnow(),
    }


def _split_pass_fail(df: pd.DataFrame, reasons: pd.Series) -> Tuple[pd.DataFrame, pd.DataFrame]:
    failed_mask = reasons.str.len() > 0
    passed = df.loc[~failed_mask].copy()
    failed = df.loc[failed_mask].copy()
    failed["failure_reason"] = reasons.loc[failed_mask]
    return passed, failed


def validate_stores(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    work = df.copy()
    reasons = pd.Series([""] * len(work), index=work.index, dtype="object")
    records: List[dict] = []

    dup_count = work.duplicated(subset=["store_id"], keep="first").sum()
    records.append(_dq_record("store_id_unique", "Uniqueness", "Critical", len(work), int(dup_count)))
    dup_mask = work.duplicated(subset=["store_id"], keep="first")
    reasons.loc[dup_mask] = reasons.loc[dup_mask].apply(
        lambda x: x + ("; " if x else "") + "duplicate store_id"
    )

    blank_zone = work["zone"].astype(str).str.strip() == ""
    records.append(_dq_record("store_zone_present", "Completeness", "Warning", len(work), int(blank_zone.sum())))
    work.loc[blank_zone, "zone"] = work.loc[blank_zone, "area"]

    bad_cap = work["capacity_orders_per_day"].apply(parse_float).fillna(-1) < 0
    records.append(_dq_record("store_capacity_non_negative", "Accuracy", "Warning", len(work), int(bad_cap.sum())))

    return *_split_pass_fail(work, reasons), records


def validate_products(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    work = df.copy()
    reasons = pd.Series([""] * len(work), index=work.index, dtype="object")
    records: List[dict] = []

    dup_mask = work.duplicated(subset=["product_id"], keep="first")
    records.append(_dq_record("product_id_unique", "Uniqueness", "Critical", len(work), int(dup_mask.sum())))
    reasons.loc[dup_mask] = "duplicate product_id"

    null_cat = work["category"].astype(str).str.strip() == ""
    records.append(_dq_record("product_category_present", "Completeness", "Critical", len(work), int(null_cat.sum())))
    reasons.loc[null_cat] = reasons.loc[null_cat].apply(
        lambda x: x + ("; " if x else "") + "missing category"
    )

    mrp = work["mrp"].apply(parse_float)
    bad_mrp = mrp.isna() | (mrp <= 0)
    records.append(_dq_record("product_mrp_positive", "Accuracy", "Critical", len(work), int(bad_mrp.sum())))
    reasons.loc[bad_mrp] = reasons.loc[bad_mrp].apply(
        lambda x: x + ("; " if x else "") + "invalid mrp"
    )

    cost = work["cost_price"].apply(parse_float)
    cost_gt = cost.notna() & mrp.notna() & (cost > mrp)
    records.append(_dq_record("product_cost_le_mrp", "Accuracy", "Critical", len(work), int(cost_gt.sum())))
    reasons.loc[cost_gt] = reasons.loc[cost_gt].apply(
        lambda x: x + ("; " if x else "") + "cost_price exceeds mrp"
    )

    work = work.loc[~dup_mask].copy()
    reasons = reasons.loc[work.index]
    return *_split_pass_fail(work, reasons), records


def validate_customers(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    work = df.copy()
    work["loyalty_tier"] = work["loyalty_tier"].apply(standardize_loyalty_tier)
    reasons = pd.Series([""] * len(work), index=work.index, dtype="object")
    records: List[dict] = []

    dup_mask = work.duplicated(subset=["customer_id"], keep="first")
    records.append(_dq_record("customer_id_unique", "Uniqueness", "Critical", len(work), int(dup_mask.sum())))
    reasons.loc[dup_mask] = "duplicate customer_id"

    bad_email = ~work["email"].apply(is_valid_email)
    records.append(_dq_record("customer_email_format", "Validity", "Warning", len(work), int(bad_email.sum())))

    bad_phone = ~work["phone"].apply(is_valid_phone)
    records.append(_dq_record("customer_phone_format", "Validity", "Warning", len(work), int(bad_phone.sum())))

    bad_pin = ~work["pincode"].apply(is_valid_pincode)
    records.append(_dq_record("customer_pincode_format", "Validity", "Warning", len(work), int(bad_pin.sum())))

    future_signup = work["signup_date"].apply(parse_timestamp).apply(
        lambda dt: dt is not None and dt > datetime(2026, 9, 30, 23, 59, 59)
    )
    records.append(_dq_record("customer_signup_not_future", "Timeliness", "Critical", len(work), int(future_signup.sum())))
    reasons.loc[future_signup] = reasons.loc[future_signup].apply(
        lambda x: x + ("; " if x else "") + "future signup_date"
    )

    work = work.loc[~dup_mask].copy()
    reasons = reasons.loc[work.index]
    return *_split_pass_fail(work, reasons), records


def validate_payments(
    df: pd.DataFrame,
    order_ids: set,
    order_amounts: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    work = df.copy()
    work["payment_mode"] = work["payment_mode"].apply(standardize_payment_mode)
    reasons = pd.Series([""] * len(work), index=work.index, dtype="object")
    records: List[dict] = []

    dup_mask = work.duplicated(subset=["payment_id"], keep="first")
    records.append(_dq_record("payment_id_unique", "Uniqueness", "Critical", len(work), int(dup_mask.sum())))
    reasons.loc[dup_mask] = "duplicate payment_id"

    missing_order = ~work["order_id"].isin(order_ids)
    records.append(_dq_record("payment_order_exists", "Consistency", "Critical", len(work), int(missing_order.sum())))
    reasons.loc[missing_order] = reasons.loc[missing_order].apply(
        lambda x: x + ("; " if x else "") + "unknown order_id"
    )

    bad_mode = ~work["payment_mode"].isin(VALID_PAYMENT_MODES)
    records.append(_dq_record("payment_mode_valid", "Validity", "Critical", len(work), int(bad_mode.sum())))
    reasons.loc[bad_mode] = reasons.loc[bad_mode].apply(
        lambda x: x + ("; " if x else "") + "invalid payment_mode"
    )

    def amount_mismatch(row) -> bool:
        if str(row["payment_status"]).upper() != "SUCCESS":
            return False
        oid = row["order_id"]
        paid = parse_float(row["amount_paid"])
        expected = order_amounts.get(oid)
        if paid is None or expected is None:
            return False
        return abs(paid - expected) > 0.02

    mismatch = work.apply(amount_mismatch, axis=1)
    records.append(_dq_record("payment_amount_matches_order", "Consistency", "Warning", len(work), int(mismatch.sum())))

    work = work.loc[~dup_mask].copy()
    reasons = reasons.loc[work.index]
    return *_split_pass_fail(work, reasons), records


def validate_orders(
    df: pd.DataFrame,
    customer_ids: set,
    product_ids: set,
    store_ids: set,
    as_of: datetime | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    work = df.copy()
    work["payment_mode"] = work["payment_mode"].apply(standardize_payment_mode)
    work["order_status"] = work["order_status"].apply(standardize_order_status)
    reasons = pd.Series([""] * len(work), index=work.index, dtype="object")
    records: List[dict] = []
    as_of = as_of or datetime(2026, 9, 30, 23, 59, 59)

    dup_mask = work.duplicated(subset=["order_id"], keep="first")
    records.append(_dq_record("order_id_unique", "Uniqueness", "Critical", len(work), int(dup_mask.sum())))
    reasons.loc[dup_mask] = "duplicate order_id"

    null_customer = work["customer_id"].astype(str).str.strip() == ""
    records.append(_dq_record("order_customer_present", "Completeness", "Critical", len(work), int(null_customer.sum())))
    reasons.loc[null_customer] = reasons.loc[null_customer].apply(
        lambda x: x + ("; " if x else "") + "missing customer_id"
    )

    bad_status = ~work["order_status"].isin(VALID_ORDER_STATUS)
    records.append(_dq_record("order_status_valid", "Validity", "Critical", len(work), int(bad_status.sum())))
    reasons.loc[bad_status] = reasons.loc[bad_status].apply(
        lambda x: x + ("; " if x else "") + "invalid order_status"
    )

    bad_mode = ~work["payment_mode"].isin(VALID_PAYMENT_MODES)
    records.append(_dq_record("order_payment_mode_valid", "Validity", "Critical", len(work), int(bad_mode.sum())))
    reasons.loc[bad_mode] = reasons.loc[bad_mode].apply(
        lambda x: x + ("; " if x else "") + "invalid payment_mode"
    )

    qty = work["quantity"].apply(parse_float)
    bad_qty = qty.isna() | (qty <= 0)
    records.append(_dq_record("order_quantity_positive", "Accuracy", "Critical", len(work), int(bad_qty.sum())))
    reasons.loc[bad_qty] = reasons.loc[bad_qty].apply(
        lambda x: x + ("; " if x else "") + "quantity must be > 0"
    )

    disc = work["discount_pct"].apply(parse_float)
    bad_disc = disc.isna() | (disc < 0) | (disc > 100)
    records.append(_dq_record("order_discount_range", "Validity", "Critical", len(work), int(bad_disc.sum())))
    reasons.loc[bad_disc] = reasons.loc[bad_disc].apply(
        lambda x: x + ("; " if x else "") + "discount_pct out of range"
    )

    unit_price = work["unit_price"].apply(parse_float)
    order_amount = work["order_amount"].apply(parse_float)
    expected = [
        expected_order_amount(q or 0, u or 0, d or 0)
        for q, u, d in zip(qty.fillna(0), unit_price.fillna(0), disc.fillna(0))
    ]
    amount_bad = [
        o is None or abs(o - e) > 0.02 for o, e in zip(order_amount, expected)
    ]
    records.append(_dq_record("order_amount_formula", "Accuracy", "Critical", len(work), int(sum(amount_bad))))
    for idx, bad in zip(work.index, amount_bad):
        if bad:
            reasons.at[idx] = reasons.at[idx] + ("; " if reasons.at[idx] else "") + "order_amount mismatch"

    missing_product = ~work["product_id"].isin(product_ids)
    records.append(_dq_record("order_product_exists", "Consistency", "Critical", len(work), int(missing_product.sum())))
    reasons.loc[missing_product] = reasons.loc[missing_product].apply(
        lambda x: x + ("; " if x else "") + "unknown product_id"
    )

    missing_store = ~work["store_id"].isin(store_ids)
    records.append(_dq_record("order_store_exists", "Consistency", "Critical", len(work), int(missing_store.sum())))
    reasons.loc[missing_store] = reasons.loc[missing_store].apply(
        lambda x: x + ("; " if x else "") + "unknown store_id"
    )

    missing_customer = (~work["customer_id"].isin(customer_ids)) & (~null_customer)
    records.append(_dq_record("order_customer_exists", "Consistency", "Critical", len(work), int(missing_customer.sum())))
    reasons.loc[missing_customer] = reasons.loc[missing_customer].apply(
        lambda x: x + ("; " if x else "") + "unknown customer_id"
    )

    parsed_ts = work["order_ts"].apply(parse_timestamp)
    bad_ts = parsed_ts.isna()
    records.append(_dq_record("order_ts_parseable", "Validity", "Critical", len(work), int(bad_ts.sum())))
    reasons.loc[bad_ts] = reasons.loc[bad_ts].apply(
        lambda x: x + ("; " if x else "") + "invalid order_ts"
    )

    future_ts = parsed_ts.apply(lambda dt: dt is not None and dt > as_of)
    records.append(_dq_record("order_ts_not_future", "Timeliness", "Critical", len(work), int(future_ts.sum())))
    reasons.loc[future_ts] = reasons.loc[future_ts].apply(
        lambda x: x + ("; " if x else "") + "future-dated order"
    )

    delivery = work["delivery_minutes"].apply(parse_float)
    delivered = work["order_status"] == "DELIVERED"
    missing_delivery = delivered & delivery.isna()
    records.append(
        _dq_record(
            "delivered_has_delivery_minutes",
            "Completeness",
            "Critical",
            len(work),
            int(missing_delivery.sum()),
        )
    )
    reasons.loc[missing_delivery] = reasons.loc[missing_delivery].apply(
        lambda x: x + ("; " if x else "") + "missing delivery_minutes for DELIVERED"
    )

    long_delivery = delivered & delivery.notna() & (delivery > 120)
    records.append(_dq_record("delivery_minutes_plausible", "Accuracy", "Critical", len(work), int(long_delivery.sum())))
    reasons.loc[long_delivery] = reasons.loc[long_delivery].apply(
        lambda x: x + ("; " if x else "") + "delivery_minutes above 120"
    )

    stale = parsed_ts.apply(lambda dt: dt is not None and (as_of - dt).days > 1)
    records.append(_dq_record("orders_freshness_warning", "Timeliness", "Warning", len(work), int(stale.sum())))

    work = work.loc[~dup_mask].copy()
    reasons = reasons.loc[work.index]
    return *_split_pass_fail(work, reasons), records


def summarize_quarantine(files: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for file_name, failed in files.items():
        if failed.empty:
            continue
        counts = Counter(failed["failure_reason"].astype(str))
        for reason, count in counts.items():
            rows.append({"file": file_name, "failure_reason": reason, "rows": count})
    return pd.DataFrame(rows)
