"""
include/nammamart_quality.py
----------------------------
Data Quality checks for NammaMart pipelines.
Implements all 6 DQ dimensions: Completeness, Uniqueness, Validity,
Accuracy, Consistency, Timeliness.

Each check returns a dict:
  {
    "check_name": str,
    "dimension": str,
    "severity": "Critical" | "Warning",
    "rows_checked": int,
    "rows_failed": int,
    "failed_index": list[int],  # row positions that failed
    "failure_reason": str
  }
"""

from __future__ import annotations

import pandas as pd
import re
from datetime import datetime, timezone


# ─── allowed value sets ──────────────────────────────────────────────────────

VALID_ORDER_STATUS  = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODE  = {"UPI", "Card", "Cash on Delivery", "Wallet"}
VALID_LOYALTY_TIER  = {"Silver", "Gold", "Platinum"}
VALID_ZONES         = {"North", "South", "East", "West"}


# ─── helper ──────────────────────────────────────────────────────────────────

def _result(check_name, dimension, severity, rows_checked, failed_idx, reason):
    return {
        "check_name":    check_name,
        "dimension":     dimension,
        "severity":      severity,
        "rows_checked":  rows_checked,
        "rows_failed":   len(failed_idx),
        "failed_index":  list(failed_idx),
        "failure_reason": reason,
    }


# ─── ORDERS checks ──────────────────────────────────────────────────────────

def orders_completeness_customer_id(df: pd.DataFrame) -> dict:
    idx = df[df["customer_id"].isna()].index
    return _result("orders_customer_id_not_null", "Completeness", "Critical",
                   len(df), idx, "customer_id is null")


def orders_completeness_delivery_minutes(df: pd.DataFrame) -> dict:
    """DELIVERED orders must have delivery_minutes."""
    delivered = df[df["order_status"].str.strip().str.upper() == "DELIVERED"]
    idx = delivered[delivered["delivery_minutes"].isna()].index
    return _result("orders_delivered_has_delivery_minutes", "Completeness", "Critical",
                   len(df), idx, "DELIVERED order missing delivery_minutes")


def orders_uniqueness_order_id(df: pd.DataFrame) -> dict:
    dupes = df[df.duplicated(subset=["order_id"], keep=False)]
    idx = dupes.index
    return _result("orders_order_id_unique", "Uniqueness", "Critical",
                   len(df), idx, "Duplicate order_id")


def orders_validity_order_status(df: pd.DataFrame) -> dict:
    norm = df["order_status"].str.strip().str.upper()
    idx = df[~norm.isin(VALID_ORDER_STATUS)].index
    return _result("orders_order_status_valid", "Validity", "Critical",
                   len(df), idx, f"order_status not in {VALID_ORDER_STATUS}")


def orders_validity_payment_mode(df: pd.DataFrame) -> dict:
    """Normalise first, then check."""
    norm = df["payment_mode"].str.strip()
    idx = df[~norm.isin(VALID_PAYMENT_MODE)].index
    return _result("orders_payment_mode_valid", "Validity", "Critical",
                   len(df), idx, f"payment_mode not in {VALID_PAYMENT_MODE}")


def orders_validity_order_ts(df: pd.DataFrame) -> dict:
    """order_ts must parse as a timestamp."""
    failed_idx = []
    for i, v in df["order_ts"].items():
        try:
            pd.to_datetime(v)
        except Exception:
            failed_idx.append(i)
    return _result("orders_order_ts_parseable", "Validity", "Critical",
                   len(df), failed_idx, "order_ts cannot be parsed as timestamp")


def orders_accuracy_quantity(df: pd.DataFrame) -> dict:
    idx = df[pd.to_numeric(df["quantity"], errors="coerce").fillna(0) <= 0].index
    return _result("orders_quantity_positive", "Accuracy", "Critical",
                   len(df), idx, "quantity must be > 0")


def orders_accuracy_order_amount(df: pd.DataFrame) -> dict:
    """order_amount = quantity * unit_price * (1 - discount_pct/100), rounded to 2 dp."""
    df2 = df.copy()
    df2["_qty"]       = pd.to_numeric(df2["quantity"],    errors="coerce")
    df2["_up"]        = pd.to_numeric(df2["unit_price"],  errors="coerce")
    df2["_disc"]      = pd.to_numeric(df2["discount_pct"], errors="coerce").fillna(0)
    df2["_oa"]        = pd.to_numeric(df2["order_amount"], errors="coerce")
    df2["_expected"]  = (df2["_qty"] * df2["_up"] * (1 - df2["_disc"] / 100)).round(2)
    diff = (df2["_oa"] - df2["_expected"]).abs()
    idx = df2[diff > 0.02].index
    return _result("orders_order_amount_formula", "Accuracy", "Critical",
                   len(df2), idx, "order_amount != quantity*unit_price*(1-discount_pct/100)")


def orders_accuracy_delivery_minutes(df: pd.DataFrame) -> dict:
    """DELIVERED delivery_minutes should be 8–35 (allow up to 120 per spec)."""
    delivered = df[df["order_status"].str.strip().str.upper() == "DELIVERED"].copy()
    dm = pd.to_numeric(delivered["delivery_minutes"], errors="coerce")
    idx = delivered[dm > 120].index
    return _result("orders_delivery_minutes_range", "Accuracy", "Warning",
                   len(df), idx, "delivery_minutes > 120 for DELIVERED order")


def orders_consistency_customer_id(df: pd.DataFrame, customers_df: pd.DataFrame) -> dict:
    valid_ids = set(customers_df["customer_id"].dropna())
    mask = ~df["customer_id"].isin(valid_ids) & df["customer_id"].notna()
    idx = df[mask].index
    return _result("orders_customer_id_exists", "Consistency", "Critical",
                   len(df), idx, "customer_id not found in customers master")


def orders_consistency_product_id(df: pd.DataFrame, products_df: pd.DataFrame) -> dict:
    valid_ids = set(products_df["product_id"].dropna())
    mask = ~df["product_id"].isin(valid_ids) & df["product_id"].notna()
    idx = df[mask].index
    return _result("orders_product_id_exists", "Consistency", "Critical",
                   len(df), idx, "product_id not found in products master")


def orders_consistency_store_id(df: pd.DataFrame, stores_df: pd.DataFrame) -> dict:
    valid_ids = set(stores_df["store_id"].dropna())
    mask = ~df["store_id"].isin(valid_ids) & df["store_id"].notna()
    idx = df[mask].index
    return _result("orders_store_id_exists", "Consistency", "Critical",
                   len(df), idx, "store_id not found in stores master")


def orders_timeliness_future(df: pd.DataFrame) -> dict:
    """No future-dated order_ts."""
    now = datetime.now(timezone.utc)
    failed_idx = []
    for i, v in df["order_ts"].items():
        try:
            ts = pd.to_datetime(v)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            if ts > now:
                failed_idx.append(i)
        except Exception:
            pass
    return _result("orders_order_ts_not_future", "Timeliness", "Critical",
                   len(df), failed_idx, "order_ts is in the future")


# ─── PAYMENTS checks ─────────────────────────────────────────────────────────

def payments_completeness(df: pd.DataFrame) -> dict:
    required = ["payment_id", "order_id", "amount_paid", "payment_status"]
    idx = df[df[required].isna().any(axis=1)].index
    return _result("payments_required_fields", "Completeness", "Critical",
                   len(df), idx, "Required payment field is null")


def payments_uniqueness(df: pd.DataFrame) -> dict:
    dupes = df[df.duplicated(subset=["payment_id"], keep=False)]
    return _result("payments_payment_id_unique", "Uniqueness", "Critical",
                   len(df), dupes.index, "Duplicate payment_id")


def payments_validity_mode(df: pd.DataFrame) -> dict:
    norm = df["payment_mode"].str.strip()
    idx = df[~norm.isin(VALID_PAYMENT_MODE)].index
    return _result("payments_payment_mode_valid", "Validity", "Warning",
                   len(df), idx, f"payment_mode not in {VALID_PAYMENT_MODE}")


def payments_consistency_order_id(df: pd.DataFrame, orders_df: pd.DataFrame) -> dict:
    valid_ids = set(orders_df["order_id"].dropna())
    mask = ~df["order_id"].isin(valid_ids) & df["order_id"].notna()
    idx = df[mask].index
    return _result("payments_order_id_exists", "Consistency", "Critical",
                   len(df), idx, "order_id in payments not in orders")


# ─── CUSTOMERS checks ────────────────────────────────────────────────────────

def customers_completeness(df: pd.DataFrame) -> dict:
    idx = df[df["customer_id"].isna()].index
    return _result("customers_customer_id_not_null", "Completeness", "Critical",
                   len(df), idx, "customer_id is null")


def customers_uniqueness(df: pd.DataFrame) -> dict:
    dupes = df[df.duplicated(subset=["customer_id"], keep=False)]
    return _result("customers_customer_id_unique", "Uniqueness", "Critical",
                   len(df), dupes.index, "Duplicate customer_id")


def customers_validity_email(df: pd.DataFrame) -> dict:
    email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    mask = df["email"].notna() & ~df["email"].str.match(email_re)
    idx = df[mask].index
    return _result("customers_email_format", "Validity", "Warning",
                   len(df), idx, "email does not match expected format")


def customers_validity_phone(df: pd.DataFrame) -> dict:
    mask = df["phone"].notna() & ~df["phone"].astype(str).str.match(r"^\d{10}$")
    idx = df[mask].index
    return _result("customers_phone_10_digits", "Validity", "Warning",
                   len(df), idx, "phone is not 10 digits")


def customers_validity_loyalty_tier(df: pd.DataFrame) -> dict:
    norm = df["loyalty_tier"].str.strip().str.title()
    idx = df[~norm.isin(VALID_LOYALTY_TIER)].index
    return _result("customers_loyalty_tier_valid", "Validity", "Warning",
                   len(df), idx, f"loyalty_tier not in {VALID_LOYALTY_TIER}")


def customers_timeliness_signup(df: pd.DataFrame) -> dict:
    now = datetime.now(timezone.utc).date()
    failed_idx = []
    for i, v in df["signup_date"].items():
        try:
            d = pd.to_datetime(v).date()
            if d > now:
                failed_idx.append(i)
        except Exception:
            pass
    return _result("customers_signup_not_future", "Timeliness", "Critical",
                   len(df), failed_idx, "signup_date is in the future")


# ─── PRODUCTS checks ─────────────────────────────────────────────────────────

def products_completeness(df: pd.DataFrame) -> dict:
    idx = df[df["category"].isna()].index
    return _result("products_category_not_null", "Completeness", "Critical",
                   len(df), idx, "category is null")


def products_uniqueness(df: pd.DataFrame) -> dict:
    dupes = df[df.duplicated(subset=["product_id"], keep=False)]
    return _result("products_product_id_unique", "Uniqueness", "Critical",
                   len(df), dupes.index, "Duplicate product_id")


def products_accuracy_mrp(df: pd.DataFrame) -> dict:
    mrp = pd.to_numeric(df["mrp"], errors="coerce")
    idx = df[mrp.fillna(0) <= 0].index
    return _result("products_mrp_positive", "Accuracy", "Critical",
                   len(df), idx, "mrp must be > 0")


def products_accuracy_cost_vs_mrp(df: pd.DataFrame) -> dict:
    mrp  = pd.to_numeric(df["mrp"],        errors="coerce")
    cost = pd.to_numeric(df["cost_price"], errors="coerce")
    idx = df[cost > mrp].index
    return _result("products_cost_le_mrp", "Accuracy", "Warning",
                   len(df), idx, "cost_price > mrp")


def products_timeliness_launch(df: pd.DataFrame) -> dict:
    now = datetime.now(timezone.utc).date()
    failed_idx = []
    for i, v in df["launch_date"].items():
        try:
            d = pd.to_datetime(v).date()
            if d > now:
                failed_idx.append(i)
        except Exception:
            pass
    return _result("products_launch_not_future", "Timeliness", "Warning",
                   len(df), failed_idx, "launch_date is in the future")


# ─── STORES checks ───────────────────────────────────────────────────────────

def stores_completeness(df: pd.DataFrame) -> dict:
    idx = df[df["zone"].isna() | (df["zone"].str.strip() == "")].index
    return _result("stores_zone_not_null", "Completeness", "Warning",
                   len(df), idx, "zone is null or empty")


def stores_uniqueness(df: pd.DataFrame) -> dict:
    dupes = df[df.duplicated(subset=["store_id"], keep=False)]
    return _result("stores_store_id_unique", "Uniqueness", "Critical",
                   len(df), dupes.index, "Duplicate store_id")


def stores_validity_zone(df: pd.DataFrame) -> dict:
    norm = df["zone"].str.strip().str.title()
    idx = df[~norm.isin(VALID_ZONES) & df["zone"].notna() & (df["zone"].str.strip() != "")].index
    return _result("stores_zone_valid", "Validity", "Warning",
                   len(df), idx, f"zone not in {VALID_ZONES}")


def stores_accuracy_capacity(df: pd.DataFrame) -> dict:
    cap = pd.to_numeric(df["capacity_orders_per_day"], errors="coerce")
    idx = df[cap.fillna(-1) < 0].index
    return _result("stores_capacity_non_negative", "Accuracy", "Warning",
                   len(df), idx, "capacity_orders_per_day < 0")


# ─── PII masking ──────────────────────────────────────────────────────────────

def mask_email(email: str) -> str:
    if pd.isna(email) or not isinstance(email, str) or "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if len(local) <= 2:
        masked_local = local[0] + "***"
    else:
        masked_local = local[:2] + "***"
    return f"{masked_local}@{domain}"


def mask_phone(phone) -> str:
    if pd.isna(phone):
        return phone
    s = str(int(float(phone))) if str(phone).replace(".", "").isdigit() else str(phone)
    if len(s) >= 4:
        return s[:2] + "******" + s[-2:]
    return "***"


# ─── Standardisation helpers ─────────────────────────────────────────────────

PAYMENT_MODE_MAP = {
    "upi":             "UPI",
    "card":            "Card",
    "cash on delivery": "Cash on Delivery",
    "wallet":          "Wallet",
}


def normalise_payment_mode(val: str) -> str:
    if pd.isna(val):
        return val
    return PAYMENT_MODE_MAP.get(val.strip().lower(), val.strip())


def normalise_order_status(val: str) -> str:
    if pd.isna(val):
        return val
    return val.strip().upper()


def normalise_loyalty_tier(val: str) -> str:
    if pd.isna(val):
        return val
    return val.strip().title()


# ─── Aggregate runner ────────────────────────────────────────────────────────

def run_orders_checks(orders_df, customers_df, products_df, stores_df):
    return [
        orders_completeness_customer_id(orders_df),
        orders_completeness_delivery_minutes(orders_df),
        orders_uniqueness_order_id(orders_df),
        orders_validity_order_status(orders_df),
        orders_validity_payment_mode(orders_df),
        orders_validity_order_ts(orders_df),
        orders_accuracy_quantity(orders_df),
        orders_accuracy_order_amount(orders_df),
        orders_accuracy_delivery_minutes(orders_df),
        orders_consistency_customer_id(orders_df, customers_df),
        orders_consistency_product_id(orders_df, products_df),
        orders_consistency_store_id(orders_df, stores_df),
        orders_timeliness_future(orders_df),
    ]


def get_critical_failed_index(checks):
    """Union of all row indices that failed a Critical check."""
    failed = set()
    for c in checks:
        if c["severity"] == "Critical":
            failed.update(c["failed_index"])
    return failed


def get_warning_failed_index(checks):
    failed = set()
    for c in checks:
        if c["severity"] == "Warning":
            failed.update(c["failed_index"])
    return failed


def build_failure_reason_map(checks):
    """Map row index -> comma-separated reasons for all failed checks."""
    reason_map: dict[int, list[str]] = {}
    for c in checks:
        for idx in c["failed_index"]:
            reason_map.setdefault(idx, []).append(c["failure_reason"])
    return {k: "; ".join(v) for k, v in reason_map.items()}
