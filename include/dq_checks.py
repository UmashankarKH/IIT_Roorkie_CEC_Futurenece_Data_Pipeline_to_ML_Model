"""
Data quality engine (pandas) for the ETL pipeline.
Each Check returns True for rows that FAIL.
  Critical -> row goes to quarantine with a failure_reason
  Warning  -> row still loads, but the failure is logged in dq_results
"""
from dataclasses import dataclass
from typing import Callable
import pandas as pd

from include.nammamart_config import (
    ORDER_STATUSES, PAYMENT_MODES, PAYMENT_MODE_MAP,
    LOYALTY_TIERS, ZONES, MAX_DELIVERY_MIN)

EMAIL_RE = r"^[\w.+\-]+@[\w\-]+(\.[\w\-]+)+$"


@dataclass
class Check:
    name: str
    dimension: str      # Completeness, Uniqueness, Validity, Accuracy, Consistency, Timeliness
    severity: str       # "Critical" or "Warning"
    fn: Callable        # fn(df) -> True/False per row (True = fail)
    scope: str = "row"  # "row" or "dataset"


# ---------- small helpers (all data is read as text) ----------
def s(df, c):
    return df[c].fillna("").astype(str).str.strip()

def num(df, c):
    return pd.to_numeric(s(df, c), errors="coerce")

def ts(df, c):
    return pd.to_datetime(s(df, c), errors="coerce")

def blank(df, c):
    return s(df, c).eq("")

def dup(df, c):
    return s(df, c).ne("") & s(df, c).duplicated(keep="first")

def norm_mode(df, c="payment_mode"):
    return s(df, c).str.lower().map(PAYMENT_MODE_MAP)


# ---------- runner ----------
def run_checks(df, checks, file, dag_id, run_id, now):
    """Returns (passed_df, failed_df with failure_reason, dq_results rows)."""
    reasons = pd.Series("", index=df.index)
    results = []
    for c in checks:
        if c.scope == "dataset":
            checked = 1
            failed = int(bool(c.fn(df))) if len(df) else 1
        else:
            if len(df):
                mask = c.fn(df).fillna(True).astype(bool)
            else:
                mask = pd.Series(False, index=df.index)
            checked, failed = len(df), int(mask.sum())
            if c.severity == "Critical":
                reasons[mask] = reasons[mask] + c.name + ";"
        status = "PASS" if failed == 0 else ("FAIL" if c.severity == "Critical" else "WARN")
        results.append(dict(run_id=run_id, dag_id=dag_id, check_name=f"{file}.{c.name}",
                            dimension=c.dimension, severity=c.severity,
                            rows_checked=checked, rows_failed=failed,
                            status=status, checked_at=pd.Timestamp(now)))
    bad = reasons.ne("")
    failed_df = df[bad].copy()
    failed_df["failure_reason"] = reasons[bad].str.rstrip(";")
    return df[~bad].copy(), failed_df, results



# ---------- ORDERS checks ----------
def orders_checks(ctx):
    now = ctx["now"]
    status = lambda d: s(d, "order_status").str.upper()
    expected = lambda d: (num(d, "quantity") * num(d, "unit_price")
                          * (1 - num(d, "discount_pct") / 100)).round(2)
    return [
        # Completeness
        Check("order_id_not_null", "Completeness", "Critical", lambda d: blank(d, "order_id")),
        Check("customer_id_not_null", "Completeness", "Critical", lambda d: blank(d, "customer_id")),
        Check("store_id_not_null", "Completeness", "Critical", lambda d: blank(d, "store_id")),
        Check("product_id_not_null", "Completeness", "Critical", lambda d: blank(d, "product_id")),
        Check("delivered_has_delivery_minutes", "Completeness", "Critical",
              lambda d: status(d).eq("DELIVERED") & num(d, "delivery_minutes").isna()),
        # Uniqueness
        Check("order_id_unique", "Uniqueness", "Critical", lambda d: dup(d, "order_id")),
        # Validity
        Check("order_status_allowed", "Validity", "Critical",
              lambda d: ~status(d).isin(ORDER_STATUSES)),
        Check("payment_mode_allowed", "Validity", "Critical",
              lambda d: norm_mode(d).isna()),
        Check("payment_mode_needs_standardising", "Validity", "Warning",
              lambda d: norm_mode(d).notna() & ~s(d, "payment_mode").isin(PAYMENT_MODES)),
        Check("discount_pct_0_100", "Validity", "Critical",
              lambda d: ~num(d, "discount_pct").between(0, 100)),
        Check("order_ts_parses", "Validity", "Critical", lambda d: ts(d, "order_ts").isna()),
        # Accuracy
        Check("quantity_positive", "Accuracy", "Critical", lambda d: ~(num(d, "quantity") > 0)),
        Check("unit_price_positive", "Accuracy", "Critical", lambda d: ~(num(d, "unit_price") > 0)),
        Check("order_amount_matches_formula", "Accuracy", "Critical",
              lambda d: ~((expected(d) - num(d, "order_amount")).abs() <= 0.01)),
        Check("delivery_minutes_plausible", "Accuracy", "Critical",
              lambda d: num(d, "delivery_minutes").notna()
                        & ((num(d, "delivery_minutes") >= MAX_DELIVERY_MIN)
                           | (num(d, "delivery_minutes") < 0))),
        Check("delivery_minutes_in_8_35", "Accuracy", "Warning",
              lambda d: num(d, "delivery_minutes").notna()
                        & ~num(d, "delivery_minutes").between(8, 35)
                        & (num(d, "delivery_minutes") < MAX_DELIVERY_MIN)
                        & (num(d, "delivery_minutes") >= 0)),
        # Consistency
        Check("customer_id_exists", "Consistency", "Critical",
              lambda d: ~blank(d, "customer_id") & ~s(d, "customer_id").isin(ctx["customer_ids"])),
        Check("product_id_exists", "Consistency", "Critical",
              lambda d: ~blank(d, "product_id") & ~s(d, "product_id").isin(ctx["product_ids"])),
        Check("store_id_exists", "Consistency", "Critical",
              lambda d: ~blank(d, "store_id") & ~s(d, "store_id").isin(ctx["store_ids"])),
        # Timeliness
        Check("order_ts_not_future", "Timeliness", "Critical", lambda d: ts(d, "order_ts") > now),
        Check("latest_order_not_stale", "Timeliness", "Warning",
              lambda d: bool((now - ts(d, "order_ts").max()) > pd.Timedelta(days=1)),
              scope="dataset"),
    ]





# ---------- PAYMENTS checks ----------
def payments_checks(ctx):
    now = ctx["now"]
    return [
        Check("payment_id_not_null", "Completeness", "Critical", lambda d: blank(d, "payment_id")),
        Check("payment_id_unique", "Uniqueness", "Critical", lambda d: dup(d, "payment_id")),
        Check("payment_mode_allowed", "Validity", "Critical", lambda d: norm_mode(d).isna()),
        Check("amount_paid_positive", "Accuracy", "Critical", lambda d: ~(num(d, "amount_paid") > 0)),
        Check("payment_has_order", "Consistency", "Critical",
              lambda d: ~s(d, "order_id").isin(ctx["all_order_ids"])),
        # amount mismatch is a Warning: it goes to the reconciliation table, not quarantine
        Check("success_amount_equals_order_amount", "Consistency", "Warning",
              lambda d: pd.to_numeric(s(d, "order_id").map(ctx["order_amounts"]), errors="coerce")
                        .sub(num(d, "amount_paid")).abs().gt(0.01)
                        & s(d, "payment_status").str.upper().eq("SUCCESS")),
        Check("payment_ts_not_future", "Timeliness", "Critical", lambda d: ts(d, "payment_ts") > now),
    ]


# ---------- CUSTOMERS checks ----------
def customers_checks(ctx):
    now = ctx["now"]
    return [
        Check("customer_id_not_null", "Completeness", "Critical", lambda d: blank(d, "customer_id")),
        Check("customer_id_unique", "Uniqueness", "Critical", lambda d: dup(d, "customer_id")),
        Check("email_format", "Validity", "Warning", lambda d: ~s(d, "email").str.match(EMAIL_RE)),
        Check("phone_10_digits", "Validity", "Warning", lambda d: ~s(d, "phone").str.fullmatch(r"\d{10}")),
        Check("pincode_6_digits", "Validity", "Warning", lambda d: ~s(d, "pincode").str.fullmatch(r"\d{6}")),
        Check("loyalty_tier_casing", "Consistency", "Warning",
              lambda d: ~s(d, "loyalty_tier").isin(LOYALTY_TIERS)),
        Check("signup_date_not_future", "Timeliness", "Critical", lambda d: ts(d, "signup_date") > now),
    ]


# ---------- PRODUCTS checks ----------
def products_checks(ctx):
    return [
        Check("product_id_not_null", "Completeness", "Critical", lambda d: blank(d, "product_id")),
        Check("category_not_null", "Completeness", "Critical", lambda d: blank(d, "category")),
        Check("product_id_unique", "Uniqueness", "Critical", lambda d: dup(d, "product_id")),
        Check("mrp_positive", "Accuracy", "Critical", lambda d: ~(num(d, "mrp") > 0)),
        Check("cost_price_le_mrp", "Accuracy", "Critical",
              lambda d: ~(num(d, "cost_price") <= num(d, "mrp"))),
        Check("launch_date_not_future", "Timeliness", "Critical",
              lambda d: ts(d, "launch_date") > ctx["now"]),
    ]


# ---------- STORES checks ----------
def stores_checks(ctx):
    return [
        Check("store_id_not_null", "Completeness", "Critical", lambda d: blank(d, "store_id")),
        Check("store_id_unique", "Uniqueness", "Critical", lambda d: dup(d, "store_id")),
        Check("zone_allowed", "Validity", "Critical", lambda d: ~s(d, "zone").isin(ZONES)),
        Check("pincode_6_digits", "Validity", "Warning", lambda d: ~s(d, "pincode").str.fullmatch(r"\d{6}")),
        Check("capacity_positive", "Accuracy", "Warning",
              lambda d: ~(num(d, "capacity_orders_per_day") > 0)),
    ]


CHECKSETS = {"orders": orders_checks, "payments": payments_checks, "customers": customers_checks,
             "products": products_checks, "stores": stores_checks}