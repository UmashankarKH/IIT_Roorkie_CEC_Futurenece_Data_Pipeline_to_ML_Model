"""
Data-quality rules for the ETL pipeline, written in pandas.

Each validate_* function takes the raw extract (every column as text) and
returns the standardised, typed frame with:
  f_<check_name>  True when the row FAILS that check
  failure_reason  "; "-joined reasons of every failed Critical check
  dq_warnings     "; "-joined reasons of every failed Warning check
  dq_status       PASSED / FAILED (FAILED = at least one Critical check failed)

The ELT pipeline implements exactly the same rules in include/sql/staging/.
"""

import re

import pandas as pd

from nammamart_divya.config import (
    AMOUNT_TOLERANCE, CHECKS, CRITICAL, MAX_DELIVERY_MINUTES, PAYMENT_MODE_MAP,
    PRICE_DRIFT_FACTOR, SUBCATEGORY_TO_CATEGORY, VALID_ORDER_STATUS,
    VALID_PAYMENT_MODES, VALID_PAYMENT_STATUS, VALID_TIERS, checks_for,
)

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def _clean_text(df):
    """Trim every column and turn empty strings into nulls (raw file is untouched)."""
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].astype("object").map(
            lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "" else str(v).strip()
        )
    return out


def _num(series):
    return pd.to_numeric(series, errors="coerce")


def _finish(df, file_key):
    """Build failure_reason / dq_warnings / dq_status from the f_ columns."""
    checks = checks_for(file_key)
    reasons, warnings = [], []
    for idx in df.index:
        r, w = [], []
        for name, (_, _, severity, text) in checks.items():
            col = f"f_{name}"
            if col in df.columns and bool(df.at[idx, col]):
                (r if severity == CRITICAL else w).append(text)
        reasons.append("; ".join(r) or None)
        warnings.append("; ".join(w) or None)
    df["failure_reason"] = reasons
    df["dq_warnings"] = warnings
    df["dq_status"] = ["FAILED" if r else "PASSED" for r in reasons]
    return df


def row_check_counts(df, file_key):
    """{check_name: (rows_checked, rows_failed)} for every row-level check on this file."""
    return {
        name: (len(df), int(df[f"f_{name}"].sum()))
        for name in checks_for(file_key)
        if f"f_{name}" in df.columns
    }


# ── Dimensions ───────────────────────────────────────────────────────────────

def validate_stores(raw):
    df = _clean_text(raw)
    df["capacity_orders_per_day"] = _num(df["capacity_orders_per_day"])
    df["open_date"] = pd.to_datetime(df["open_date"], format=DATE_FORMAT, errors="coerce")

    df["f_store_id_unique"] = df["store_id"].duplicated(keep="first") & df["store_id"].notna()
    df["f_store_zone_present"] = df["zone"].isna()
    df["f_store_pincode_valid"] = ~df["pincode"].fillna("").str.fullmatch(r"\d{6}")
    df["f_store_capacity_positive"] = ~(df["capacity_orders_per_day"] > 0)

    df["zone"] = df["zone"].fillna("Unknown")
    return _finish(df, "stores")


def validate_products(raw, cutoff):
    df = _clean_text(raw)
    df["_row"] = range(len(df))
    df["mrp"] = _num(df["mrp"])
    df["cost_price"] = _num(df["cost_price"])
    df["is_perishable"] = df["is_perishable"].str.lower().eq("true")
    df["launch_date"] = pd.to_datetime(df["launch_date"], format=DATE_FORMAT, errors="coerce")

    # Duplicate product_id: keep the current row, the "(old)" row is superseded.
    df["_is_old"] = df["product_name"].fillna("").str.lower().str.endswith("(old)")
    ranked = df.sort_values(["product_id", "_is_old", "_row"])
    keep = ~ranked["product_id"].duplicated(keep="first")
    df["f_prod_id_unique"] = ~keep.reindex(df.index)

    df["f_prod_mrp_positive"] = ~(df["mrp"] > 0)
    df["f_prod_launch_not_future"] = df["launch_date"] >= pd.Timestamp(cutoff.date())
    df["f_prod_category_present"] = df["category"].isna()
    df["f_prod_cost_le_mrp"] = df["cost_price"] > df["mrp"]

    df["category"] = df["category"].fillna(df["sub_category"].map(SUBCATEGORY_TO_CATEGORY)).fillna("Unknown")
    df = df.drop(columns=["_row", "_is_old"])
    return _finish(df, "products")


def validate_customers(raw, cutoff):
    df = _clean_text(raw)
    tier_std = df["loyalty_tier"].str.title()
    df["f_cust_id_not_null"] = df["customer_id"].isna()
    df["f_cust_id_unique"] = df["customer_id"].duplicated(keep="first") & df["customer_id"].notna()
    df["f_cust_city_present"] = df["city"].isna()
    df["f_cust_email_valid"] = ~df["email"].fillna("").str.match(EMAIL_RE)
    df["f_cust_phone_valid"] = ~df["phone"].fillna("").str.fullmatch(r"\d{10}")
    df["f_cust_pincode_valid"] = ~df["pincode"].fillna("").str.fullmatch(r"\d{6}")
    df["f_cust_tier_casing"] = tier_std.isin(VALID_TIERS) & (df["loyalty_tier"] != tier_std)
    signup = pd.to_datetime(df["signup_date"], format=DATE_FORMAT, errors="coerce")
    df["f_cust_signup_not_future"] = signup >= pd.Timestamp(cutoff.date())

    df["loyalty_tier"] = tier_std
    df["is_active"] = df["is_active"].eq("Y")
    # A future signup date is wrong, but the customer is real: keep the row, null the date.
    df["signup_date"] = signup.where(~df["f_cust_signup_not_future"])
    return _finish(df, "customers")


# ── Facts ────────────────────────────────────────────────────────────────────

def standardise_payment_mode(series):
    return series.map(lambda v: None if v is None else PAYMENT_MODE_MAP.get(v.strip().lower(), v.strip()))


def validate_orders(raw, cutoff, store_ids, product_mrp, customer_ids):
    """
    store_ids     ids in the store master
    product_mrp   {product_id: mrp} for products that PASSED validation
    customer_ids  ids in the customer master
    """
    df = _clean_text(raw)
    raw_status, raw_mode = df["order_status"], df["payment_mode"]
    df["order_status"] = raw_status.str.upper()
    df["payment_mode"] = standardise_payment_mode(raw_mode)
    df["order_ts"] = pd.to_datetime(df["order_ts"], format=TS_FORMAT, errors="coerce")
    for col in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        df[col] = _num(df[col])

    expected = df["quantity"] * df["unit_price"] * (1 - df["discount_pct"] / 100)
    mrp = df["product_id"].map(product_mrp)

    df["f_ord_customer_id_not_null"] = df["customer_id"].isna()
    df["f_ord_delivered_has_minutes"] = df["order_status"].eq("DELIVERED") & df["delivery_minutes"].isna()
    df["f_ord_order_id_unique"] = df["order_id"].duplicated(keep="first")
    df["f_ord_status_valid"] = ~df["order_status"].isin(VALID_ORDER_STATUS)
    df["f_ord_payment_mode_valid"] = ~df["payment_mode"].isin(VALID_PAYMENT_MODES)
    df["f_ord_ts_parseable"] = df["order_ts"].isna()
    df["f_ord_discount_in_range"] = ~df["discount_pct"].between(0, 100)
    df["f_ord_quantity_positive"] = ~(df["quantity"] > 0)
    df["f_ord_amount_matches_formula"] = ~((expected - df["order_amount"]).abs() <= AMOUNT_TOLERANCE)
    df["f_ord_delivery_under_limit"] = df["delivery_minutes"] >= MAX_DELIVERY_MINUTES
    df["f_ord_store_exists"] = ~df["store_id"].isin(store_ids)
    df["f_ord_product_exists"] = ~df["product_id"].isin(list(product_mrp))
    df["f_ord_customer_exists"] = df["customer_id"].notna() & ~df["customer_id"].isin(customer_ids)
    df["f_ord_ts_not_future"] = df["order_ts"] >= pd.Timestamp(cutoff)
    df["f_ord_value_casing"] = (
        (df["order_status"].isin(VALID_ORDER_STATUS) & (raw_status != df["order_status"]))
        | (df["payment_mode"].isin(VALID_PAYMENT_MODES) & (raw_mode != df["payment_mode"]))
    )
    df["f_ord_price_drift"] = (df["unit_price"] > PRICE_DRIFT_FACTOR * mrp).fillna(False)
    return _finish(df, "orders")


def validate_payments(raw, cutoff, order_ids):
    df = _clean_text(raw)
    raw_mode = df["payment_mode"]
    df["payment_mode"] = standardise_payment_mode(raw_mode)
    df["payment_status"] = df["payment_status"].str.upper()
    df["payment_ts"] = pd.to_datetime(df["payment_ts"], format=TS_FORMAT, errors="coerce")
    df["amount_paid"] = _num(df["amount_paid"])

    df["f_pay_payment_id_unique"] = df["payment_id"].duplicated(keep="first")
    df["f_pay_order_exists"] = ~df["order_id"].isin(order_ids)
    df["f_pay_mode_valid"] = ~df["payment_mode"].isin(VALID_PAYMENT_MODES)
    df["f_pay_status_valid"] = ~df["payment_status"].isin(VALID_PAYMENT_STATUS)
    df["f_pay_ts_parseable"] = df["payment_ts"].isna()
    df["f_pay_amount_positive"] = ~(df["amount_paid"] > 0)
    df["f_pay_ts_not_future"] = df["payment_ts"] >= pd.Timestamp(cutoff)
    df["f_pay_gateway_ref_present"] = df["gateway_ref"].isna() & df["payment_mode"].ne("Cash on Delivery")
    df["f_pay_value_casing"] = df["payment_mode"].isin(VALID_PAYMENT_MODES) & (raw_mode != df["payment_mode"])
    return _finish(df, "payments")


def flag_columns(df):
    return [c for c in df.columns if c.startswith("f_")]


assert all(re.fullmatch(r"[a-z_]+", k) for k in CHECKS)
