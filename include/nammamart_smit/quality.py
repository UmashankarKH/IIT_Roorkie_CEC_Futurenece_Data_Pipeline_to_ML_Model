"""
include/nammamart_smit/quality.py
---------------------------------
Data quality checks for the ETL pipeline (pandas).
The ELT pipeline runs the SAME checks, with the same names, in SQL
(include/sql/stg_*.sql), so both scorecards in dq_results line up.

Every check has a dimension and a severity:
  Critical -> row goes to quarantine (name added to failure_reason)
  Warning  -> row still loads, but the problem is logged (name added to warnings)

Input DataFrames are read with dtype=str, so every value arrives exactly as
it was in the CSV and we decide how to parse it here.
"""

import pandas as pd

from include.nammamart_smit.config import (
    AMOUNT_TOLERANCE, FRESHNESS_HOURS, MAX_DELIVERY_MINUTES, NORMAL_DELIVERY_MINUTES,
    PAYMENT_MODE_ALIASES, PRICE_DRIFT_FACTOR, VALID_LOYALTY_TIER, VALID_ORDER_STATUS,
    VALID_PAYMENT_MODE, VALID_PAYMENT_STATUS, VALID_ZONE, VOLUME_DROP_THRESHOLD,
)

EMAIL_REGEX   = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"
PHONE_REGEX   = r"^[6-9]\d{9}$"
PINCODE_REGEX = r"^\d{6}$"
TS_FORMAT     = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT   = "%Y-%m-%d"


# ── Small helpers ────────────────────────────────────────────────────────────

def is_blank(series):
    return series.fillna("").str.strip() == ""


def to_number(series):
    return pd.to_numeric(series.str.strip(), errors="coerce")


def to_timestamp(series, fmt=TS_FORMAT):
    return pd.to_datetime(series.str.strip(), format=fmt, errors="coerce")


def standard_payment_mode(series):
    cleaned = series.fillna("").str.strip()
    return cleaned.str.lower().map(PAYMENT_MODE_ALIASES).fillna(cleaned)


def start_checks(df):
    df = df.copy()
    df["failure_reason"] = ""
    df["warnings"] = ""
    return df


def run_check(df, results, source_file, name, dimension, severity, failed):
    """Record one check: mark the failed rows and add a line to the scorecard."""
    failed = failed.fillna(False).astype(bool)
    column = "failure_reason" if severity == "Critical" else "warnings"
    df.loc[failed, column] = df.loc[failed, column] + name + "; "

    results.append({
        "source_file": source_file,
        "check_name": name,
        "dimension": dimension,
        "severity": severity,
        "rows_checked": int(len(df)),
        "rows_failed": int(failed.sum()),
    })


def finish_checks(df):
    df["failure_reason"] = df["failure_reason"].str.rstrip("; ")
    df["warnings"] = df["warnings"].str.rstrip("; ")
    df["dq_status"] = "PASSED"
    df.loc[df["failure_reason"] != "", "dq_status"] = "FAILED"
    return df


# ── Stores ───────────────────────────────────────────────────────────────────

def validate_stores(df, as_of, source_file="stores.csv"):
    df = start_checks(df)
    results = []
    open_date = to_timestamp(df["open_date"], DATE_FORMAT)
    capacity = to_number(df["capacity_orders_per_day"])

    run_check(df, results, source_file, "store_id_not_null", "Completeness", "Critical", is_blank(df["store_id"]))
    # Missing zone is a warning, not critical: dropping S07 would drop ~7 % of revenue.
    # The store loads with zone = 'Unknown' and ops is asked to fix the master.
    run_check(df, results, source_file, "zone_present", "Completeness", "Warning", is_blank(df["zone"]))
    run_check(df, results, source_file, "store_id_unique", "Uniqueness", "Critical", df.duplicated("store_id", keep="first"))
    run_check(df, results, source_file, "zone_valid", "Validity", "Critical",
              ~is_blank(df["zone"]) & ~df["zone"].str.strip().isin(VALID_ZONE))
    run_check(df, results, source_file, "pincode_6_digits", "Validity", "Warning",
              ~df["pincode"].fillna("").str.strip().str.match(PINCODE_REGEX))
    run_check(df, results, source_file, "capacity_positive", "Accuracy", "Warning", ~(capacity > 0))
    run_check(df, results, source_file, "open_date_not_future", "Timeliness", "Critical",
              open_date.isna() | (open_date > as_of))

    return finish_checks(df), results


# ── Products ─────────────────────────────────────────────────────────────────

def validate_products(df, as_of, source_file="products.csv"):
    df = start_checks(df)
    results = []
    mrp = to_number(df["mrp"])
    cost = to_number(df["cost_price"])
    launch = to_timestamp(df["launch_date"], DATE_FORMAT)

    # Two product_ids appear twice: the current row and an "(old)" row with last
    # season's MRP. Keep the current one, quarantine the "(old)" one.
    is_old = df["product_name"].str.contains("(old)", regex=False)
    newest_first = df.assign(is_old=is_old).sort_values("is_old", kind="stable")
    duplicate = newest_first.duplicated("product_id", keep="first").reindex(df.index)

    run_check(df, results, source_file, "category_not_null", "Completeness", "Critical", is_blank(df["category"]))
    run_check(df, results, source_file, "product_id_unique", "Uniqueness", "Critical", duplicate)
    run_check(df, results, source_file, "launch_date_valid", "Validity", "Critical", launch.isna())
    run_check(df, results, source_file, "mrp_positive", "Accuracy", "Critical", ~(mrp > 0))
    run_check(df, results, source_file, "cost_price_not_above_mrp", "Accuracy", "Critical", ~(cost <= mrp))
    run_check(df, results, source_file, "launch_date_not_future", "Timeliness", "Critical", launch > as_of)

    return finish_checks(df), results


# ── Customers ────────────────────────────────────────────────────────────────

def validate_customers(df, as_of, source_file="customers.csv"):
    df = start_checks(df)
    results = []
    signup = to_timestamp(df["signup_date"], DATE_FORMAT)
    tier = df["loyalty_tier"].fillna("").str.strip().str.title()

    run_check(df, results, source_file, "customer_id_not_null", "Completeness", "Critical", is_blank(df["customer_id"]))
    run_check(df, results, source_file, "city_present", "Completeness", "Warning", is_blank(df["city"]))
    # Same customer_id twice with different phone/area: we can't tell which is
    # newer, so keep the first row in the file and quarantine the rest.
    run_check(df, results, source_file, "customer_id_unique", "Uniqueness", "Critical", df.duplicated("customer_id", keep="first"))
    run_check(df, results, source_file, "signup_date_valid", "Validity", "Critical", signup.isna())
    run_check(df, results, source_file, "loyalty_tier_valid", "Validity", "Critical", ~tier.isin(VALID_LOYALTY_TIER))
    run_check(df, results, source_file, "email_format", "Validity", "Warning",
              ~df["email"].fillna("").str.strip().str.match(EMAIL_REGEX))
    run_check(df, results, source_file, "phone_10_digits", "Validity", "Warning",
              ~df["phone"].fillna("").str.strip().str.match(PHONE_REGEX))
    run_check(df, results, source_file, "pincode_6_digits", "Validity", "Warning",
              ~df["pincode"].fillna("").str.strip().str.match(PINCODE_REGEX))
    run_check(df, results, source_file, "loyalty_tier_casing", "Consistency", "Warning", df["loyalty_tier"] != tier)
    run_check(df, results, source_file, "signup_date_not_future", "Timeliness", "Critical", signup > as_of)

    return finish_checks(df), results


# ── Orders ───────────────────────────────────────────────────────────────────

def validate_orders(df, as_of, source_file, customer_ids, product_ids, store_ids, product_mrp):
    """customer_ids / product_ids / store_ids are the keys that PASSED their own
    checks, so every order that loads can join to all three dimensions."""
    df = start_checks(df)
    results = []
    order_ts = to_timestamp(df["order_ts"])
    quantity = to_number(df["quantity"])
    unit_price = to_number(df["unit_price"])
    discount = to_number(df["discount_pct"])
    amount = to_number(df["order_amount"])
    minutes = to_number(df["delivery_minutes"])
    status = df["order_status"].fillna("").str.strip().str.upper()
    mode = standard_payment_mode(df["payment_mode"])
    expected_amount = (quantity * unit_price * (1 - discount / 100)).round(2)
    mrp = df["product_id"].map(product_mrp)

    run_check(df, results, source_file, "customer_id_not_null", "Completeness", "Critical", is_blank(df["customer_id"]))
    run_check(df, results, source_file, "delivered_has_delivery_minutes", "Completeness", "Critical",
              (status == "DELIVERED") & minutes.isna())
    run_check(df, results, source_file, "order_id_unique", "Uniqueness", "Critical", df.duplicated("order_id", keep="first"))
    run_check(df, results, source_file, "order_ts_valid", "Validity", "Critical", order_ts.isna())
    run_check(df, results, source_file, "order_status_valid", "Validity", "Critical", ~status.isin(VALID_ORDER_STATUS))
    run_check(df, results, source_file, "payment_mode_valid", "Validity", "Critical", ~mode.isin(VALID_PAYMENT_MODE))
    run_check(df, results, source_file, "discount_pct_in_range", "Validity", "Critical", ~discount.between(0, 100))
    run_check(df, results, source_file, "quantity_positive", "Accuracy", "Critical", ~(quantity > 0))
    run_check(df, results, source_file, "order_amount_matches_formula", "Accuracy", "Critical",
              ~((amount - expected_amount).abs() <= AMOUNT_TOLERANCE))
    run_check(df, results, source_file, "delivery_minutes_below_120", "Accuracy", "Critical", minutes > MAX_DELIVERY_MINUTES)
    run_check(df, results, source_file, "delivery_minutes_normal_range", "Accuracy", "Warning",
              (minutes > NORMAL_DELIVERY_MINUTES) & (minutes <= MAX_DELIVERY_MINUTES))
    run_check(df, results, source_file, "customer_id_in_master", "Consistency", "Critical",
              ~is_blank(df["customer_id"]) & ~df["customer_id"].isin(customer_ids))
    run_check(df, results, source_file, "product_id_in_master", "Consistency", "Critical", ~df["product_id"].isin(product_ids))
    run_check(df, results, source_file, "store_id_in_master", "Consistency", "Critical", ~df["store_id"].isin(store_ids))
    run_check(df, results, source_file, "values_need_standardising", "Consistency", "Warning",
              (df["order_status"] != status) | (df["payment_mode"] != mode))
    run_check(df, results, source_file, "unit_price_vs_mrp", "Consistency", "Warning", unit_price > PRICE_DRIFT_FACTOR * mrp)
    run_check(df, results, source_file, "order_ts_not_future", "Timeliness", "Critical", order_ts > as_of)

    df = finish_checks(df)
    df["order_status_std"] = status
    df["payment_mode_std"] = mode
    return df, results


# ── Payments ─────────────────────────────────────────────────────────────────

def validate_payments(df, as_of, known_order_ids, source_file="payments.csv"):
    """known_order_ids = every order_id the order system sent (good or bad),
    so we only flag payments for orders that never existed."""
    df = start_checks(df)
    results = []
    payment_ts = to_timestamp(df["payment_ts"])
    amount = to_number(df["amount_paid"])
    status = df["payment_status"].fillna("").str.strip().str.upper()
    mode = standard_payment_mode(df["payment_mode"])

    run_check(df, results, source_file, "order_id_not_null", "Completeness", "Critical", is_blank(df["order_id"]))
    run_check(df, results, source_file, "gateway_ref_present", "Completeness", "Warning",
              (mode != "Cash on Delivery") & is_blank(df["gateway_ref"]))
    run_check(df, results, source_file, "payment_id_unique", "Uniqueness", "Critical", df.duplicated("payment_id", keep="first"))
    run_check(df, results, source_file, "payment_ts_valid", "Validity", "Critical", payment_ts.isna())
    run_check(df, results, source_file, "payment_status_valid", "Validity", "Critical", ~status.isin(VALID_PAYMENT_STATUS))
    run_check(df, results, source_file, "payment_mode_valid", "Validity", "Critical", ~mode.isin(VALID_PAYMENT_MODE))
    run_check(df, results, source_file, "amount_paid_valid", "Accuracy", "Critical",
              amount.isna() | (amount < 0) | ((status == "SUCCESS") & (amount == 0)))
    run_check(df, results, source_file, "order_id_in_orders", "Consistency", "Critical", ~df["order_id"].isin(known_order_ids))
    run_check(df, results, source_file, "values_need_standardising", "Consistency", "Warning", df["payment_mode"] != mode)
    run_check(df, results, source_file, "payment_ts_not_future", "Timeliness", "Critical", payment_ts > as_of)

    df = finish_checks(df)
    df["payment_status_std"] = status
    df["payment_mode_std"] = mode
    return df, results


# ── File-level checks (one result per file, not per row) ─────────────────────

def freshness_check(source_file, latest_ts, as_of):
    """Warn if the newest record is more than 24 hours older than the run time."""
    stale = latest_ts is None or pd.isna(latest_ts) or (as_of - latest_ts).total_seconds() > FRESHNESS_HOURS * 3600
    lag = "unknown" if latest_ts is None or pd.isna(latest_ts) else str(as_of - latest_ts)
    print(f"[Freshness] {source_file}: latest record {latest_ts}, run time {as_of}, lag {lag}")
    return {"source_file": source_file, "check_name": "freshness_latest_record", "dimension": "Timeliness",
            "severity": "Warning", "rows_checked": 1, "rows_failed": int(stale)}


def volume_check(source_file, current_rows, previous_rows):
    """Warn if a file has dropped by more than 90 % compared to the last run.
    rows_checked holds this run's row count, so the next run can compare against it."""
    dropped = previous_rows is not None and current_rows < VOLUME_DROP_THRESHOLD * previous_rows
    print(f"[Volume] {source_file}: {current_rows} rows now, {previous_rows} on the previous run")
    return {"source_file": source_file, "check_name": "volume_vs_previous_run", "dimension": "Completeness",
            "severity": "Warning", "rows_checked": int(current_rows), "rows_failed": int(dropped)}


def store_capacity_check(orders, stores, source_file="orders.csv"):
    """Bonus: clean orders per store per day must not exceed capacity_orders_per_day.
    One row checked per store-day. A store over capacity is an ops problem (or a
    wrong capacity in the store master), so it's a warning, not a reason to drop orders."""
    # an order corrected by an increment file appears twice; count its latest version once
    passed = orders[orders["dq_status"] == "PASSED"].drop_duplicates("order_id", keep="last").copy()
    passed["order_date"] = to_timestamp(passed["order_ts"]).dt.date
    daily = passed.groupby(["store_id", "order_date"]).size().rename("orders").reset_index()

    good_stores = stores[stores["dq_status"] == "PASSED"]
    capacity = to_number(good_stores.set_index("store_id")["capacity_orders_per_day"])
    daily["capacity"] = daily["store_id"].map(capacity)
    over = daily[daily["orders"] > daily["capacity"]]

    print(f"[Capacity] {len(over)} of {len(daily)} store-days over capacity "
          f"(stores: {sorted(over['store_id'].unique())}); busiest store-day: {daily['orders'].max()} orders")
    return {"source_file": source_file, "check_name": "store_daily_orders_within_capacity", "dimension": "Consistency",
            "severity": "Warning", "rows_checked": int(len(daily)), "rows_failed": int(len(over))}
