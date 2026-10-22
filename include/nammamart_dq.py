"""Python data-quality checks for the ETL pipeline (the ELT pipeline runs the same rules in SQL).

Severity: Critical rows are quarantined; Warning rows load and are only logged in dq_results.
Checks are evaluated independently, so rows_failed counts every row that breaks that one rule.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

import nammamart_common as nc

EMAIL_RE = r"[^@\s]+@[^@\s]+\.[^@\s]+"
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
TIERS = ("Bronze", "Silver", "Gold", "Platinum")


@dataclass
class Check:
    name: str
    dimension: str
    severity: str  # "Critical" or "Warning"
    failed: pd.Series  # True where the row breaks the rule
    reason: str


def read_csv(path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def _after(ts: pd.Series, as_of: date) -> pd.Series:
    """True where the timestamp falls on a day after as_of (NaT is False)."""
    return ts >= pd.Timestamp(as_of + timedelta(days=1))


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _clean_text(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].str.strip()
    return out.replace({"": None})


def normalise_payment_mode(s: pd.Series) -> pd.Series:
    mapped = s.str.strip().str.lower().map(nc.PAYMENT_MODE_MAP)
    return mapped.where(mapped.notna(), s.str.strip())



def run_checks(df: pd.DataFrame, checks: list[Check]):
    """Return (passed, failed_with_reason, dq_result rows)."""
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index)
    results = []
    critical_fail = pd.Series(False, index=df.index)
    for c in checks:
        mask = c.failed.fillna(True).astype(bool)
        n_failed = int(mask.sum())
        status = "PASS" if n_failed == 0 else ("FAIL" if c.severity == "Critical" else "WARN")
        results.append({"check_name": c.name, "dimension": c.dimension, "severity": c.severity,
                        "rows_checked": len(df), "rows_failed": n_failed, "status": status})
        if c.severity == "Critical" and n_failed:
            critical_fail |= mask
            for idx in df.index[mask]:
                reasons[idx].append(c.reason)
    failed = df[critical_fail].copy()
    failed["failure_reason"] = reasons[critical_fail].map("; ".join)
    return df[~critical_fail].copy(), failed, results


def dataset_check(name, dimension, severity, failed: bool) -> dict:
    status = "PASS" if not failed else ("FAIL" if severity == "Critical" else "WARN")
    return {"check_name": name, "dimension": dimension, "severity": severity,
            "rows_checked": 1, "rows_failed": int(failed), "status": status}


def _dupes(s: pd.Series) -> pd.Series:
    return s.notna() & s.duplicated(keep="first")


def validate_customers(raw: pd.DataFrame, as_of: date):
    df = _clean_text(raw)
    signup = pd.to_datetime(df.signup_date, format=DATE_FORMAT, errors="coerce")
    checks = [
        Check("customers.customer_id_present", "Completeness", "Critical", df.customer_id.isna(), "customer_id missing"),
        Check("customers.customer_id_unique", "Uniqueness", "Critical", _dupes(df.customer_id), "duplicate customer_id"),
        Check("customers.signup_date_valid", "Validity", "Critical", signup.isna(), "signup_date not a valid date"),
        Check("customers.signup_date_not_future", "Timeliness", "Critical",
              _after(signup, as_of), "signup_date in the future"),
        Check("customers.email_format", "Validity", "Warning", ~df.email.fillna("").str.fullmatch(EMAIL_RE), "invalid email"),
        Check("customers.phone_10_digits", "Validity", "Warning", ~df.phone.fillna("").str.fullmatch(r"\d{10}"), "invalid phone"),
        Check("customers.pincode_6_digits", "Validity", "Warning", ~df.pincode.fillna("").str.fullmatch(r"\d{6}"), "invalid pincode"),
        Check("customers.loyalty_tier_casing", "Consistency", "Warning", ~df.loyalty_tier.isin(TIERS), "loyalty_tier casing"),
        Check("customers.city_present", "Completeness", "Warning", df.city.isna(), "city missing"),
    ]
    return run_checks(df, checks)


def validate_products(raw: pd.DataFrame, as_of: date):
    df = _clean_text(raw)
    mrp, cost = _num(df.mrp), _num(df.cost_price)
    launch = pd.to_datetime(df.launch_date, format=DATE_FORMAT, errors="coerce")
    checks = [
        Check("products.product_id_present", "Completeness", "Critical", df.product_id.isna(), "product_id missing"),
        Check("products.product_id_unique", "Uniqueness", "Critical", _dupes(df.product_id), "duplicate product_id"),
        Check("products.mrp_positive", "Accuracy", "Critical", ~(mrp > 0), "mrp not positive"),
        Check("products.cost_price_positive", "Accuracy", "Critical", ~(cost > 0), "cost_price not positive"),
        Check("products.cost_not_above_mrp", "Accuracy", "Critical", cost > mrp, "cost_price above mrp"),
        Check("products.category_present", "Completeness", "Warning", df.category.isna(), "category missing"),
        Check("products.launch_date_valid", "Validity", "Warning", launch.isna() | _after(launch, as_of),
              "launch_date invalid or in the future"),
    ]
    return run_checks(df, checks)


def validate_stores(raw: pd.DataFrame, as_of: date):
    df = _clean_text(raw)
    opened = pd.to_datetime(df.open_date, format=DATE_FORMAT, errors="coerce")
    checks = [
        Check("stores.store_id_present", "Completeness", "Critical", df.store_id.isna(), "store_id missing"),
        Check("stores.store_id_unique", "Uniqueness", "Critical", _dupes(df.store_id), "duplicate store_id"),
        Check("stores.zone_present", "Completeness", "Warning", df.zone.isna(), "zone missing"),
        Check("stores.zone_allowed", "Validity", "Warning",
              df.zone.notna() & ~df.zone.isin(["North", "South", "East", "West"]), "zone not in allowed list"),
        Check("stores.capacity_positive", "Accuracy", "Warning", ~(_num(df.capacity_orders_per_day) > 0),
              "capacity not positive"),
        Check("stores.open_date_not_future", "Timeliness", "Warning", opened.isna() | _after(opened, as_of),
              "open_date invalid or in the future"),
    ]
    return run_checks(df, checks)


def validate_orders(raw: pd.DataFrame, dataset: str, customer_ids, product_ids, store_ids, as_of: date,
                    batch: str):
    df = _clean_text(raw)
    # An order cannot be newer than the extract it came from, nor newer than the run date.
    cutoff = min(as_of, date.fromisoformat(batch))
    df["order_status"] = df.order_status.str.upper()
    df["payment_mode"] = normalise_payment_mode(df.payment_mode)
    ts = pd.to_datetime(df.order_ts, format=TS_FORMAT, errors="coerce")
    q, price, disc, amt, dm = (_num(df[c]) for c in ("quantity", "unit_price", "discount_pct", "order_amount",
                                                     "delivery_minutes"))
    expected = (q * price * (1 - disc / 100)).round(2)
    d = dataset
    checks = [
        Check(f"{d}.order_id_present", "Completeness", "Critical", df.order_id.isna(), "order_id missing"),
        Check(f"{d}.customer_id_present", "Completeness", "Critical", df.customer_id.isna(), "customer_id missing"),
        Check(f"{d}.order_ts_present", "Completeness", "Critical", df.order_ts.isna(), "order_ts missing"),
        Check(f"{d}.delivered_has_delivery_minutes", "Completeness", "Critical",
              (df.order_status == "DELIVERED") & dm.isna(), "DELIVERED order without delivery_minutes"),
        Check(f"{d}.order_id_unique", "Uniqueness", "Critical", _dupes(df.order_id), "duplicate order_id"),
        Check(f"{d}.order_status_allowed", "Validity", "Critical", ~df.order_status.isin(nc.VALID_STATUS),
              "order_status not in allowed list"),
        Check(f"{d}.payment_mode_allowed", "Validity", "Critical", ~df.payment_mode.isin(nc.VALID_PAYMENT_MODE),
              "payment_mode not in allowed list"),
        Check(f"{d}.discount_pct_0_100", "Validity", "Critical", ~disc.between(0, 100), "discount_pct outside 0-100"),
        Check(f"{d}.order_ts_parses", "Validity", "Critical", df.order_ts.notna() & ts.isna(), "order_ts not a valid timestamp"),
        Check(f"{d}.channel_allowed", "Validity", "Warning", ~df.channel.isin(["App", "Web"]), "channel not App/Web"),
        Check(f"{d}.quantity_positive", "Accuracy", "Critical", ~(q > 0), "quantity not positive"),
        Check(f"{d}.unit_price_positive", "Accuracy", "Critical", ~(price > 0), "unit_price not positive"),
        Check(f"{d}.order_amount_formula", "Accuracy", "Critical", ~((expected - amt).abs() <= 0.01),
              "order_amount does not match quantity x unit_price x (1 - discount)"),
        Check(f"{d}.delivery_minutes_below_120", "Accuracy", "Critical", dm >= 120, "delivery_minutes 120 or more"),
        Check(f"{d}.delivery_minutes_8_to_35", "Accuracy", "Warning", dm.notna() & (dm < 120) & ~dm.between(8, 35),
              "delivery_minutes outside 8-35"),
        Check(f"{d}.customer_exists", "Consistency", "Critical",
              df.customer_id.notna() & ~df.customer_id.isin(customer_ids), "customer_id not in validated customers"),
        Check(f"{d}.product_exists", "Consistency", "Critical", ~df.product_id.isin(product_ids),
              "product_id not in validated products"),
        Check(f"{d}.store_exists", "Consistency", "Critical", ~df.store_id.isin(store_ids),
              "store_id not in validated stores"),
        Check(f"{d}.order_ts_not_future", "Timeliness", "Critical", _after(ts, cutoff),
              "order_ts in the future"),
    ]
    passed, failed, results = run_checks(df, checks)
    usable = ts[ts.notna() & ~_after(ts, cutoff)]
    stale = usable.empty or usable.max().date() < as_of - timedelta(days=1)
    results.append(dataset_check(f"{d}.latest_order_within_1_day", "Timeliness", "Warning", stale))
    return passed, failed, results


def validate_payments(raw: pd.DataFrame, raw_order_ids, as_of: date):
    df = _clean_text(raw)
    df["payment_status"] = df.payment_status.str.upper()
    df["payment_mode"] = normalise_payment_mode(df.payment_mode)
    ts = pd.to_datetime(df.payment_ts, format=TS_FORMAT, errors="coerce")
    checks = [
        Check("payments.payment_id_present", "Completeness", "Critical", df.payment_id.isna(), "payment_id missing"),
        Check("payments.order_id_present", "Completeness", "Critical", df.order_id.isna(), "order_id missing"),
        Check("payments.gateway_ref_for_non_cod", "Completeness", "Warning",
              (df.payment_mode != "Cash on Delivery") & df.gateway_ref.isna(), "gateway_ref missing"),
        Check("payments.payment_id_unique", "Uniqueness", "Critical", _dupes(df.payment_id), "duplicate payment_id"),
        Check("payments.payment_status_allowed", "Validity", "Critical", ~df.payment_status.isin(nc.VALID_PAYMENT_STATUS),
              "payment_status not in allowed list"),
        Check("payments.payment_mode_allowed", "Validity", "Critical", ~df.payment_mode.isin(nc.VALID_PAYMENT_MODE),
              "payment_mode not in allowed list"),
        Check("payments.payment_ts_parses", "Validity", "Critical", ts.isna(), "payment_ts not a valid timestamp"),
        Check("payments.amount_paid_positive", "Accuracy", "Critical", ~(_num(df.amount_paid) > 0),
              "amount_paid not positive"),
        Check("payments.order_exists", "Consistency", "Warning", ~df.order_id.isin(raw_order_ids),
              "order_id not found in orders"),
        Check("payments.payment_ts_not_future", "Timeliness", "Critical", _after(ts, as_of),
              "payment_ts in the future"),
    ]
    return run_checks(df, checks)
