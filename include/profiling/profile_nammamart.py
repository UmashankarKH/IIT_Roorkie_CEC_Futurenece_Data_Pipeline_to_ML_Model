#!/usr/bin/env python3
"""
NammaMart Quick Commerce - Part 0: Discovery & Data Profiling
=============================================================

Profiles the six NammaMart CSV exports and produces a Data Issues Log.

What it does
  1. Profiles every file: row/column counts, nulls per column, distinct values,
     duplicate keys, min/max of numeric and date columns, value counts of
     category columns, whitespace and format problems.
  2. Checks referential integrity for all four relationships.
  3. Checks every client business rule (order_amount formula, allowed values,
     delivery-time range, SUCCESS payment = order amount, ...).
  4. Compares the increment file with orders.csv (new orders vs corrections).
  5. Writes a row-level Data Issues Log and a grouped summary for SUBMISSION.md.

Raw files are read as text and never modified.

Outputs (in --out-dir, default include/reports/profiling/)
  profile_summary.md    human-readable profile of every file
  data_issues_log.csv   one row per (row, problem): file, source_row, row_key,
                        column, problem, detail, dimension, severity, proposed_rule
  issues_summary.csv    the log grouped by file/column/problem with row counts
                        and sample keys -> paste into SUBMISSION.md

Personal data: values of full_name, email, phone and manager_name are never
written to the outputs, only described (e.g. "digits after cleaning: 9").

Usage (from the project root)
  python include/profiling/profile_nammamart.py
  python include/profiling/profile_nammamart.py --as-of 2026-10-01
  # or inside the Airflow container:
  astro dev bash
  python include/profiling/profile_nammamart.py
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FILES = {  # logical name -> (file name, primary key)
    "orders": ("orders.csv", "order_id"),
    "payments": ("payments.csv", "payment_id"),
    "customers": ("customers.csv", "customer_id"),
    "products": ("products.csv", "product_id"),
    "stores": ("stores.csv", "store_id"),
    "orders_increment": ("orders_2026-10-01_increment.csv", "order_id"),
}

ORDER_COLS = [
    "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
    "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
    "channel", "delivery_minutes",
]
EXPECTED_COLUMNS = {
    "orders": ORDER_COLS,
    "orders_increment": ORDER_COLS,
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode",
                 "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area",
                  "pincode", "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand",
                 "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date",
               "manager_name", "capacity_orders_per_day"],
}
ORDER_NUMERIC = ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]
NUMERIC_COLUMNS = {
    "orders": ORDER_NUMERIC, "orders_increment": ORDER_NUMERIC,
    "payments": ["amount_paid"], "customers": [],
    "products": ["mrp", "cost_price"], "stores": ["capacity_orders_per_day"],
}
DATE_COLUMNS = {
    "orders": ["order_ts"], "orders_increment": ["order_ts"],
    "payments": ["payment_ts"], "customers": ["signup_date"],
    "products": ["launch_date"], "stores": ["open_date"],
}
# Columns whose distinct values are listed in the profile
CATEGORY_COLUMNS = {
    "orders": ["order_status", "payment_mode", "channel"],
    "orders_increment": ["order_status", "payment_mode", "channel"],
    "payments": ["payment_mode", "payment_status"],
    "customers": ["city", "area", "loyalty_tier", "is_active"],
    "products": ["category", "sub_category", "brand", "unit", "is_perishable"],
    "stores": ["zone", "area"],
}
# Columns checked for "same value, different spelling/casing".
# order_status, payment_mode and zone have dedicated allowed-list checks instead.
CASING_COLUMNS = {
    "orders": ["channel"], "orders_increment": ["channel"],
    "payments": ["payment_status"],
    "customers": ["city", "area", "loyalty_tier", "is_active"],
    "products": ["category", "sub_category", "brand", "unit", "is_perishable"],
    "stores": ["area"],
}
PII_COLUMNS = {"full_name", "email", "phone", "manager_name"}

VALID_ORDER_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
PAYMENT_MODE_MAP = {
    "upi": "UPI", "card": "Card", "credit card": "Card", "debit card": "Card",
    "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery",
    "cash": "Cash on Delivery", "wallet": "Wallet",
}
VALID_ZONES = {"North", "South", "East", "West"}
BOOLEAN_TOKENS = {"true", "false", "1", "0", "yes", "no", "y", "n", "t", "f"}
NULL_TOKENS = {"", "null", "none", "nan", "na", "n/a", "-"}

DELIVERY_NORMAL = (8, 35)
DELIVERY_MAX = 120
AMOUNT_TOLERANCE = 0.01

ISO_TS = r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:[+-]\d{2}:?\d{2}|Z)?)?$"
EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
TEST_MARKER = r"(?:^|[^a-z])(?:test|dummy|fake)"

SEVERITY_ORDER = {"Critical": 0, "Warning": 1}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def blank(s: pd.Series) -> pd.Series:
    """True where the value is empty or a null placeholder such as 'NULL' or 'N/A'."""
    return s.isna() | s.astype(str).str.strip().str.lower().isin(NULL_TOKENS)


def clean(s: pd.Series) -> pd.Series:
    """Trimmed text, NaN for blanks."""
    return s.astype(str).str.strip().where(~blank(s))


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(clean(s), errors="coerce")


def _parse_ts(v):
    if not isinstance(v, str):
        return pd.NaT
    try:
        ts = pd.Timestamp(v)
    except (ValueError, TypeError, OverflowError):
        return pd.NaT
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is not None:  # normalise everything to naive IST
        ts = ts.tz_convert("Asia/Kolkata").tz_localize(None)
    return ts


def to_ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(clean(s).map(_parse_ts), errors="coerce")


def rx(s: pd.Series, pattern: str) -> pd.Series:
    """Boolean regex match that treats NaN as no-match."""
    return s.fillna("").astype(str).str.match(pattern)


def fmt(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)) or pd.isna(x):
        return ""
    if isinstance(x, (int, float, np.integer, np.floating)):
        return f"{x:,.2f}"
    return str(x)


def first_by_key(df: pd.DataFrame, key: str, values: pd.Series) -> pd.Series:
    """Series of values indexed by the (first occurrence of each) key."""
    s = pd.Series(values.values, index=clean(df[key]).values)
    s = s[~s.index.isna()]
    return s[~s.index.duplicated()]


def md_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "_none_\n"
    cols = [str(c) for c in df.columns]
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.itertuples(index=False):
        cells = [str(v).replace("|", "\\|").replace("\n", " ") for v in row]
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# Issue log
# ---------------------------------------------------------------------------
class IssueLog:
    COLUMNS = ["file", "source_row", "row_key", "column", "problem", "detail",
               "dimension", "severity", "proposed_rule"]

    def __init__(self):
        self.rows: list[dict] = []

    def flag(self, name, df, mask, key, column, problem, dimension, severity,
             rule, detail=None, pii_safe=False) -> int:
        """Log one issue per row where mask is True. Returns rows affected."""
        mask = pd.Series(mask, index=df.index).fillna(False).astype(bool)
        n = int(mask.sum())
        if n == 0:
            return 0
        if column in PII_COLUMNS and not pii_safe:
            detail = None  # never write personal data into the log
        sub = df.loc[mask]
        keys = sub[key].astype(str).str.strip().where(~blank(sub[key]), "<blank>")
        if isinstance(detail, pd.Series):
            details = detail.reindex(df.index).loc[mask].fillna("").astype(str).tolist()
        else:
            details = [detail or ""] * n
        fname = FILES[name][0] if name in FILES else name
        for r, k, d in zip(sub["_row"], keys, details):
            self.rows.append(dict(file=fname, source_row=int(r), row_key=k, column=column,
                                  problem=problem, detail=d, dimension=dimension,
                                  severity=severity, proposed_rule=rule))
        return n

    def note(self, name, column, problem, dimension, severity, rule, detail=""):
        """File-level issue (schema, freshness) not tied to a row."""
        self.rows.append(dict(file=FILES[name][0], source_row="", row_key="<file>",
                              column=column, problem=problem, detail=detail,
                              dimension=dimension, severity=severity, proposed_rule=rule))

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=self.COLUMNS)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def load(path: Path) -> tuple[pd.DataFrame, list[str]]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False,
                     encoding="utf-8-sig")
    raw_cols = list(df.columns)
    df.columns = [c.strip() for c in df.columns]
    df.insert(0, "_row", range(2, len(df) + 2))  # CSV line number (header = line 1)
    return df, raw_cols


# ---------------------------------------------------------------------------
# Generic checks (every file)
# ---------------------------------------------------------------------------
def check_schema(name, df, raw_cols, log):
    expected = EXPECTED_COLUMNS[name]
    missing = [c for c in expected if c not in df.columns]
    extra = [c for c in df.columns if c not in expected and c != "_row"]
    padded = [c for c in raw_cols if c != c.strip()]
    for c in missing:
        log.note(name, c, "expected column missing", "Completeness", "Critical",
                 "fail the load loudly if the schema differs")
        df[c] = ""  # add as blank so the remaining checks can still run
    for c in extra:
        log.note(name, c, "unexpected column", "Consistency", "Warning",
                 "confirm with the source-system owner")
    for c in padded:
        log.note(name, c.strip(), "column header has extra spaces", "Validity", "Warning",
                 "trim headers on extract")
    return missing, extra


def check_key_uniqueness(name, df, key, log):
    k = clean(df[key])
    log.flag(name, df, k.isna(), key, key, "primary key missing", "Completeness",
             "Critical", f"{key} must not be null")
    body = df.drop(columns="_row")
    exact = body.duplicated(keep="first")
    log.flag(name, df, exact, key, key, "exact duplicate row (whole row repeated)",
             "Uniqueness", "Critical", "keep the first occurrence, quarantine repeats")
    versions = body.assign(_k=k).drop_duplicates().dropna(subset=["_k"]).groupby("_k").size()
    conflict = k.isin(versions[versions > 1].index)
    log.flag(name, df, conflict, key, key,
             f"same {key} appears with different values (conflicting duplicate)",
             "Uniqueness", "Critical", f"{key} must be unique; ask client which version is correct")


def check_text_hygiene(name, df, key, log):
    for c in df.columns:
        if c == "_row":
            continue
        s = df[c].astype(str)
        not_blank = ~blank(df[c])
        log.flag(name, df, not_blank & (s != s.str.strip()), key, c,
                 "leading/trailing whitespace", "Validity", "Warning", "trim all text values")
        placeholder = s.str.strip().str.lower().isin(NULL_TOKENS - {""})
        log.flag(name, df, placeholder, key, c, "null written as a text placeholder",
                 "Completeness", "Warning", "treat 'NULL', 'N/A', '-' as missing",
                 detail=df[c])
        test = s.str.contains(TEST_MARKER, case=False, regex=True, na=False)
        log.flag(name, df, test, key, c, "looks like a test/dummy record", "Accuracy",
                 "Critical", "exclude test entries from reporting", detail=df[c])


def check_casing(name, df, key, log):
    for c in CASING_COLUMNS.get(name, []):
        raw = clean(df[c])
        if raw.notna().sum() == 0:
            continue
        norm = raw.str.lower().str.replace(r"\s+", " ", regex=True)
        majority = raw.groupby(norm).agg(lambda x: x.value_counts().index[0])
        canonical = norm.map(majority)
        m = raw.notna() & (raw != canonical)
        log.flag(name, df, m, key, c, "inconsistent spelling/casing of a category value",
                 "Consistency", "Warning", f"standardise {c} to one spelling",
                 detail=raw + " -> " + canonical.fillna(""))


def check_numeric_format(name, df, key, log):
    for c in NUMERIC_COLUMNS[name]:
        raw = clean(df[c])
        log.flag(name, df, raw.notna() & to_num(df[c]).isna(), key, c,
                 f"{c} is not a number", "Validity", "Critical", f"{c} must be numeric",
                 detail=raw)


def check_dates(name, df, key, col, log, as_of, severity_unparseable="Critical",
                window=None):
    raw = clean(df[col])
    ts = to_ts(df[col])
    log.flag(name, df, raw.notna() & ts.isna(), key, col, f"{col} cannot be parsed as a date/time",
             "Validity", severity_unparseable, f"{col} must be a valid ISO-8601 date/time",
             detail=raw)
    log.flag(name, df, raw.notna() & ts.notna() & ~rx(raw, ISO_TS), key, col,
             "non-ISO date format (day/month order is ambiguous)", "Validity", "Warning",
             "parse with an explicit format; standardise to YYYY-MM-DD", detail=raw)
    cutoff = as_of + pd.Timedelta(days=1)
    log.flag(name, df, ts >= cutoff, key, col, f"{col} is in the future", "Timeliness",
             "Critical", f"{col} must be before {cutoff:%Y-%m-%d}", detail=raw)
    if window is not None:
        lo, hi = window
        outside = ts.notna() & (ts < cutoff) & ((ts < lo) | (ts >= hi))
        log.flag(name, df, outside, key, col, f"{col} outside the file's expected period",
                 "Timeliness", "Warning",
                 f"expected {lo:%Y-%m-%d} to {(hi - pd.Timedelta(days=1)):%Y-%m-%d}",
                 detail=raw)
    return ts


def flag_outliers(name, df, key, col, values, log):
    v = values.dropna()
    if len(v) < 20:
        return
    q1, q3 = v.quantile([0.25, 0.75])
    iqr = q3 - q1
    if iqr == 0:
        return
    lo, hi = q1 - 3 * iqr, q3 + 3 * iqr
    log.flag(name, df, (values < lo) | (values > hi), key, col,
             "statistical outlier (beyond 3x IQR)", "Accuracy", "Warning",
             f"review; typical range about {lo:,.2f} to {hi:,.2f}", detail=values.astype(str))


# ---------------------------------------------------------------------------
# File-specific checks
# ---------------------------------------------------------------------------
def check_orders(name, o, ref, log, as_of, window):
    K = "order_id"
    for col in ["order_ts", "customer_id", "store_id", "product_id", "quantity",
                "unit_price", "order_amount", "order_status", "payment_mode"]:
        log.flag(name, o, blank(o[col]), K, col, "required value missing", "Completeness",
                 "Critical", f"{col} must not be null")
    log.flag(name, o, blank(o["discount_pct"]), K, "discount_pct", "discount_pct missing",
             "Completeness", "Warning", "treat blank as 0? confirm with client")
    log.flag(name, o, blank(o["channel"]), K, "channel", "channel missing",
             "Completeness", "Warning", "channel should always be filled")

    check_key_uniqueness(name, o, K, log)
    oid = clean(o[K])

    # Same customer/store/product/time/qty under different order_ids -> double submit?
    sig = ["customer_id", "store_id", "product_id", "order_ts", "quantity"]
    t = pd.DataFrame({c: clean(o[c]) for c in sig})
    t["_id"] = oid
    t = t.dropna()
    if len(t):
        n_ids = t.groupby(sig)["_id"].transform("nunique")
        log.flag(name, o, (n_ids > 1).reindex(o.index, fill_value=False), K, "order_id",
                 "possible double-submitted order (same customer, store, product, time, qty; different order_id)",
                 "Uniqueness", "Warning", "ask client whether these are real repeat orders")

    # Allowed values
    st_raw = clean(o["order_status"])
    st = st_raw.str.upper()
    log.flag(name, o, st_raw.notna() & ~st.isin(VALID_ORDER_STATUS), K, "order_status",
             "order_status not in allowed list", "Validity", "Critical",
             "allowed: DELIVERED, CANCELLED, RETURNED", detail=st_raw)
    log.flag(name, o, st.isin(VALID_ORDER_STATUS) & (st_raw != st), K, "order_status",
             "order_status needs upper-casing", "Validity", "Warning",
             "standardise to upper case", detail=st_raw)
    st = st.where(st.isin(VALID_ORDER_STATUS))

    pm_raw = clean(o["payment_mode"])
    pm = pm_raw.str.lower().str.replace(r"\s+", " ", regex=True).map(PAYMENT_MODE_MAP)
    log.flag(name, o, pm_raw.notna() & pm.isna(), K, "payment_mode",
             "payment_mode not in allowed list", "Validity", "Critical",
             "allowed: UPI, Card, Cash on Delivery, Wallet", detail=pm_raw)
    log.flag(name, o, pm.notna() & (pm_raw != pm), K, "payment_mode",
             "non-standard payment_mode spelling", "Validity", "Warning",
             "map variants to the canonical value", detail=pm_raw + " -> " + pm.fillna(""))

    ts = check_dates(name, o, K, "order_ts", log, as_of, window=window)

    # Numbers and the order_amount formula
    check_numeric_format(name, o, K, log)
    q, p = to_num(o["quantity"]), to_num(o["unit_price"])
    d, a = to_num(o["discount_pct"]), to_num(o["order_amount"])
    dm = to_num(o["delivery_minutes"])
    log.flag(name, o, q.notna() & (q % 1 != 0), K, "quantity", "quantity is not a whole number",
             "Validity", "Critical", "quantity must be an integer", detail=q.astype(str))
    log.flag(name, o, q <= 0, K, "quantity", "quantity is zero or negative", "Accuracy",
             "Critical", "quantity > 0 (ask: is a negative a return?)", detail=q.astype(str))
    log.flag(name, o, p <= 0, K, "unit_price", "unit_price is zero or negative", "Accuracy",
             "Critical", "unit_price > 0", detail=p.astype(str))
    log.flag(name, o, a < 0, K, "order_amount", "order_amount is negative", "Accuracy",
             "Critical", "order_amount >= 0", detail=a.astype(str))
    log.flag(name, o, (d < 0) | (d > 100), K, "discount_pct", "discount_pct outside 0-100",
             "Validity", "Critical", "0 <= discount_pct <= 100", detail=d.astype(str))
    expected = (q * p * (1 - d.fillna(0) / 100)).round(2)
    mismatch = a.notna() & expected.notna() & ((a - expected).abs() > AMOUNT_TOLERANCE)
    log.flag(name, o, mismatch, K, "order_amount",
             "order_amount does not match quantity x unit_price x (1 - discount_pct/100)",
             "Accuracy", "Critical", "recompute and compare, tolerance 0.01",
             detail="reported " + a.astype(str) + " vs expected " + expected.astype(str))

    mrp = clean(o["product_id"]).map(ref["product_mrp"])
    log.flag(name, o, p.notna() & mrp.notna() & (p > mrp + AMOUNT_TOLERANCE), K, "unit_price",
             "unit_price is above the product's MRP", "Consistency", "Warning",
             "unit_price <= products.mrp",
             detail=p.astype(str) + " > MRP " + mrp.astype(str))

    # Delivery time
    delivered = st == "DELIVERED"
    dm_raw = clean(o["delivery_minutes"])
    log.flag(name, o, delivered & dm_raw.isna(), K, "delivery_minutes",
             "DELIVERED order has no delivery_minutes", "Completeness", "Critical",
             "DELIVERED orders must have delivery_minutes")
    log.flag(name, o, dm <= 0, K, "delivery_minutes", "delivery_minutes zero or negative",
             "Accuracy", "Critical", "delivery_minutes > 0", detail=dm.astype(str))
    log.flag(name, o, dm >= DELIVERY_MAX, K, "delivery_minutes",
             f"delivery_minutes >= {DELIVERY_MAX} (implausible: hours or seconds?)",
             "Accuracy", "Critical", f"delivery_minutes < {DELIVERY_MAX}", detail=dm.astype(str))
    lo, hi = DELIVERY_NORMAL
    log.flag(name, o, (dm > 0) & (dm < DELIVERY_MAX) & ((dm < lo) | (dm > hi)), K,
             "delivery_minutes", f"delivery_minutes outside the normal {lo}-{hi} range",
             "Accuracy", "Warning", f"expected {lo}-{hi} minutes", detail=dm.astype(str))
    # RETURNED orders were delivered first, so they may legitimately keep delivery_minutes
    log.flag(name, o, (st == "CANCELLED") & dm_raw.notna(), K, "delivery_minutes",
             "delivery_minutes present on a CANCELLED order", "Consistency", "Warning",
             "cancelled orders should not carry delivery_minutes", detail=st)

    # Referential integrity
    for col, ids, master in [("customer_id", ref["customer_ids"], "customers.csv"),
                             ("product_id", ref["product_ids"], "products.csv"),
                             ("store_id", ref["store_ids"], "stores.csv")]:
        v = clean(o[col])
        unknown = v.notna() & ~v.isin(ids)
        fixable = unknown & v.str.upper().isin({i.upper() for i in ids})
        log.flag(name, o, unknown & ~fixable, K, col, f"{col} not found in {master}",
                 "Consistency", "Critical", f"{col} must exist in {master}", detail=v)
        log.flag(name, o, fixable, K, col, f"{col} matches {master} only after fixing case",
                 "Consistency", "Warning", "upper-case keys before joining", detail=v)

    s_open = clean(o["store_id"]).map(ref["store_open"])
    log.flag(name, o, ts.notna() & s_open.notna() & (ts < s_open), K, "order_ts",
             "order placed before the store's open_date", "Consistency", "Warning",
             "order_ts >= stores.open_date", detail=clean(o["store_id"]))
    p_launch = clean(o["product_id"]).map(ref["product_launch"])
    log.flag(name, o, ts.notna() & p_launch.notna() & (ts < p_launch), K, "order_ts",
             "order placed before the product's launch_date", "Consistency", "Warning",
             "order_ts >= products.launch_date", detail=clean(o["product_id"]))

    for col, v in [("quantity", q), ("unit_price", p), ("order_amount", a)]:
        flag_outliers(name, o, K, col, v, log)

    by_id = (pd.DataFrame({"id": oid, "ts": ts, "status": st, "mode": pm, "amount": a,
                           "customer_id": clean(o["customer_id"])})
             .dropna(subset=["id"]).drop_duplicates("id").set_index("id"))
    raw_by_id = (o.drop(columns="_row").apply(clean).assign(_id=oid)
                 .dropna(subset=["_id"]).drop_duplicates("_id").set_index("_id"))
    return dict(id=oid, ts=ts, status=st, mode=pm, amount=a, delivered=delivered,
                by_id=by_id, raw_by_id=raw_by_id)


def check_payments(p, orders_parsed, log, as_of):
    name, K = "payments", "payment_id"
    for col in ["order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status"]:
        log.flag(name, p, blank(p[col]), K, col, "required value missing", "Completeness",
                 "Critical", f"{col} must not be null")
    check_key_uniqueness(name, p, K, log)
    check_numeric_format(name, p, K, log)

    oid = clean(p["order_id"])
    info = orders_parsed["by_id"]
    log.flag(name, p, oid.notna() & ~oid.isin(info.index), K, "order_id",
             "payment for an order_id not in orders.csv", "Consistency", "Critical",
             "payments.order_id must exist in orders", detail=oid)

    pm_raw = clean(p["payment_mode"])
    pm = pm_raw.str.lower().str.replace(r"\s+", " ", regex=True).map(PAYMENT_MODE_MAP)
    log.flag(name, p, pm_raw.notna() & pm.isna(), K, "payment_mode",
             "payment_mode not in allowed list", "Validity", "Critical",
             "allowed: UPI, Card, Cash on Delivery, Wallet", detail=pm_raw)
    log.flag(name, p, pm.notna() & (pm_raw != pm), K, "payment_mode",
             "non-standard payment_mode spelling", "Validity", "Warning",
             "map variants to the canonical value", detail=pm_raw + " -> " + pm.fillna(""))

    ps = clean(p["payment_status"]).str.upper()
    success = ps == "SUCCESS"
    amt = to_num(p["amount_paid"])
    log.flag(name, p, amt <= 0, K, "amount_paid", "amount_paid zero or negative", "Accuracy",
             "Critical", "amount_paid > 0", detail=amt.astype(str))
    ts = check_dates(name, p, K, "payment_ts", log, as_of)

    o_amt, o_st = oid.map(info["amount"]), oid.map(info["status"])
    o_mode, o_ts = oid.map(info["mode"]), oid.map(info["ts"])
    log.flag(name, p, success & amt.notna() & o_amt.notna() & ((amt - o_amt).abs() > AMOUNT_TOLERANCE),
             K, "amount_paid", "SUCCESS payment amount does not equal order_amount",
             "Consistency", "Critical", "SUCCESS amount_paid = orders.order_amount",
             detail="paid " + amt.astype(str) + " vs order " + o_amt.astype(str))
    log.flag(name, p, success & (o_st == "CANCELLED"), K, "order_id",
             "SUCCESS payment recorded against a CANCELLED order", "Consistency", "Critical",
             "cancelled orders have no payment", detail=oid)
    succ_ids = oid.where(success)
    n_success = succ_ids.map(succ_ids.value_counts())
    log.flag(name, p, success & (n_success > 1), K, "order_id",
             "more than one SUCCESS payment for the same order (double charge?)",
             "Uniqueness", "Critical", "one SUCCESS payment per order", detail=oid)
    log.flag(name, p, pm.notna() & o_mode.notna() & (pm != o_mode), K, "payment_mode",
             "payment_mode differs from the order's payment_mode", "Consistency", "Warning",
             "payment_mode should match the order",
             detail=pm.astype(str) + " vs order " + o_mode.astype(str))
    log.flag(name, p, ts.notna() & o_ts.notna() & (ts < o_ts), K, "payment_ts",
             "payment_ts is before order_ts", "Consistency", "Warning",
             "payment_ts >= orders.order_ts")
    gref = clean(p["gateway_ref"])
    log.flag(name, p, success & gref.isna(), K, "gateway_ref", "SUCCESS payment has no gateway_ref",
             "Completeness", "Warning", "SUCCESS payments need a gateway_ref")
    log.flag(name, p, gref.notna() & gref.duplicated(keep=False) & ~p.drop(columns="_row").duplicated(keep=False),
             K, "gateway_ref", "gateway_ref reused by different payments", "Uniqueness",
             "Critical", "gateway_ref must be unique", detail=gref)
    flag_outliers(name, p, K, "amount_paid", amt, log)

    return dict(success_order_ids=set(oid[success].dropna()),
                payment_statuses=ps.value_counts(dropna=False))


def check_customers(c, first_order_ts, log, as_of):
    name, K = "customers", "customer_id"
    check_key_uniqueness(name, c, K, log)
    for col in ["full_name", "email", "phone", "loyalty_tier", "city"]:
        log.flag(name, c, blank(c[col]), K, col, f"{col} missing", "Completeness", "Warning",
                 f"{col} should be filled")
    ids = clean(c[K])

    email = clean(c["email"])
    log.flag(name, c, email.notna() & ~rx(email, EMAIL_RE), K, "email", "email format invalid",
             "Validity", "Warning", "email must look like name@domain.tld")
    el = email.str.lower()
    tmp = pd.DataFrame({"id": ids, "v": el}).dropna()
    if len(tmp):
        n = tmp.groupby("v")["id"].transform("nunique")
        log.flag(name, c, (n > 1).reindex(c.index, fill_value=False), K, "email",
                 "same email on more than one customer_id (duplicate customer?)",
                 "Uniqueness", "Warning", "one customer_id per person; ask client how to merge")

    ph = clean(c["phone"])
    digits = ph.str.replace(r"\D", "", regex=True)
    norm = digits.where(~((digits.str.len() == 12) & digits.str.startswith("91")), digits.str[2:])
    norm = norm.where(~((norm.str.len() == 11) & norm.str.startswith("0")), norm.str[1:])
    valid = rx(norm, r"^[6-9]\d{9}$")
    n_digits = norm.str.len().astype("Int64").astype(str)
    log.flag(name, c, ph.notna() & ~valid, K, "phone", "phone is not a valid 10-digit Indian mobile",
             "Validity", "Warning", "10 digits starting 6-9 after removing +91/0",
             detail="digits after cleaning: " + n_digits, pii_safe=True)
    log.flag(name, c, valid & (ph != norm), K, "phone", "phone needs normalising (+91, spaces, dashes)",
             "Validity", "Warning", "store phone as 10 digits")
    tmp = pd.DataFrame({"id": ids, "v": norm.where(valid)}).dropna()
    if len(tmp):
        n = tmp.groupby("v")["id"].transform("nunique")
        log.flag(name, c, (n > 1).reindex(c.index, fill_value=False), K, "phone",
                 "same phone on more than one customer_id", "Uniqueness", "Warning",
                 "one customer_id per person")

    pin = clean(c["pincode"])
    six = rx(pin, r"^\d{6}$")
    log.flag(name, c, pin.notna() & ~six, K, "pincode", "pincode is not 6 digits", "Validity",
             "Warning", "pincode must be exactly 6 digits", detail=pin)
    log.flag(name, c, six & ~pin.str.startswith("56"), K, "pincode",
             "pincode outside Bengaluru (56xxxx)", "Consistency", "Warning",
             "Bengaluru pincodes start with 56", detail=pin)

    sd = check_dates(name, c, K, "signup_date", log, as_of, severity_unparseable="Warning")
    first = ids.map(first_order_ts)
    log.flag(name, c, sd.notna() & first.notna() & (sd > first), K, "signup_date",
             "signup_date is after the customer's first order", "Consistency", "Warning",
             "signup_date <= first order_ts", detail=clean(c["signup_date"]))

    act = clean(c["is_active"])
    log.flag(name, c, act.notna() & ~act.str.lower().isin(BOOLEAN_TOKENS), K, "is_active",
             "is_active is not a boolean", "Validity", "Warning", "is_active must be true/false",
             detail=act)


def check_products(pr, log, as_of):
    name, K = "products", "product_id"
    check_key_uniqueness(name, pr, K, log)
    log.flag(name, pr, blank(pr["category"]), K, "category", "category missing", "Completeness",
             "Critical", "category must not be null")
    for col in ["product_name", "brand", "sub_category", "unit"]:
        log.flag(name, pr, blank(pr[col]), K, col, f"{col} missing", "Completeness", "Warning",
                 f"{col} should be filled")

    ids = clean(pr[K])
    nb = (clean(pr["product_name"]).str.lower() + "|" + clean(pr["brand"]).fillna("").str.lower())
    tmp = pd.DataFrame({"id": ids, "v": nb}).dropna()
    if len(tmp):
        n = tmp.groupby("v")["id"].transform("nunique")
        log.flag(name, pr, (n > 1).reindex(pr.index, fill_value=False), K, "product_name",
                 "same product_name + brand under more than one product_id", "Uniqueness",
                 "Warning", "one product_id per product; ask client how to merge",
                 detail=clean(pr["product_name"]))

    check_numeric_format(name, pr, K, log)
    mrp, cost = to_num(pr["mrp"]), to_num(pr["cost_price"])
    log.flag(name, pr, blank(pr["mrp"]), K, "mrp", "mrp missing", "Completeness", "Critical",
             "mrp must not be null")
    log.flag(name, pr, blank(pr["cost_price"]), K, "cost_price", "cost_price missing",
             "Completeness", "Critical", "cost_price is needed for margin")
    log.flag(name, pr, mrp <= 0, K, "mrp", "mrp zero or negative", "Accuracy", "Critical",
             "mrp > 0", detail=mrp.astype(str))
    log.flag(name, pr, cost <= 0, K, "cost_price", "cost_price zero or negative", "Accuracy",
             "Critical", "cost_price > 0", detail=cost.astype(str))
    log.flag(name, pr, cost > mrp, K, "cost_price", "cost_price is higher than mrp", "Accuracy",
             "Critical", "cost_price <= mrp",
             detail="cost " + cost.astype(str) + " > mrp " + mrp.astype(str))
    check_dates(name, pr, K, "launch_date", log, as_of, severity_unparseable="Warning")
    per = clean(pr["is_perishable"])
    log.flag(name, pr, per.notna() & ~per.str.lower().isin(BOOLEAN_TOKENS), K, "is_perishable",
             "is_perishable is not a boolean", "Validity", "Warning", "true/false only", detail=per)
    flag_outliers(name, pr, K, "mrp", mrp, log)


def check_stores(s, log, as_of):
    name, K = "stores", "store_id"
    check_key_uniqueness(name, s, K, log)
    for col in ["store_name", "zone", "pincode"]:
        log.flag(name, s, blank(s[col]), K, col, f"{col} missing", "Completeness", "Critical",
                 f"{col} must not be null")
    z_raw = clean(s["zone"])
    z = z_raw.str.title()
    log.flag(name, s, z_raw.notna() & ~z.isin(VALID_ZONES), K, "zone", "zone not in allowed list",
             "Validity", "Critical", "allowed: North, South, East, West", detail=z_raw)
    log.flag(name, s, z.isin(VALID_ZONES) & (z_raw != z), K, "zone", "zone needs standard casing",
             "Consistency", "Warning", "standardise to Title case", detail=z_raw)
    pin = clean(s["pincode"])
    six = rx(pin, r"^\d{6}$")
    log.flag(name, s, pin.notna() & ~six, K, "pincode", "pincode is not 6 digits", "Validity",
             "Warning", "pincode must be exactly 6 digits", detail=pin)
    log.flag(name, s, six & ~pin.str.startswith("56"), K, "pincode",
             "pincode outside Bengaluru (56xxxx)", "Consistency", "Warning",
             "Bengaluru pincodes start with 56", detail=pin)
    check_numeric_format(name, s, K, log)
    cap = to_num(s["capacity_orders_per_day"])
    log.flag(name, s, cap <= 0, K, "capacity_orders_per_day", "capacity zero or negative",
             "Accuracy", "Critical", "capacity_orders_per_day > 0", detail=cap.astype(str))
    check_dates(name, s, K, "open_date", log, as_of, severity_unparseable="Warning")


def analyse_increment(inc, inc_parsed, orders_parsed, log, as_of):
    name, K = "orders_increment", "order_id"
    ids = inc_parsed["id"]
    base, raw_base = orders_parsed["by_id"], orders_parsed["raw_by_id"]
    existing = ids.notna() & ids.isin(base.index)
    new = ids.notna() & ~existing

    ts = inc_parsed["ts"]
    next_day = as_of + pd.Timedelta(days=1)
    log.flag(name, inc, new & ts.notna() & ((ts < as_of) | (ts >= next_day)), K, "order_ts",
             f"new order not dated {as_of:%Y-%m-%d}", "Timeliness", "Warning",
             f"new orders in the increment should be dated {as_of:%Y-%m-%d}",
             detail=clean(inc["order_ts"]))

    old_status = ids.map(base["status"])
    new_status = inc_parsed["status"]
    status_changed = existing & (new_status != old_status)
    log.flag(name, inc, existing & ~status_changed, K, "order_status",
             "correction row for an existing order, but status did not change",
             "Consistency", "Warning", "confirm why the row was re-sent")

    transition = old_status.fillna("?") + " -> " + new_status.fillna("?")
    odd = status_changed & (transition != "DELIVERED -> RETURNED")
    log.flag(name, inc, odd, K, "order_status", "unexpected status transition",
             "Consistency", "Warning", "only DELIVERED -> RETURNED expected", detail=transition)

    inc_clean = inc.drop(columns="_row").apply(clean)
    compare_cols = [c for c in ORDER_COLS if c not in ("order_id", "order_status", "delivery_minutes")]
    changed_cols = pd.Series("", index=inc.index)
    for c in compare_cols:
        old = ids.map(raw_base[c])
        diff = existing & (inc_clean[c].fillna("") != old.fillna(""))
        changed_cols = changed_cols.where(~diff, changed_cols + c + " ")
    other = existing & (changed_cols != "")
    log.flag(name, inc, other, K, "order_id", "correction also changes fields other than order_status",
             "Consistency", "Warning", "ask client whether corrections may change amounts/dates",
             detail=changed_cols.str.strip())

    return dict(rows=len(inc), new=int(new.sum()), corrections=int(existing.sum()),
                status_changes=int(status_changed.sum()),
                transitions=transition[status_changed].value_counts(),
                other_field_changes=int(other.sum()))


# ---------------------------------------------------------------------------
# Profiling (descriptive, not rule-based)
# ---------------------------------------------------------------------------
def profile_file(name, df, key) -> str:
    cols = [c for c in df.columns if c != "_row"]
    out = [f"## {FILES[name][0]}\n",
           f"Rows: **{len(df):,}** · Columns: **{len(cols)}** · Key: `{key}`\n"]

    k = clean(df[key])
    out.append(f"Duplicate `{key}` values: **{int(k.dropna().duplicated().sum())}** · "
               f"Exact duplicate rows: **{int(df.drop(columns='_row').duplicated().sum())}** · "
               f"Blank keys: **{int(k.isna().sum())}**\n")

    rows = []
    for c in cols:
        s, b = df[c], blank(df[c])
        nb = clean(s)
        sample = ("(personal data hidden)" if c in PII_COLUMNS
                  else ", ".join(v[:25] for v in nb.dropna().unique()[:3]))
        rows.append(dict(column=c, nulls=int(b.sum()), null_pct=f"{b.mean() * 100:.1f}%",
                         distinct=int(nb.nunique()),
                         whitespace=int(((s.astype(str) != s.astype(str).str.strip()) & ~b).sum()),
                         sample=sample))
    out += ["\n### Columns\n", md_table(pd.DataFrame(rows))]

    num_rows = []
    for c in NUMERIC_COLUMNS[name]:
        raw, v = clean(df[c]), to_num(df[c])
        num_rows.append(dict(column=c, non_numeric=int((raw.notna() & v.isna()).sum()),
                             min=fmt(v.min()), median=fmt(v.median()), max=fmt(v.max()),
                             mean=fmt(v.mean()), negatives=int((v < 0).sum()),
                             zeros=int((v == 0).sum())))
    if num_rows:
        out += ["\n### Numeric columns\n", md_table(pd.DataFrame(num_rows))]

    date_rows = []
    for c in DATE_COLUMNS[name]:
        raw, ts = clean(df[c]), to_ts(df[c])
        date_rows.append(dict(column=c, unparseable=int((raw.notna() & ts.isna()).sum()),
                              non_iso=int((raw.notna() & ts.notna() & ~rx(raw, ISO_TS)).sum()),
                              min=fmt(ts.min()), max=fmt(ts.max())))
    out += ["\n### Date columns\n", md_table(pd.DataFrame(date_rows))]

    for c in CATEGORY_COLUMNS[name]:
        vc = df[c].map(repr).value_counts()  # repr() exposes stray spaces: 'UPI '
        extra = f" (showing 25 of {len(vc)})" if len(vc) > 25 else ""
        out += [f"\n### Distinct values: `{c}`{extra}\n",
                md_table(vc.head(25).rename_axis("value").reset_index(name="rows"))]
    return "\n".join(out)


def ri_summary(frames) -> pd.DataFrame:
    o, p = frames["orders"], frames["payments"]
    rels = [("orders.customer_id -> customers", o, "customer_id", frames["customers"], "customer_id"),
            ("orders.product_id -> products", o, "product_id", frames["products"], "product_id"),
            ("orders.store_id -> stores", o, "store_id", frames["stores"], "store_id"),
            ("payments.order_id -> orders", p, "order_id", o, "order_id")]
    if "orders_increment" in frames:
        inc = frames["orders_increment"]
        rels += [("increment.customer_id -> customers", inc, "customer_id", frames["customers"], "customer_id"),
                 ("increment.product_id -> products", inc, "product_id", frames["products"], "product_id"),
                 ("increment.store_id -> stores", inc, "store_id", frames["stores"], "store_id")]
    rows = []
    for label, child, ccol, parent, pcol in rels:
        v = clean(child[ccol])
        orphans = v.notna() & ~v.isin(set(clean(parent[pcol]).dropna()))
        rows.append(dict(relationship=label, child_rows=len(child), blank_keys=int(v.isna().sum()),
                         orphan_rows=int(orphans.sum()),
                         sample_orphans=", ".join(v[orphans].unique()[:5])))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Profile NammaMart CSV exports (Part 0).")
    ap.add_argument("--data-dir", default="include/data/nammamart")
    ap.add_argument("--out-dir", default="include/reports/profiling")
    ap.add_argument("--as-of", default="2026-10-01",
                    help="the 'run date' used for future/stale checks (default 2026-10-01)")
    ap.add_argument("--period-start", default="2026-09-01",
                    help="first day covered by orders.csv (default 2026-09-01)")
    args = ap.parse_args()

    data_dir, out_dir = Path(args.data_dir), Path(args.out_dir)
    as_of = pd.Timestamp(args.as_of)
    period_start = pd.Timestamp(args.period_start)
    out_dir.mkdir(parents=True, exist_ok=True)

    frames, raw_cols = {}, {}
    for name, (fname, _) in FILES.items():
        path = data_dir / fname
        if path.exists():
            frames[name], raw_cols[name] = load(path)
        elif name != "orders_increment":
            print(f"ERROR: required file missing: {path}", file=sys.stderr)
            return 1
        else:
            print(f"note: {fname} not found; skipping increment analysis")

    log = IssueLog()

    # 1. Schema and generic text checks on every file
    for name, df in frames.items():
        check_schema(name, df, raw_cols[name], log)
        check_text_hygiene(name, df, FILES[name][1], log)
        check_casing(name, df, FILES[name][1], log)

    # 2. Reference data for cross-file checks
    cu, pr, st = frames["customers"], frames["products"], frames["stores"]
    ref = dict(
        customer_ids=set(clean(cu["customer_id"]).dropna()),
        product_ids=set(clean(pr["product_id"]).dropna()),
        store_ids=set(clean(st["store_id"]).dropna()),
        product_mrp=first_by_key(pr, "product_id", to_num(pr["mrp"])),
        product_launch=first_by_key(pr, "product_id", to_ts(pr["launch_date"])),
        store_open=first_by_key(st, "store_id", to_ts(st["open_date"])),
    )

    # 3. File-specific rules
    orders = frames["orders"]
    op = check_orders("orders", orders, ref, log, as_of, window=(period_start, as_of))
    pay = check_payments(frames["payments"], op, log, as_of)
    log.flag("orders", orders,
             op["delivered"] & op["id"].notna() & ~op["id"].isin(pay["success_order_ids"]),
             "order_id", "order_id", "DELIVERED order has no SUCCESS payment", "Consistency",
             "Critical", "every DELIVERED order needs a SUCCESS payment (ask: how is COD recorded?)",
             detail=op["mode"])
    first_order = (pd.DataFrame({"c": clean(orders["customer_id"]), "ts": op["ts"]})
                   .dropna().groupby("c")["ts"].min())
    check_customers(cu, first_order, log, as_of)
    check_products(pr, log, as_of)
    check_stores(st, log, as_of)

    latest = op["ts"].max()
    if pd.notna(latest) and (as_of - latest) > pd.Timedelta(days=1):
        log.note("orders", "order_ts", "latest order is more than 1 day older than the run date",
                 "Timeliness", "Warning", "latest order_ts within 1 day of run date",
                 detail=f"latest {latest}, as-of {as_of:%Y-%m-%d}")

    inc_summary = None
    if "orders_increment" in frames:
        inc = frames["orders_increment"]
        # No date window here: correction rows keep their original September order_ts.
        # analyse_increment checks the window for genuinely new orders only.
        ip = check_orders("orders_increment", inc, ref, log, as_of, window=None)
        inc_summary = analyse_increment(inc, ip, op, log, as_of)

    # 4. Write outputs
    issues = log.frame()
    issues["_sev"] = issues["severity"].map(SEVERITY_ORDER)
    issues = issues.sort_values(["file", "_sev", "column", "problem"]).drop(columns="_sev")
    issues.to_csv(out_dir / "data_issues_log.csv", index=False)

    group_cols = ["file", "column", "problem", "dimension", "severity", "proposed_rule"]
    summary = (issues.groupby(group_cols, dropna=False)
               .agg(rows_affected=("row_key", "size"),
                    sample_keys=("row_key", lambda s: ", ".join(pd.unique(s)[:5])))
               .reset_index())
    summary["_sev"] = summary["severity"].map(SEVERITY_ORDER)
    summary = (summary.sort_values(["file", "_sev", "rows_affected"], ascending=[True, True, False])
               .drop(columns="_sev"))
    summary = summary[["file", "column", "problem", "dimension", "severity", "rows_affected",
                       "sample_keys", "proposed_rule"]]
    summary.to_csv(out_dir / "issues_summary.csv", index=False)

    overview = []
    for name, df in frames.items():
        f = issues[issues["file"] == FILES[name][0]]
        rows_with_critical = f.loc[(f.severity == "Critical") & (f.source_row != ""), "source_row"].nunique()
        overview.append(dict(file=FILES[name][0], rows=len(df), columns=len(df.columns) - 1,
                             issue_types=int(summary[summary.file == FILES[name][0]].shape[0]),
                             critical_flags=int((f.severity == "Critical").sum()),
                             warning_flags=int((f.severity == "Warning").sum()),
                             rows_with_a_critical_issue=int(rows_with_critical)))
    overview = pd.DataFrame(overview)
    by_dim = (pd.crosstab(issues["file"], issues["dimension"]).reset_index()
              if len(issues) else pd.DataFrame())

    md = ["# NammaMart — Data Profiling Report (Part 0)\n",
          f"Data folder: `{data_dir}` · As-of date: **{as_of:%Y-%m-%d}** · "
          f"Generated: {pd.Timestamp.now():%Y-%m-%d %H:%M}\n",
          f"Total rows profiled: **{sum(len(d) for d in frames.values()):,}** · "
          f"Issue flags: **{len(issues):,}** across **{len(summary)}** issue types\n",
          "\n## Overview\n", md_table(overview),
          "\n## Issue flags by quality dimension\n", md_table(by_dim),
          "\n## Referential integrity\n", md_table(ri_summary(frames)),
          "\n## Payment status values\n",
          md_table(pay["payment_statuses"].rename_axis("payment_status").reset_index(name="rows"))]
    if inc_summary:
        md += ["\n## Increment file vs orders.csv\n",
               f"- Rows: **{inc_summary['rows']}** (brief says 160)\n"
               f"- New order_ids: **{inc_summary['new']}** (brief says 150)\n"
               f"- Rows for existing orders (corrections): **{inc_summary['corrections']}** (brief says 10)\n"
               f"- Corrections that change status: **{inc_summary['status_changes']}**\n"
               f"- Corrections that change other fields too: **{inc_summary['other_field_changes']}**\n",
               "\nStatus transitions:\n",
               md_table(inc_summary["transitions"].rename_axis("transition").reset_index(name="rows"))]
    md += ["\n## Issues summary (paste into SUBMISSION.md)\n", md_table(summary), "\n---\n"]
    md += [profile_file(name, df, FILES[name][1]) for name, df in frames.items()]
    (out_dir / "profile_summary.md").write_text("\n".join(md), encoding="utf-8")

    # 5. Console summary
    pd.set_option("display.width", 200, "display.max_columns", 20, "display.max_colwidth", 60)
    print("\n=== NammaMart profiling overview ===")
    print(overview.to_string(index=False))
    if not by_dim.empty:
        print("\n=== Issue flags by dimension ===")
        print(by_dim.to_string(index=False))
    clean_files = overview.loc[overview.issue_types == 0, "file"].tolist()
    if clean_files:
        print(f"\nNo issues found in: {', '.join(clean_files)} — profile these by hand too.")
    print("\nTop issue types:")
    print(summary.sort_values("rows_affected", ascending=False)
          .head(15)[["file", "column", "problem", "severity", "rows_affected"]].to_string(index=False))
    print(f"\nWrote:\n  {out_dir / 'profile_summary.md'}\n  {out_dir / 'data_issues_log.csv'}\n"
          f"  {out_dir / 'issues_summary.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
