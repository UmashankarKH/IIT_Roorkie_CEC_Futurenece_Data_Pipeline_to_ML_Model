#!/usr/bin/env python3
"""
NammaMart — shared data quality rules (Part D)
==============================================

ONE place that defines every data quality check, used by BOTH pipelines:

  * ETL (pandas):  standardise() -> build_context() -> apply_flags() -> dq_results()
  * ELT (DuckDB):  sql_reference_views() -> sql_flagged_select() -> sql_dq_results()

Every rule carries two implementations of the same condition:
  py  - a pandas expression, True where the row FAILS
  sql - a DuckDB SQL expression, TRUE where the row FAILS

Running this file directly is the PARITY TEST: it applies both versions to the
same data and proves they flag exactly the same rows. If ETL and ELT ever
disagree, this test tells you which rule drifted.

    python include/dq_rules.py                     # parity test on the real data
    python include/dq_rules.py --markdown          # rule catalogue for SUBMISSION.md
    python include/dq_rules.py --as-of 2026-10-02 --include-increment
    python include/dq_rules.py --write-sql include/sql/staging/generated   # after changing a rule

How severities are used (see the Part 0 issues log):
  Critical -> the row is quarantined (ETL) / dq_status = 'FAIL' (ELT)
  Warning  -> the row loads, the issue is logged / dq_status = 'WARN'
The `action` column says what happens next:
  QUARANTINE  bad row kept out of reporting tables
  DEDUPE      a duplicate copy is quarantined, one version is kept
  REPAIR      row loads, the bad attribute becomes NULL / 'UNKNOWN'
  STANDARDISE row loads with the value cleaned (casing, spaces)
  RECONCILE   row loads and appears in the payment reconciliation table
  LOG         row loads, the issue is only recorded
  ALERT       dataset-level check, raises a warning in dq_results

Import from a DAG:   from include.dq_rules import standardise, apply_flags, ...
Personal data: this module never prints names, emails or phone numbers.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from string import Template
from typing import Callable

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Business constants — the single source of truth for both pipelines
# ---------------------------------------------------------------------------
VALID_ORDER_STATUS = ("DELIVERED", "CANCELLED", "RETURNED")
VALID_PAYMENT_STATUS = ("SUCCESS", "FAILED", "REFUNDED")
PAYMENT_MODE_MAP = {  # lower-case, single-spaced variant -> canonical
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "wallet": "Wallet",
}  # 'Cashh' is deliberately NOT mapped: its payments carry gateway refs (DI-08)
GATEWAY_MODES = ("UPI", "Card", "Wallet")  # COD never goes through the gateway (DI-26)
VALID_ZONES = ("North", "South", "East", "West")
VALID_TIERS = ("Bronze", "Silver", "Gold", "Platinum")
NULL_TOKENS = {"", "null", "none", "nan", "na", "n/a", "-"}

DELIVERY_MAX_MINUTES = 120
DELIVERY_NORMAL = (8, 35)
AMOUNT_TOLERANCE = 0.01
EMAIL_RE = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
PHONE_RE = r"[6-9][0-9]{9}"
PINCODE_RE = r"[0-9]{6}"

FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "orders_increment": "orders_2026-10-01_increment.csv",
}
RULE_FILE = {"orders_increment": "orders"}  # the increment uses the orders rules


# ---------------------------------------------------------------------------
# 1. Standardise: raw text -> typed, trimmed, canonical columns
#    (ELT staging SQL must produce exactly these columns; see COLUMN_CONTRACT)
# ---------------------------------------------------------------------------
def read_raw(path: Path | str) -> pd.DataFrame:
    """Read a CSV exactly as received: every column as text, plus _row (CSV line number)."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    df.insert(0, "_row", range(2, len(df) + 2))
    return df


def _raw(s: pd.Series) -> pd.Series:
    """Original value (untrimmed) or NA when blank/placeholder."""
    s = s.astype("string")
    return s.where(~s.str.strip().str.lower().isin(NULL_TOKENS), pd.NA)


def _text(s: pd.Series) -> pd.Series:
    """Trimmed value or NA."""
    return _raw(s).str.strip()


def _num(s: pd.Series) -> pd.Series:
    t = _text(s)
    return pd.to_numeric(t.astype(object).where(t.notna(), None), errors="coerce").astype("Float64")


def _ts(s: pd.Series) -> pd.Series:
    """Strict ISO-8601 parsing: '31/09/2026' and '2026-13-05' become NaT (DI-04)."""
    t = _text(s)
    return pd.to_datetime(t.astype(object).where(t.notna(), None), errors="coerce", format="ISO8601")


def _canon(s: pd.Series, mapping: dict) -> pd.Series:
    key = _text(s).str.lower().str.replace(r"\s+", " ", regex=True)
    return key.map(mapping).astype("string")


def _phone_norm(s: pd.Series) -> pd.Series:
    d = _text(s).str.replace(r"[^0-9]", "", regex=True)
    d = d.where(~((d.str.len() == 12) & d.str.startswith("91")), d.str.slice(2))
    d = d.where(~((d.str.len() == 11) & d.str.startswith("0")), d.str.slice(1))
    return d


def standardise(file: str, raw: pd.DataFrame) -> pd.DataFrame:
    """Typed, cleaned frame for one file. Never drops or reorders rows."""
    f = RULE_FILE.get(file, file)
    r = raw
    if f == "orders":
        status_map = {v.lower(): v for v in VALID_ORDER_STATUS}
        return pd.DataFrame({
            "_row": r["_row"],
            "order_id": _text(r["order_id"]),
            "order_ts": _ts(r["order_ts"]),
            "order_ts_raw": _raw(r["order_ts"]),
            "customer_id": _text(r["customer_id"]),
            "store_id": _text(r["store_id"]),
            "product_id": _text(r["product_id"]),
            "quantity": _num(r["quantity"]),
            "unit_price": _num(r["unit_price"]),
            "discount_pct": _num(r["discount_pct"]),
            "order_amount": _num(r["order_amount"]),
            "payment_mode": _canon(r["payment_mode"], PAYMENT_MODE_MAP),
            "payment_mode_raw": _raw(r["payment_mode"]),
            "order_status": _canon(r["order_status"], status_map),
            "order_status_raw": _raw(r["order_status"]),
            "channel": _text(r["channel"]),
            "delivery_minutes": _num(r["delivery_minutes"]),
        })
    if f == "payments":
        status_map = {v.lower(): v for v in VALID_PAYMENT_STATUS}
        return pd.DataFrame({
            "_row": r["_row"],
            "payment_id": _text(r["payment_id"]),
            "order_id": _text(r["order_id"]),
            "payment_ts": _ts(r["payment_ts"]),
            "payment_ts_raw": _raw(r["payment_ts"]),
            "payment_mode": _canon(r["payment_mode"], PAYMENT_MODE_MAP),
            "payment_mode_raw": _raw(r["payment_mode"]),
            "amount_paid": _num(r["amount_paid"]),
            "payment_status": _canon(r["payment_status"], status_map),
            "payment_status_raw": _raw(r["payment_status"]),
            "gateway_ref": _text(r["gateway_ref"]),
        })
    if f == "customers":
        return pd.DataFrame({
            "_row": r["_row"],
            "customer_id": _text(r["customer_id"]),
            "full_name": _text(r["full_name"]),
            "email": _text(r["email"]),
            "phone": _text(r["phone"]),
            "phone_norm": _phone_norm(r["phone"]),
            "city": _text(r["city"]),
            "area": _text(r["area"]),
            "pincode": _text(r["pincode"]),
            "signup_date": _ts(r["signup_date"]),
            "signup_date_raw": _raw(r["signup_date"]),
            "loyalty_tier": _canon(r["loyalty_tier"], {v.lower(): v for v in VALID_TIERS}),
            "loyalty_tier_raw": _raw(r["loyalty_tier"]),
            "is_active": _text(r["is_active"]),
        })
    if f == "products":
        return pd.DataFrame({
            "_row": r["_row"],
            "product_id": _text(r["product_id"]),
            "product_name": _text(r["product_name"]),
            "category": _text(r["category"]),
            "sub_category": _text(r["sub_category"]),
            "brand": _text(r["brand"]),
            "mrp": _num(r["mrp"]),
            "cost_price": _num(r["cost_price"]),
            "unit": _text(r["unit"]),
            "is_perishable": _text(r["is_perishable"]),
            "launch_date": _ts(r["launch_date"]),
        })
    if f == "stores":
        return pd.DataFrame({
            "_row": r["_row"],
            "store_id": _text(r["store_id"]),
            "store_name": _text(r["store_name"]),
            "zone": _canon(r["zone"], {v.lower(): v for v in VALID_ZONES}),
            "zone_raw": _raw(r["zone"]),
            "area": _text(r["area"]),
            "pincode": _text(r["pincode"]),
            "open_date": _ts(r["open_date"]),
            "manager_name": _text(r["manager_name"]),
            "capacity_orders_per_day": _num(r["capacity_orders_per_day"]),
        })
    raise ValueError(f"unknown file: {file}")


COLUMN_CONTRACT = """\
Staging columns both pipelines produce before the rules run:
  *_raw         original text (untrimmed), NULL when blank or a placeholder (NULL, N/A, -)
  ids / text    trimmed text, NULL when blank or a placeholder
  numbers       DOUBLE; NULL when not a number          (SQL: TRY_CAST(... AS DOUBLE))
  timestamps    strict ISO-8601; NULL when invalid      (SQL: TRY_CAST(... AS TIMESTAMP))
  order_status, payment_status, payment_mode, loyalty_tier, zone
                canonical value, or NULL if the value is not in the allowed list
  phone_norm    digits only, leading 91 (12 digits) or 0 (11 digits) removed
  _row          source CSV line number (header = 1); defines "first occurrence"
"""


# ---------------------------------------------------------------------------
# 2. Context: reference data the cross-file rules need
# ---------------------------------------------------------------------------
def build_context(std: dict[str, pd.DataFrame], as_of: str | pd.Timestamp) -> dict:
    as_of = pd.Timestamp(as_of)
    o, p = std["orders"], std["payments"]
    first = o[o["order_id"].notna()].drop_duplicates("order_id", keep="first")
    success = p["payment_status"].eq("SUCCESS").fillna(False).astype(bool)
    return {
        "as_of": as_of,
        "next_day": as_of + pd.Timedelta(days=1),      # anything at/after this is "future"
        "stale_before": as_of - pd.Timedelta(days=1),  # latest order older than this is stale
        "order_ids": set(first["order_id"]),
        "order_amounts": pd.Series(first["order_amount"].to_numpy(), index=first["order_id"].to_numpy()),
        "customer_ids": set(std["customers"]["customer_id"].dropna()),
        "product_ids": set(std["products"]["product_id"].dropna()),
        "store_ids": set(std["stores"]["store_id"].dropna()),
        "success_order_ids": set(p.loc[success, "order_id"].dropna()),
        "product_mrp": _current_mrp(std["products"]),
    }


def _current_mrp(pr: pd.DataFrame) -> pd.Series:
    """MRP of the current catalogue row per product (the row product_id_unique keeps); MRP <= 0 ignored."""
    keep = pr[~_dup_product(pr, None) & pr["product_id"].notna()]
    keep = keep[(keep["mrp"] > 0).fillna(False)]
    return pd.Series(keep["mrp"].to_numpy(), index=keep["product_id"].to_numpy())


def sql_reference_views(tables: dict[str, str], prefix: str = "ref_") -> list[str]:
    """CREATE VIEW statements that mirror build_context() in SQL.

    tables maps 'orders'/'payments'/'customers'/'products'/'stores' to the
    staging table names, e.g. {'orders': 'stg_orders_std', ...}.
    """
    t = tables
    return [
        f"""CREATE OR REPLACE VIEW {prefix}orders AS
            SELECT order_id, order_amount FROM {t['orders']}
            WHERE order_id IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _row) = 1""",
        f"""CREATE OR REPLACE VIEW {prefix}payments AS
            SELECT order_id, payment_status FROM {t['payments']} WHERE order_id IS NOT NULL""",
        f"""CREATE OR REPLACE VIEW {prefix}customers AS
            SELECT DISTINCT customer_id FROM {t['customers']} WHERE customer_id IS NOT NULL""",
        f"""CREATE OR REPLACE VIEW {prefix}products AS
            SELECT DISTINCT product_id FROM {t['products']} WHERE product_id IS NOT NULL""",
        f"""CREATE OR REPLACE VIEW {prefix}stores AS
            SELECT DISTINCT store_id FROM {t['stores']} WHERE store_id IS NOT NULL""",
        f"""CREATE OR REPLACE VIEW {prefix}product_mrp AS
            SELECT product_id, mrp FROM {t['products']}
            WHERE product_id IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY product_id ORDER BY
                    CASE WHEN product_name LIKE '%(old)' THEN 1 ELSE 0 END, _row) = 1
                AND mrp > 0""",
    ]


# ---------------------------------------------------------------------------
# 3. The rules
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Rule:
    name: str
    file: str
    dimension: str
    severity: str
    action: str
    description: str
    issues: str
    py: Callable[[pd.DataFrame, dict], pd.Series | bool]
    sql: str               # $placeholders: next_day, stale_before, table, ref_*
    level: str = "row"     # "row" or "dataset"


def _is(s, value):
    return s.eq(value).fillna(False).astype(bool)


def _dup_first(d, key, ascending=True):
    """True for every copy of `key` except the one kept (first by _row, or last)."""
    order = d.sort_values("_row", ascending=ascending)
    dup = order[key].duplicated(keep="first") & order[key].notna()
    return dup.reindex(d.index)


def _dup_product(d, c):
    """Keep the current catalogue row: prefer names NOT ending in '(old)', then first by _row."""
    is_old = d["product_name"].fillna("").str.endswith("(old)").astype(int)
    order = d.assign(_old=is_old).sort_values(["_old", "_row"])
    return (order["product_id"].duplicated(keep="first") & order["product_id"].notna()).reindex(d.index)


def _ts_invalid(col):
    return lambda d, c: d[f"{col}_raw"].notna() & d[col].isna()


def _ts_future(col):
    return lambda d, c: (d[col] >= c["next_day"]).fillna(False)


R = Rule  # short alias for the table below

RULES: list[Rule] = [
    # ------------------------------------------------------------- orders ---
    R("order_id_not_null", "orders", "Completeness", "Critical", "QUARANTINE",
      "order_id is present", "-",
      lambda d, c: d["order_id"].isna(),
      "t.order_id IS NULL"),
    R("order_customer_id_not_null", "orders", "Completeness", "Critical", "QUARANTINE",
      "customer_id is present", "DI-02",
      lambda d, c: d["customer_id"].isna(),
      "t.customer_id IS NULL"),
    R("order_ts_not_null", "orders", "Completeness", "Critical", "QUARANTINE",
      "order_ts is present", "DI-03",
      lambda d, c: d["order_ts_raw"].isna(),
      "t.order_ts_raw IS NULL"),
    R("delivered_has_delivery_minutes", "orders", "Completeness", "Critical", "QUARANTINE",
      "DELIVERED orders have delivery_minutes", "DI-15",
      lambda d, c: _is(d["order_status"], "DELIVERED") & d["delivery_minutes"].isna(),
      "t.order_status = 'DELIVERED' AND t.delivery_minutes IS NULL"),
    R("order_id_unique", "orders", "Uniqueness", "Critical", "DEDUPE",
      "only the first copy of an order_id loads", "DI-01",
      lambda d, c: _dup_first(d, "order_id"),
      "t.order_id IS NOT NULL AND row_number() OVER (PARTITION BY t.order_id ORDER BY t._row) > 1"),
    R("order_ts_valid", "orders", "Validity", "Critical", "QUARANTINE",
      "order_ts is a real ISO-8601 timestamp", "DI-04",
      _ts_invalid("order_ts"),
      "t.order_ts_raw IS NOT NULL AND t.order_ts IS NULL"),
    R("order_status_allowed", "orders", "Validity", "Critical", "QUARANTINE",
      "order_status is DELIVERED, CANCELLED or RETURNED", "DI-06",
      lambda d, c: d["order_status"].isna(),
      "t.order_status IS NULL"),
    R("order_payment_mode_allowed", "orders", "Validity", "Critical", "QUARANTINE",
      "payment_mode maps to UPI, Card, Cash on Delivery or Wallet", "DI-08",
      lambda d, c: d["payment_mode"].isna(),
      "t.payment_mode IS NULL"),
    R("discount_pct_in_range", "orders", "Validity", "Critical", "QUARANTINE",
      "discount_pct between 0 and 100 (blank = 0)", "DI-12",
      lambda d, c: d["discount_pct"].notna() & ((d["discount_pct"] < 0) | (d["discount_pct"] > 100)),
      "t.discount_pct IS NOT NULL AND (t.discount_pct < 0 OR t.discount_pct > 100)"),
    R("order_status_standard", "orders", "Validity", "Warning", "STANDARDISE",
      "order_status already in canonical form", "DI-07",
      lambda d, c: d["order_status"].notna() & (d["order_status_raw"] != d["order_status"]),
      "t.order_status IS NOT NULL AND t.order_status_raw <> t.order_status"),
    R("order_payment_mode_standard", "orders", "Validity", "Warning", "STANDARDISE",
      "payment_mode already in canonical form (no variants or spaces)", "DI-09",
      lambda d, c: d["payment_mode"].notna() & (d["payment_mode_raw"] != d["payment_mode"]),
      "t.payment_mode IS NOT NULL AND t.payment_mode_raw <> t.payment_mode"),
    R("quantity_positive_integer", "orders", "Accuracy", "Critical", "QUARANTINE",
      "quantity is a whole number above 0", "DI-10",
      lambda d, c: d["quantity"].isna() | (d["quantity"] <= 0) | (d["quantity"] % 1 != 0),
      "t.quantity IS NULL OR t.quantity <= 0 OR t.quantity <> floor(t.quantity)"),
    R("unit_price_positive", "orders", "Accuracy", "Critical", "QUARANTINE",
      "unit_price above 0", "-",
      lambda d, c: d["unit_price"].isna() | (d["unit_price"] <= 0),
      "t.unit_price IS NULL OR t.unit_price <= 0"),
    R("order_amount_matches_formula", "orders", "Accuracy", "Critical", "QUARANTINE",
      "order_amount = quantity x unit_price x (1 - discount_pct/100), within 0.01", "DI-11",
      lambda d, c: (d["order_amount"].isna() | d["quantity"].isna() | d["unit_price"].isna()
                    | ((d["order_amount"] - d["quantity"] * d["unit_price"]
                        * (1 - d["discount_pct"].fillna(0) / 100)).abs() > AMOUNT_TOLERANCE)),
      f"t.order_amount IS NULL OR t.quantity IS NULL OR t.unit_price IS NULL "
      f"OR abs(t.order_amount - t.quantity * t.unit_price * (1 - coalesce(t.discount_pct, 0) / 100)) "
      f"> {AMOUNT_TOLERANCE}"),
    R("delivery_minutes_plausible", "orders", "Accuracy", "Critical", "QUARANTINE",
      f"delivery_minutes above 0 and below {DELIVERY_MAX_MINUTES}", "DI-16",
      lambda d, c: d["delivery_minutes"].notna() & ((d["delivery_minutes"] <= 0)
                                                    | (d["delivery_minutes"] >= DELIVERY_MAX_MINUTES)),
      f"t.delivery_minutes IS NOT NULL AND (t.delivery_minutes <= 0 "
      f"OR t.delivery_minutes >= {DELIVERY_MAX_MINUTES})"),
    R("delivery_minutes_normal_range", "orders", "Accuracy", "Warning", "LOG",
      f"delivery_minutes inside the normal {DELIVERY_NORMAL[0]}-{DELIVERY_NORMAL[1]} range", "-",
      lambda d, c: ((d["delivery_minutes"] > 0) & (d["delivery_minutes"] < DELIVERY_MAX_MINUTES)
                    & ((d["delivery_minutes"] < DELIVERY_NORMAL[0])
                       | (d["delivery_minutes"] > DELIVERY_NORMAL[1]))),
      f"t.delivery_minutes > 0 AND t.delivery_minutes < {DELIVERY_MAX_MINUTES} "
      f"AND (t.delivery_minutes < {DELIVERY_NORMAL[0]} OR t.delivery_minutes > {DELIVERY_NORMAL[1]})"),
    R("order_customer_exists", "orders", "Consistency", "Critical", "QUARANTINE",
      "customer_id exists in customers", "-",
      lambda d, c: d["customer_id"].notna() & ~d["customer_id"].isin(c["customer_ids"]),
      "t.customer_id IS NOT NULL AND NOT EXISTS "
      "(SELECT 1 FROM $ref_customers r WHERE r.customer_id = t.customer_id)"),
    R("order_product_exists", "orders", "Consistency", "Critical", "QUARANTINE",
      "product_id exists in products", "DI-13",
      lambda d, c: d["product_id"].notna() & ~d["product_id"].isin(c["product_ids"]),
      "t.product_id IS NOT NULL AND NOT EXISTS "
      "(SELECT 1 FROM $ref_products r WHERE r.product_id = t.product_id)"),
    R("order_store_exists", "orders", "Consistency", "Critical", "QUARANTINE",
      "store_id exists in stores", "DI-14",
      lambda d, c: d["store_id"].notna() & ~d["store_id"].isin(c["store_ids"]),
      "t.store_id IS NOT NULL AND NOT EXISTS "
      "(SELECT 1 FROM $ref_stores r WHERE r.store_id = t.store_id)"),
    R("unit_price_not_above_mrp", "orders", "Consistency", "Warning", "LOG",
      "unit_price not above the product's current MRP (invalid MRPs ignored)", "DI-17 (restored)",
      lambda d, c: (d["unit_price"]
                    > d["product_id"].map(c["product_mrp"]).astype("Float64") + AMOUNT_TOLERANCE).fillna(False),
      f"EXISTS (SELECT 1 FROM $ref_product_mrp r WHERE r.product_id = t.product_id "
      f"AND t.unit_price > r.mrp + {AMOUNT_TOLERANCE})"),
    R("delivered_has_success_payment", "orders", "Consistency", "Warning", "RECONCILE",
      "DELIVERED orders have a SUCCESS payment", "DI-18",
      lambda d, c: (_is(d["order_status"], "DELIVERED") & d["order_id"].notna()
                    & ~d["order_id"].isin(c["success_order_ids"])),
      "t.order_status = 'DELIVERED' AND t.order_id IS NOT NULL AND NOT EXISTS "
      "(SELECT 1 FROM $ref_payments r WHERE r.order_id = t.order_id AND r.payment_status = 'SUCCESS')"),
    R("order_ts_not_future", "orders", "Timeliness", "Critical", "QUARANTINE",
      "order_ts is not after the run date", "DI-05",
      _ts_future("order_ts"),
      "t.order_ts >= TIMESTAMP '$next_day'"),
    R("orders_fresh", "orders", "Timeliness", "Warning", "ALERT",
      "the latest order is no more than 1 day older than the run date", "-",
      # future-dated rows (DI-05) are ignored, otherwise one bad 2027 date makes stale data look fresh
      lambda d, c: bool(pd.isna(d.loc[d["order_ts"] < c["next_day"], "order_ts"].max())
                        or d.loc[d["order_ts"] < c["next_day"], "order_ts"].max() < c["stale_before"]),
      "coalesce((SELECT max(order_ts) FROM $table WHERE order_ts < TIMESTAMP '$next_day') "
      "< TIMESTAMP '$stale_before', TRUE)",
      level="dataset"),

    # ----------------------------------------------------------- payments ---
    R("payment_id_not_null", "payments", "Completeness", "Critical", "QUARANTINE",
      "payment_id is present", "-",
      lambda d, c: d["payment_id"].isna(),
      "t.payment_id IS NULL"),
    R("payment_order_id_not_null", "payments", "Completeness", "Critical", "QUARANTINE",
      "order_id is present", "-",
      lambda d, c: d["order_id"].isna(),
      "t.order_id IS NULL"),
    R("gateway_ref_present", "payments", "Completeness", "Warning", "LOG",
      "SUCCESS UPI/Card/Wallet payments have a gateway_ref (COD never does)", "DI-26 (revised)",
      lambda d, c: (_is(d["payment_status"], "SUCCESS") & d["payment_mode"].isin(GATEWAY_MODES).fillna(False)
                    & d["gateway_ref"].isna()),
      "t.payment_status = 'SUCCESS' AND t.payment_mode IN ("
      + ", ".join(f"'{m}'" for m in GATEWAY_MODES) + ") AND t.gateway_ref IS NULL"),
    R("payment_id_unique", "payments", "Uniqueness", "Critical", "DEDUPE",
      "only the first copy of a payment_id loads", "DI-19",
      lambda d, c: _dup_first(d, "payment_id"),
      "t.payment_id IS NOT NULL AND row_number() OVER (PARTITION BY t.payment_id ORDER BY t._row) > 1"),
    R("payment_ts_valid", "payments", "Validity", "Critical", "QUARANTINE",
      "payment_ts is a real ISO-8601 timestamp", "-",
      _ts_invalid("payment_ts"),
      "t.payment_ts_raw IS NOT NULL AND t.payment_ts IS NULL"),
    R("payment_status_allowed", "payments", "Validity", "Critical", "QUARANTINE",
      "payment_status is SUCCESS, FAILED or REFUNDED", "-",
      lambda d, c: d["payment_status"].isna(),
      "t.payment_status IS NULL"),
    R("payment_mode_known", "payments", "Validity", "Warning", "REPAIR",
      "payment_mode maps to an allowed value (else loads as UNKNOWN)", "DI-24",
      lambda d, c: d["payment_mode"].isna(),
      "t.payment_mode IS NULL"),
    R("payment_mode_standard", "payments", "Validity", "Warning", "STANDARDISE",
      "payment_mode already in canonical form", "DI-25",
      lambda d, c: d["payment_mode"].notna() & (d["payment_mode_raw"] != d["payment_mode"]),
      "t.payment_mode IS NOT NULL AND t.payment_mode_raw <> t.payment_mode"),
    R("success_amount_positive", "payments", "Accuracy", "Critical", "QUARANTINE",
      "a SUCCESS payment is above 0", "DI-22",
      lambda d, c: _is(d["payment_status"], "SUCCESS") & (d["amount_paid"].isna() | (d["amount_paid"] <= 0)),
      "t.payment_status = 'SUCCESS' AND (t.amount_paid IS NULL OR t.amount_paid <= 0)"),
    R("payment_order_exists", "payments", "Consistency", "Critical", "QUARANTINE",
      "order_id exists in orders (orphans look like test transactions)", "DI-20",
      lambda d, c: d["order_id"].notna() & ~d["order_id"].isin(c["order_ids"]),
      "t.order_id IS NOT NULL AND NOT EXISTS "
      "(SELECT 1 FROM $ref_orders r WHERE r.order_id = t.order_id)"),
    R("success_amount_matches_order", "payments", "Consistency", "Warning", "RECONCILE",
      "a SUCCESS payment equals its order's order_amount", "DI-21",
      lambda d, c: (_is(d["payment_status"], "SUCCESS") & d["amount_paid"].notna()
                    & ((d["amount_paid"] - d["order_id"].map(c["order_amounts"]).astype("Float64")).abs()
                       > AMOUNT_TOLERANCE)),
      f"t.payment_status = 'SUCCESS' AND t.amount_paid IS NOT NULL AND EXISTS "
      f"(SELECT 1 FROM $ref_orders r WHERE r.order_id = t.order_id "
      f"AND abs(t.amount_paid - r.order_amount) > {AMOUNT_TOLERANCE})"),
    R("payment_ts_not_future", "payments", "Timeliness", "Critical", "QUARANTINE",
      "payment_ts is not after the run date", "DI-23",
      _ts_future("payment_ts"),
      "t.payment_ts >= TIMESTAMP '$next_day'"),

    # ---------------------------------------------------------- customers ---
    R("customer_id_not_null", "customers", "Completeness", "Critical", "QUARANTINE",
      "customer_id is present", "-",
      lambda d, c: d["customer_id"].isna(),
      "t.customer_id IS NULL"),
    R("email_present", "customers", "Completeness", "Warning", "LOG",
      "email is present", "DI-30",
      lambda d, c: d["email"].isna(),
      "t.email IS NULL"),
    R("city_present", "customers", "Completeness", "Warning", "LOG",
      "city is present", "DI-33",
      lambda d, c: d["city"].isna(),
      "t.city IS NULL"),
    R("customer_id_unique", "customers", "Uniqueness", "Critical", "DEDUPE",
      "one version per customer_id: the LAST row in the file is kept (pending client)", "DI-27",
      lambda d, c: _dup_first(d, "customer_id", ascending=False),
      "t.customer_id IS NOT NULL AND row_number() OVER (PARTITION BY t.customer_id ORDER BY t._row DESC) > 1"),
    R("email_format", "customers", "Validity", "Warning", "LOG",
      "email looks like name@domain.tld", "DI-29",
      lambda d, c: d["email"].notna() & ~d["email"].str.fullmatch(EMAIL_RE),
      f"t.email IS NOT NULL AND NOT regexp_full_match(t.email, '{EMAIL_RE}')"),
    R("phone_format", "customers", "Validity", "Warning", "LOG",
      "phone is a 10-digit Indian mobile after removing +91 / 0", "DI-31",
      lambda d, c: d["phone"].notna() & ~d["phone_norm"].str.fullmatch(PHONE_RE).fillna(False),
      f"t.phone IS NOT NULL AND NOT coalesce(regexp_full_match(t.phone_norm, '{PHONE_RE}'), FALSE)"),
    R("customer_pincode_format", "customers", "Validity", "Warning", "LOG",
      "pincode is 6 digits", "DI-34",
      lambda d, c: d["pincode"].notna() & ~d["pincode"].str.fullmatch(PINCODE_RE),
      f"t.pincode IS NOT NULL AND NOT regexp_full_match(t.pincode, '{PINCODE_RE}')"),
    R("loyalty_tier_allowed", "customers", "Validity", "Warning", "REPAIR",
      "loyalty_tier is Bronze, Silver, Gold or Platinum (else UNKNOWN)", "-",
      lambda d, c: d["loyalty_tier"].isna(),
      "t.loyalty_tier IS NULL"),
    R("signup_date_valid", "customers", "Validity", "Warning", "REPAIR",
      "signup_date is a real date (else NULL)", "-",
      _ts_invalid("signup_date"),
      "t.signup_date_raw IS NOT NULL AND t.signup_date IS NULL"),
    R("loyalty_tier_standard", "customers", "Consistency", "Warning", "STANDARDISE",
      "loyalty_tier uses one casing", "DI-32",
      lambda d, c: d["loyalty_tier"].notna() & (d["loyalty_tier_raw"] != d["loyalty_tier"]),
      "t.loyalty_tier IS NOT NULL AND t.loyalty_tier_raw <> t.loyalty_tier"),
    R("customer_pincode_bengaluru", "customers", "Consistency", "Warning", "LOG",
      "pincode is in Bengaluru (56xxxx)", "DI-35",
      lambda d, c: d["pincode"].str.fullmatch(PINCODE_RE).fillna(False) & ~d["pincode"].str.startswith("56"),
      f"regexp_full_match(t.pincode, '{PINCODE_RE}') AND t.pincode NOT LIKE '56%'"),
    R("signup_date_not_future", "customers", "Timeliness", "Warning", "REPAIR",
      "signup_date is not after the run date (else NULL)", "DI-28",
      _ts_future("signup_date"),
      "t.signup_date >= TIMESTAMP '$next_day'"),

    # ----------------------------------------------------------- products ---
    R("product_id_not_null", "products", "Completeness", "Critical", "QUARANTINE",
      "product_id is present", "-",
      lambda d, c: d["product_id"].isna(),
      "t.product_id IS NULL"),
    R("category_present", "products", "Completeness", "Warning", "REPAIR",
      "category is present (else UNKNOWN)", "DI-37",
      lambda d, c: d["category"].isna(),
      "t.category IS NULL"),
    R("product_id_unique", "products", "Uniqueness", "Critical", "DEDUPE",
      "one row per product_id: rows named '(old)' lose, then first by _row", "DI-36",
      _dup_product,
      "t.product_id IS NOT NULL AND row_number() OVER (PARTITION BY t.product_id ORDER BY "
      "CASE WHEN t.product_name LIKE '%(old)' THEN 1 ELSE 0 END, t._row) > 1"),
    R("mrp_positive", "products", "Accuracy", "Warning", "REPAIR",
      "mrp above 0 (else NULL)", "DI-39",
      lambda d, c: d["mrp"].isna() | (d["mrp"] <= 0),
      "t.mrp IS NULL OR t.mrp <= 0"),
    R("cost_price_positive", "products", "Accuracy", "Warning", "REPAIR",
      "cost_price above 0 (else NULL, excluded from margin)", "-",
      lambda d, c: d["cost_price"].isna() | (d["cost_price"] <= 0),
      "t.cost_price IS NULL OR t.cost_price <= 0"),
    R("cost_price_not_above_mrp", "products", "Accuracy", "Warning", "REPAIR",
      "cost_price <= mrp (else cost NULL, excluded from margin)", "DI-38",
      lambda d, c: d["cost_price"].notna() & (d["mrp"] > 0) & (d["cost_price"] > d["mrp"]),
      "t.cost_price IS NOT NULL AND t.mrp > 0 AND t.cost_price > t.mrp"),

    # ------------------------------------------------------------- stores ---
    R("store_id_not_null", "stores", "Completeness", "Critical", "QUARANTINE",
      "store_id is present", "-",
      lambda d, c: d["store_id"].isna(),
      "t.store_id IS NULL"),
    R("store_id_unique", "stores", "Uniqueness", "Critical", "DEDUPE",
      "one row per store_id", "-",
      lambda d, c: _dup_first(d, "store_id"),
      "t.store_id IS NOT NULL AND row_number() OVER (PARTITION BY t.store_id ORDER BY t._row) > 1"),
    R("zone_allowed", "stores", "Validity", "Warning", "REPAIR",
      "zone is North, South, East or West (else UNKNOWN)", "DI-40",
      lambda d, c: d["zone"].isna(),
      "t.zone IS NULL"),
    R("store_pincode_format", "stores", "Validity", "Warning", "LOG",
      "pincode is 6 digits", "-",
      lambda d, c: d["pincode"].notna() & ~d["pincode"].str.fullmatch(PINCODE_RE),
      f"t.pincode IS NOT NULL AND NOT regexp_full_match(t.pincode, '{PINCODE_RE}')"),
    R("capacity_positive", "stores", "Accuracy", "Warning", "REPAIR",
      "capacity_orders_per_day above 0 (else NULL)", "DI-41",
      lambda d, c: d["capacity_orders_per_day"].isna() | (d["capacity_orders_per_day"] <= 0),
      "t.capacity_orders_per_day IS NULL OR t.capacity_orders_per_day <= 0"),
]

_names = [r.name for r in RULES]
assert len(_names) == len(set(_names)), "rule names must be unique"


def rules_for(file: str, level: str | None = None) -> list[Rule]:
    f = RULE_FILE.get(file, file)
    return [r for r in RULES if r.file == f and (level is None or r.level == level)]


# ---------------------------------------------------------------------------
# 4. pandas engine (ETL)
# ---------------------------------------------------------------------------
def _as_bool(result, index) -> pd.Series:
    s = pd.Series(result, index=index)
    return s.astype("boolean").fillna(False).astype(bool)


def evaluate(file: str, std: pd.DataFrame, ctx: dict) -> tuple[dict[str, pd.Series], dict[str, bool]]:
    """Run every rule for `file`. Returns (row masks, dataset results)."""
    rows = {r.name: _as_bool(r.py(std, ctx), std.index) for r in rules_for(file, "row")}
    dataset = {r.name: bool(r.py(std, ctx)) for r in rules_for(file, "dataset")}
    return rows, dataset


def _reasons(masks: dict[str, pd.Series], names: list[str], index) -> pd.Series:
    out = pd.Series("", index=index, dtype=object)
    for n in names:
        m = masks[n]
        out = out.where(~m, np.where(out == "", n, out + "; " + n))
    return out.replace("", None)


def apply_flags(file: str, std: pd.DataFrame, ctx: dict):
    """Add dq_status ('PASS' / 'WARN' / 'FAIL'), failure_reason and warning_reason.

    Returns (flagged_frame, row_masks, dataset_results).
    ETL: passed = flagged[flagged.dq_status != 'FAIL'];
         quarantine = flagged[flagged.dq_status == 'FAIL'] (failure_reason says why).
    """
    masks, dataset = evaluate(file, std, ctx)
    crit = [r.name for r in rules_for(file, "row") if r.severity == "Critical"]
    warn = [r.name for r in rules_for(file, "row") if r.severity == "Warning"]
    any_c = pd.DataFrame({n: masks[n] for n in crit}, index=std.index).any(axis=1) if crit else False
    any_w = pd.DataFrame({n: masks[n] for n in warn}, index=std.index).any(axis=1) if warn else False
    out = std.copy()
    out["dq_status"] = np.where(any_c, "FAIL", np.where(any_w, "WARN", "PASS"))
    out["failure_reason"] = _reasons(masks, crit, std.index)
    out["warning_reason"] = _reasons(masks, warn, std.index)
    return out, masks, dataset


def dq_results(file: str, std: pd.DataFrame, masks: dict, dataset: dict,
               run_id: str, dag_id: str, checked_at: datetime | None = None) -> pd.DataFrame:
    """One row per check, in the dq_results table layout from the brief."""
    checked_at = (checked_at or datetime.now(timezone.utc)).replace(tzinfo=None, microsecond=0)
    rows = []
    for r in rules_for(file):
        if r.level == "row":
            checked, failed = len(std), int(masks[r.name].sum())
        else:
            checked, failed = 1, int(dataset[r.name])
        status = "PASS" if failed == 0 else ("FAIL" if r.severity == "Critical" else "WARN")
        rows.append(dict(run_id=run_id, dag_id=dag_id, check_name=f"{file}.{r.name}",
                         dimension=r.dimension, severity=r.severity, rows_checked=checked,
                         rows_failed=failed, status=status, checked_at=checked_at))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 5. SQL engine (ELT)
# ---------------------------------------------------------------------------
def _params(ctx_or_as_of, table: str, ref_prefix: str = "ref_") -> dict:
    """Values substituted into the rules' SQL. ctx_or_as_of=None leaves {{ placeholders }}
    in place, for the .sql files the ELT DAG renders at run time."""
    if ctx_or_as_of is None:
        next_day, stale_before = "{{ next_day }}", "{{ stale_before }}"
    else:
        as_of = pd.Timestamp(ctx_or_as_of["as_of"] if isinstance(ctx_or_as_of, dict) else ctx_or_as_of)
        next_day = (as_of + pd.Timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        stale_before = (as_of - pd.Timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "next_day": next_day,
        "stale_before": stale_before,
        "table": table,
        **{f"ref_{k}": f"{ref_prefix}{k}" for k in ("orders", "payments", "customers", "products", "stores",
                                                     "product_mrp")},
    }


def sql_flagged_select(file: str, table: str, as_of, ref_prefix: str = "ref_") -> str:
    """SELECT that returns `table` plus one f_<rule> column per rule and
    dq_status / failure_reason / warning_reason. Use it to build stg_<file>."""
    p = _params(as_of, table, ref_prefix)
    rules = rules_for(file, "row")
    flags = ",\n    ".join(
        f'COALESCE(({Template(r.sql).substitute(p)}), FALSE) AS "f_{r.name}"' for r in rules)
    crit = [r.name for r in rules if r.severity == "Critical"]
    warn = [r.name for r in rules if r.severity == "Warning"]

    def any_of(names):
        return " OR ".join(f'"f_{n}"' for n in names) or "FALSE"

    def reason(names):
        if not names:
            return "CAST(NULL AS VARCHAR)"
        parts = ", ".join(f"CASE WHEN \"f_{n}\" THEN '{n}' END" for n in names)
        return f"NULLIF(concat_ws('; ', {parts}), '')"

    return f"""SELECT *,
  CASE WHEN {any_of(crit)} THEN 'FAIL' WHEN {any_of(warn)} THEN 'WARN' ELSE 'PASS' END AS dq_status,
  {reason(crit)} AS failure_reason,
  {reason(warn)} AS warning_reason
FROM (
  SELECT t.*,
    {flags}
  FROM {table} t
) AS flagged"""


def sql_dq_results(file: str, flagged_table: str, source_table: str, as_of,
                   run_id: str, dag_id: str, ref_prefix: str = "ref_",
                   checked_at: datetime | None = None) -> str:
    """INSERT-ready SELECT producing dq_results rows from a flagged staging table.
    checked_at is written as a plain UTC timestamp, same as the pandas version."""
    p = _params(as_of, source_table, ref_prefix)
    if as_of is None:  # template mode: values filled in by the ELT DAG at run time
        ts = "{{ checked_at }}"
    else:
        ts = (checked_at or datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M:%S")
    parts = []
    for r in rules_for(file):
        common = (f"'{run_id}' AS run_id, '{dag_id}' AS dag_id, '{file}.{r.name}' AS check_name, "
                  f"'{r.dimension}' AS dimension, '{r.severity}' AS severity")
        bad = "FAIL" if r.severity == "Critical" else "WARN"
        if r.level == "row":
            parts.append(
                f"SELECT {common}, count(*) AS rows_checked, "
                f"count(*) FILTER (WHERE \"f_{r.name}\") AS rows_failed, "
                f"CASE WHEN count(*) FILTER (WHERE \"f_{r.name}\") = 0 THEN 'PASS' ELSE '{bad}' END AS status, "
                f"TIMESTAMP '{ts}' AS checked_at FROM {flagged_table}")
        else:
            expr = Template(r.sql).substitute(p)
            parts.append(
                f"SELECT {common}, 1 AS rows_checked, CASE WHEN ({expr}) THEN 1 ELSE 0 END AS rows_failed, "
                f"CASE WHEN ({expr}) THEN '{bad}' ELSE 'PASS' END AS status, TIMESTAMP '{ts}' AS checked_at")
    return "\nUNION ALL\n".join(parts)


# ---------------------------------------------------------------------------
# 5b. Write the rules out as .sql files for the ELT pipeline
# ---------------------------------------------------------------------------
GENERATED_HEADER = """-- GENERATED by include/dq_rules.py -- do not edit by hand.
-- Change the rule in dq_rules.py, then run:  python include/dq_rules.py --write-sql include/sql/staging/generated
-- Placeholders like {{ run_id }} are filled in by the ELT DAG at run time.
"""
STAGED_FILES = ["orders", "payments", "customers", "products", "stores", "orders_increment"]


def generated_sql_files() -> dict[str, str]:
    """file name -> SQL text. Same rules as the pandas engine, with {{ placeholders }}."""
    std = {f: f"stg_{f}_std" for f in STAGED_FILES}
    views = sql_reference_views({k: std[k] for k in ["orders", "payments", "customers", "products", "stores"]})
    out = {"02_ref_views.sql": GENERATED_HEADER
           + "-- Lookup lists for the cross-file rules (built from this run's base files)\n"
           + ";\n\n".join(v.strip() for v in views) + ";\n"}
    for f in STAGED_FILES:
        out[f"03_flag_{f}.sql"] = (
            GENERATED_HEADER
            + f"-- Adds one f_<check> column per rule, plus dq_status, failure_reason, warning_reason\n"
            + f"CREATE OR REPLACE TABLE stg_{f}_flagged AS\n"
            + sql_flagged_select(f, std[f], None) + ";\n")
        out[f"04_dq_results_{f}.sql"] = (
            GENERATED_HEADER
            + f"-- One dq_results row per check for {f}, for this run (safe to re-run)\n"
            + "DELETE FROM dq_results WHERE run_id = '{{ run_id }}' AND dag_id = '{{ dag_id }}' "
            + f"AND check_name LIKE '{f}.%';\n\n"
            + "INSERT INTO dq_results\n"
            + sql_dq_results(f, f"stg_{f}_flagged", std[f], None, "{{ run_id }}", "{{ dag_id }}") + ";\n")
    return out


def write_sql(out_dir: Path | str, check: bool = False) -> int:
    """Write the generated files, or with check=True report whether they are out of date."""
    out_dir = Path(out_dir)
    stale = []
    for name, text in generated_sql_files().items():
        path = out_dir / name
        if check:
            if not path.exists() or path.read_text() != text:
                stale.append(name)
        else:
            out_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
    if check:
        print("Generated SQL is up to date" if not stale else
              f"OUT OF DATE: {stale}. Run: python include/dq_rules.py --write-sql {out_dir}")
        return 1 if stale else 0
    print(f"Wrote {len(generated_sql_files())} SQL files to {out_dir}")
    return 0


# ---------------------------------------------------------------------------
# 6. Catalogue + parity test (run this file directly)
# ---------------------------------------------------------------------------
def catalogue_markdown() -> str:
    lines = ["| Check | File | Dimension | Severity | Action | Rule | Issue |",
             "|---|---|---|---|---|---|---|"]
    order = {"Completeness": 0, "Uniqueness": 1, "Validity": 2, "Accuracy": 3, "Consistency": 4, "Timeliness": 5}
    for r in sorted(RULES, key=lambda r: (order[r.dimension], r.file)):
        lines.append(f"| `{r.name}` | {r.file} | {r.dimension} | {r.severity} | {r.action} | "
                     f"{r.description} | {r.issues} |")
    cov = pd.crosstab(pd.Series([r.file for r in RULES], name="file"),
                      pd.Series([r.dimension for r in RULES], name="dimension"))
    return "\n".join(lines) + "\n\nChecks per file and dimension:\n\n" + cov.to_string()


def parity_test(data_dir: Path, as_of: str, include_increment: bool) -> int:
    import duckdb

    names = ["orders", "payments", "customers", "products", "stores"]
    if include_increment and (data_dir / FILES["orders_increment"]).exists():
        names.append("orders_increment")
    std = {n: standardise(n, read_raw(data_dir / FILES[n])) for n in names}
    ctx = build_context(std, as_of)

    con = duckdb.connect()
    for n, df in std.items():
        con.register(f"df_{n}", df)
        con.execute(f"CREATE TABLE std_{n} AS SELECT * FROM df_{n}")
    for stmt in sql_reference_views({k: f"std_{k}" for k in ["orders", "payments", "customers", "products", "stores"]}):
        con.execute(stmt)

    report, mismatches = [], 0
    for n in names:
        flagged, masks, dataset = apply_flags(n, std[n], ctx)
        con.execute(f"CREATE TABLE flagged_{n} AS {sql_flagged_select(n, f'std_{n}', as_of)}")
        sql_df = con.execute(f"SELECT * FROM flagged_{n} ORDER BY _row").df()
        for r in rules_for(n, "row"):
            py_rows = set(std[n].loc[masks[r.name], "_row"])
            sql_rows = set(sql_df.loc[sql_df[f"f_{r.name}"].astype(bool), "_row"])
            ok = py_rows == sql_rows
            mismatches += not ok
            report.append((n, r.name, r.dimension, r.severity, len(py_rows), len(sql_rows), "yes" if ok else "NO"))
        dq_sql = con.execute(sql_dq_results(n, f"flagged_{n}", f"std_{n}", as_of, "parity", "parity")).df()
        for r in rules_for(n, "dataset"):
            sql_failed = int(dq_sql.loc[dq_sql.check_name == f"{n}.{r.name}", "rows_failed"].iloc[0])
            ok = int(dataset[r.name]) == sql_failed
            mismatches += not ok
            report.append((n, r.name, r.dimension, r.severity, int(dataset[r.name]), sql_failed,
                           "yes" if ok else "NO"))
        # row-level outcome must match too
        py_status = flagged.set_index("_row")["dq_status"]
        sql_status = sql_df.set_index("_row")["dq_status"]
        if not py_status.sort_index().equals(sql_status.sort_index()):
            mismatches += 1
            print(f"!! dq_status differs between pandas and SQL for {n}")

    pd.set_option("display.width", 200, "display.max_rows", 200)
    out = pd.DataFrame(report, columns=["file", "check", "dimension", "severity",
                                        "pandas_failed", "sql_failed", "match"])
    print("\n=== Rule parity: pandas (ETL) vs DuckDB SQL (ELT) ===")
    print(out.to_string(index=False))

    print("\n=== Row outcome per file (Critical -> FAIL/quarantine, Warning -> WARN) ===")
    summary = []
    for n in names:
        f, _, _ = apply_flags(n, std[n], ctx)
        vc = f["dq_status"].value_counts()
        summary.append(dict(file=FILES[n], rows=len(f), PASS=int(vc.get("PASS", 0)),
                            WARN=int(vc.get("WARN", 0)), FAIL=int(vc.get("FAIL", 0))))
    print(pd.DataFrame(summary).to_string(index=False))

    print(f"\n{'ALL RULES MATCH' if mismatches == 0 else f'{mismatches} MISMATCH(ES)'} "
          f"- {len(RULES)} rules, as-of {as_of}")
    return 0 if mismatches == 0 else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="NammaMart shared DQ rules: parity test and catalogue")
    ap.add_argument("--data-dir", default="include/data/nammamart")
    ap.add_argument("--as-of", default="2026-10-01", help="run date for timeliness checks")
    ap.add_argument("--include-increment", action="store_true")
    ap.add_argument("--markdown", action="store_true", help="print the rule catalogue and exit")
    ap.add_argument("--write-sql", metavar="DIR", help="write the rules as .sql files for the ELT DAG")
    ap.add_argument("--check-sql", metavar="DIR", help="exit 1 if the generated .sql files are out of date")
    args = ap.parse_args()
    if args.write_sql:
        return write_sql(args.write_sql)
    if args.check_sql:
        return write_sql(args.check_sql, check=True)
    if args.markdown:
        print(catalogue_markdown())
        return 0
    return parity_test(Path(args.data_dir), args.as_of, args.include_increment)


if __name__ == "__main__":
    sys.exit(main())
