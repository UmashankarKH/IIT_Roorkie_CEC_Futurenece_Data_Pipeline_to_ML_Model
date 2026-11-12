"""
include/nammamart/transforms.py
-------------------------------
Pure pandas helpers used by the ETL pipeline (assignment Part A.4) and by the
data-quality rules (Part D).

Every function here is a small, side-effect free transformation: it takes a
DataFrame or Series and returns a new one.  Keeping them separate from the DAG
means they can be unit tested with ``pytest`` without starting Airflow.

Java comparison (learning aid): think of this module as a stateless utility
class (``OrderMapper``), and of the DAG as the service that calls it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import config

# ---------------------------------------------------------------------------
# Type coercion — never raises, invalid values become NaN / NaT
# ---------------------------------------------------------------------------


def to_number(series: pd.Series) -> pd.Series:
    """Convert a column to float. Values that cannot be parsed become ``NaN``."""
    return pd.to_numeric(series, errors="coerce")


def to_timestamp(series: pd.Series) -> pd.Series:
    """Convert a column to datetime. Values that cannot be parsed become ``NaT``."""
    return pd.to_datetime(series, errors="coerce", format="mixed")


def clean_text(series: pd.Series) -> pd.Series:
    """Trim surrounding whitespace and turn empty strings into ``NA``."""
    text = series.astype("string").str.strip()
    return text.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NULL": pd.NA})


def is_blank(series: pd.Series) -> pd.Series:
    """True where the value is null or only whitespace."""
    return clean_text(series).isna()


# ---------------------------------------------------------------------------
# Standardisation — "upi", "UPI " and "Upi" must all become "UPI"
# ---------------------------------------------------------------------------

_PAYMENT_MODE_LOOKUP = {
    "upi": "UPI",
    "u p i": "UPI",
    "card": "Card",
    "credit card": "Card",
    "debit card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cash": "Cash on Delivery",
    "wallet": "Wallet",
    "e-wallet": "Wallet",
}


def standardise_payment_mode(series: pd.Series) -> pd.Series:
    """Map free-text payment modes onto the four approved values."""
    key = clean_text(series).str.lower().str.replace(r"\s+", " ", regex=True)
    mapped = key.map(_PAYMENT_MODE_LOOKUP)
    # Anything unknown is kept as the trimmed original so the validity rule can flag it.
    return mapped.fillna(clean_text(series))


def standardise_upper(series: pd.Series) -> pd.Series:
    """Upper-case codes such as ``order_status`` and ``loyalty_tier``."""
    return clean_text(series).str.upper()


def standardise_title(series: pd.Series) -> pd.Series:
    """Title-case descriptive text such as ``zone`` (``north`` -> ``North``)."""
    return clean_text(series).str.title()


def standardise_yes_no(series: pd.Series) -> pd.Series:
    """Normalise Y/N/True/False flags to ``Y`` or ``N``."""
    key = clean_text(series).str.upper()
    return key.replace({"TRUE": "Y", "FALSE": "N", "YES": "Y", "NO": "N", "1": "Y", "0": "N"})


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def redundant_duplicate_mask(df: pd.DataFrame, key: str,
                             order_by: str | None = None) -> pd.Series:
    """Flag the *extra* copies of a duplicated key, keeping one survivor.

    Why not flag every copy?  If both copies of ``ORD000539`` were quarantined
    the order would disappear from revenue even though it is a real order.
    Instead we keep one row per key - the newest when a recency column is
    available - and quarantine the superseded copies with a reason.  The result
    still satisfies "one row per key" while protecting the revenue total.
    """
    if key not in df.columns:
        return pd.Series(False, index=df.index)

    working = pd.DataFrame({"_key": clean_text(df[key])}, index=df.index)
    if order_by and order_by in df.columns:
        working["_order"] = to_timestamp(df[order_by])
    else:
        working["_order"] = pd.Series(range(len(df)), index=df.index)

    duplicated_keys = working["_key"].duplicated(keep=False) & working["_key"].notna()
    if not duplicated_keys.any():
        return pd.Series(False, index=df.index)

    # Within each duplicated key, keep the last row after ordering by recency
    # (stable sort keeps file order when the recency value is equal or missing).
    candidates = working.loc[duplicated_keys].sort_values("_order", kind="stable",
                                                          na_position="first")
    survivors = candidates.drop_duplicates(subset=["_key"], keep="last").index
    mask = pd.Series(False, index=df.index)
    mask.loc[duplicated_keys] = True
    mask.loc[survivors] = False
    return mask


def duplicate_conflict_labels(df: pd.DataFrame, key: str) -> pd.Series:
    """``exact`` when duplicate copies are identical, ``conflicting`` otherwise."""
    labels = pd.Series("", index=df.index, dtype="object")
    if key not in df.columns:
        return labels
    business_columns = [c for c in df.columns if not c.startswith("_")]
    keys = clean_text(df[key])
    for key_value, group in df.loc[keys.duplicated(keep=False) & keys.notna()].groupby(keys):
        label = "exact" if len(group[business_columns].drop_duplicates()) == 1 else "conflicting"
        labels.loc[group.index] = label
    return labels


def deduplicate(df: pd.DataFrame, key: str, order_by: str | None = None) -> pd.DataFrame:
    """Keep one row per ``key``.

    When ``order_by`` is given the newest row wins (useful for customers and
    products where the latest export is the most correct one).  Otherwise the
    first occurrence is kept, which keeps the result deterministic.
    """
    if key not in df.columns:
        return df
    working = df.copy()
    if order_by and order_by in working.columns:
        working["_sort_key"] = to_timestamp(working[order_by])
        working = working.sort_values("_sort_key", na_position="first")
        working = working.drop_duplicates(subset=[key], keep="last").drop(columns=["_sort_key"])
    else:
        working = working.drop_duplicates(subset=[key], keep="first")
    return working.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Business derivations
# ---------------------------------------------------------------------------


def expected_order_amount(df: pd.DataFrame) -> pd.Series:
    """``quantity * unit_price * (1 - discount_pct/100)`` rounded to 2 decimals."""
    quantity = to_number(df["quantity"])
    unit_price = to_number(df["unit_price"])
    discount = to_number(df["discount_pct"]).fillna(0)
    return (quantity * unit_price * (1 - discount / 100)).round(2)


def add_order_derivations(df: pd.DataFrame) -> pd.DataFrame:
    """Add the columns the reporting layer needs.

    * ``order_date``      — calendar date of the order (for daily aggregation)
    * ``net_revenue``     — order amount for DELIVERED orders, otherwise 0
    * ``within_15_min``   — SLA flag used by the "10-minute promise" KPI
    """
    out = df.copy()
    order_ts = to_timestamp(out["order_ts"])
    out["order_ts"] = order_ts
    out["order_date"] = order_ts.dt.date
    amount = to_number(out["order_amount"]).round(2)
    out["order_amount"] = amount
    delivered = standardise_upper(out["order_status"]) == config.REVENUE_STATUS
    out["is_revenue"] = delivered
    out["net_revenue"] = np.where(delivered, amount.fillna(0.0), 0.0).round(2)
    minutes = to_number(out["delivery_minutes"])
    out["delivery_minutes"] = minutes
    out["within_15_min"] = np.where(
        delivered & minutes.notna(), minutes <= config.SLA_MINUTES, False
    )
    return out


def add_gross_margin(df: pd.DataFrame) -> pd.DataFrame:
    """``gross_margin = net_revenue - quantity * cost_price`` (0 when not revenue)."""
    out = df.copy()
    quantity = to_number(out["quantity"]).fillna(0)
    cost_price = to_number(out["cost_price"]).fillna(0)
    cost_of_goods = (quantity * cost_price).round(2)
    out["cost_of_goods"] = np.where(out["is_revenue"], cost_of_goods, 0.0)
    out["gross_margin"] = (out["net_revenue"] - out["cost_of_goods"]).round(2)
    return out
