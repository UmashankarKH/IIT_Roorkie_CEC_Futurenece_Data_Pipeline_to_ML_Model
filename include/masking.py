"""PII masking for NammaMart (include/masking.py).

Two flavours that produce the SAME output:
  * Python functions  -> used by the ETL DAG (pandas)
  * SQL expression builders -> used by the ELT staging SQL (DuckDB)

Rules
  email  priya.hegde199@example.com -> pr***@example.com
         no "@" / missing / "N/A"   -> ***
  phone  9774349021                 -> 97******21   (10 digits: first 2 + 6 stars + last 2)
         short numbers (>=4 digits) -> stars + last 2 digits
         fewer than 4 digits / NULL -> ***

Why mask instead of delete: analysts still need to tell customers apart and
spot duplicates (same masked value pattern + customer_id) without seeing the
real contact details.
"""
from __future__ import annotations

import re

import pandas as pd


# --------------------------------------------------------------------------- #
# Python (ETL)
# --------------------------------------------------------------------------- #
def mask_email(value) -> str:
    if value is None or (isinstance(value, float) and value != value):
        return "***"
    s = str(value).strip()
    if "@" not in s:
        return "***"
    parts = s.split("@")
    local, domain = parts[0], parts[1]
    return f"{local[:2]}***@{domain}"


def mask_phone(value) -> str:
    if value is None or (isinstance(value, float) and value != value):
        return "***"
    digits = re.sub(r"\D", "", str(value))
    n = len(digits)
    if n == 10:
        return digits[:2] + "******" + digits[-2:]
    if n >= 4:
        return "*" * (n - 2) + digits[-2:]
    return "***"


def mask_email_series(s: pd.Series) -> pd.Series:
    return s.map(mask_email)


def mask_phone_series(s: pd.Series) -> pd.Series:
    return s.map(mask_phone)


def mask_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of a customers frame with email and phone masked."""
    out = df.copy()
    out["email"] = mask_email_series(out["email"])
    out["phone"] = mask_phone_series(out["phone"])
    return out


# --------------------------------------------------------------------------- #
# SQL (ELT / DuckDB)  -- paste into staging SQL with an f-string, e.g.
#   f"SELECT customer_id, {sql_mask_email('email')} AS email, ..."
# --------------------------------------------------------------------------- #
def sql_mask_email(col: str) -> str:
    c = f"trim({col})"
    return (
        f"CASE WHEN {c} LIKE '%@%' "
        f"THEN substr(split_part({c}, '@', 1), 1, 2) || '***@' || split_part({c}, '@', 2) "
        f"ELSE '***' END"
    )


def sql_mask_phone(col: str) -> str:
    d = f"regexp_replace(coalesce({col}, ''), '[^0-9]', '', 'g')"
    return (
        f"CASE WHEN length({d}) = 10 "
        f"THEN substr({d}, 1, 2) || '******' || substr({d}, 9, 2) "
        f"WHEN length({d}) >= 4 "
        f"THEN repeat('*', length({d}) - 2) || substr({d}, length({d}) - 1, 2) "
        f"ELSE '***' END"
    )
