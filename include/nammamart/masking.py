"""
include/nammamart/masking.py
----------------------------
Personal-data masking helpers (assignment Part A.4 and Part E.4).

**Masking** means replacing part of a value with ``*`` so the record stays usable
for analytics (you can still count customers, group them, join them) but the
actual personal detail cannot be read by a report user.

Examples produced by this module::

    priya.hegde199@example.com  ->  pr***@example.com
    9774349021                  ->  97******21

Java comparison (learning aid): this is the same idea as a Jackson
``@JsonSerialize`` masking serializer that hides a field before it leaves the API.
"""

from __future__ import annotations

import re

import pandas as pd

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
PHONE_DIGITS = 10

#: Columns that carry personal data and must never reach a reporting table raw.
PII_COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": ("full_name", "email", "phone"),
    "stores": ("manager_name",),
}


def mask_email(value: object) -> str | None:
    """Keep the first two characters of the local part: ``pr***@example.com``.

    Values that are empty or not shaped like an email are returned as
    ``"INVALID_EMAIL"`` so a report never shows a half-masked real address.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    if "@" not in text:
        return "INVALID_EMAIL"

    local, _, domain = text.partition("@")
    if not local or not domain:
        return "INVALID_EMAIL"
    visible = local[:2]
    return f"{visible}***@{domain}"


def mask_phone(value: object) -> str | None:
    """Keep the first two and last two digits: ``9774349021`` -> ``97******21``."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    if len(digits) < 4:
        return "*" * len(digits)
    hidden = "*" * (len(digits) - 4)
    return f"{digits[:2]}{hidden}{digits[-2:]}"


def mask_name(value: object) -> str | None:
    """Keep the first name and the initial of each remaining part: ``Priya H.``."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    parts = text.split()
    if len(parts) == 1:
        return parts[0]
    initials = " ".join(f"{p[0].upper()}." for p in parts[1:] if p)
    return f"{parts[0]} {initials}".strip()


def is_valid_email(value: object) -> bool:
    """True when the value looks like ``name@domain.tld``."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return bool(EMAIL_PATTERN.match(str(value).strip()))


def is_valid_phone(value: object, expected_digits: int = PHONE_DIGITS) -> bool:
    """True when the value contains exactly ``expected_digits`` digits."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return len(re.sub(r"\D", "", str(value))) == expected_digits


def mask_customer_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of the customers frame with name, email and phone masked.

    The unmasked columns are dropped, not merely overwritten, so there is no way
    to recover the original value from a reporting table.
    """
    masked = df.copy()
    if "email" in masked.columns:
        masked["email_masked"] = masked["email"].map(mask_email)
    if "phone" in masked.columns:
        masked["phone_masked"] = masked["phone"].map(mask_phone)
    if "full_name" in masked.columns:
        masked["customer_name_masked"] = masked["full_name"].map(mask_name)
    return masked.drop(columns=[c for c in ("email", "phone", "full_name") if c in masked.columns])


#: How each known personal-data column is masked when a record has to keep its
#: original shape (for example a quarantine file, which must stay comparable to
#: the source row).
_COLUMN_MASKERS = {
    "email": mask_email,
    "phone": mask_phone,
    "full_name": mask_name,
    "manager_name": mask_name,
    "customer_name": mask_name,
}


def mask_pii_in_place(df: pd.DataFrame) -> pd.DataFrame:
    """Mask personal data while keeping the original column names and order.

    Used for quarantine output: a rejected row must stay recognisable to the
    team that has to fix it, but the repository must never contain an unmasked
    email address or phone number.  The untouched original always remains in the
    source file and in the access-controlled ELT raw layer.
    """
    masked = df.copy()
    for column, masker in _COLUMN_MASKERS.items():
        if column in masked.columns:
            masked[column] = masked[column].map(masker)
    return masked
