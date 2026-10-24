"""Masks personal data. We mask (not delete) so analysts can still count and join."""
import re
import pandas as pd


def mask_email(e) -> str:
    e = "" if pd.isna(e) else str(e).strip()
    if "@" not in e:
        return "***"
    local, domain = e.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(p) -> str:
    digits = re.sub(r"\D", "", "" if pd.isna(p) else str(p))
    if len(digits) < 6:
        return "***"
    return f"{digits[:2]}{'*' * (len(digits) - 4)}{digits[-2:]}"