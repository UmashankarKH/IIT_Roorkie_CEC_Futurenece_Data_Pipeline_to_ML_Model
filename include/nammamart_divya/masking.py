"""
PII masking. The same rules are implemented in SQL by the macros in
include/sql/staging/00_macros.sql so ETL and ELT produce identical output.

    priya.hegde199@example.com -> pr***@example.com
    9774349021                 -> 97******21
"""


def mask_email(email):
    if email is None:
        return None
    email = str(email).strip()
    if "@" not in email:
        return "***"
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(phone):
    if phone is None:
        return None
    phone = str(phone).strip()
    if len(phone) <= 4:
        return "*" * len(phone)
    return phone[:2] + "*" * (len(phone) - 4) + phone[-2:]
