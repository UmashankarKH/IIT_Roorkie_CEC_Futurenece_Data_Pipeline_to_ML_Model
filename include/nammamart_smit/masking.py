"""
include/nammamart_smit/masking.py
---------------------------------
PII masking used in every reporting table. The same rules are written in SQL
in include/sql/stg_customers.sql so both pipelines mask identically.
"""


def mask_email(email):
    """jane.doe42@example.com -> ja***@example.com ; invalid -> None"""
    if email is None or "@" not in str(email):
        return None
    email = str(email).strip()
    at = email.index("@")
    return email[:2] + "***" + email[at:]


def mask_phone(phone):
    """9876543210 -> 98******10 (keeps length so bad lengths stay visible)"""
    if phone is None or str(phone).strip() == "":
        return None
    phone = str(phone).strip()
    if len(phone) <= 4:
        return "*" * len(phone)
    return phone[:2] + "*" * (len(phone) - 4) + phone[-2:]


def mask_name(full_name):
    """Jane Doe -> Jane D. (first name + last initial)"""
    if full_name is None or str(full_name).strip() == "":
        return None
    parts = str(full_name).split()
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} {parts[-1][0]}."
