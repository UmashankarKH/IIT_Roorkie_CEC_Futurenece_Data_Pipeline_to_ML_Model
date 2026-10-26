"""Standardisation and PII masking for NammaMart pipelines."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from nammamart_config import VALID_ORDER_STATUS, VALID_PAYMENT_MODES

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def standardize_payment_mode(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    lowered = text.lower().replace(" ", "")
    mapping = {
        "upi": "UPI",
        "card": "Card",
        "wallet": "Wallet",
        "cashondelivery": "Cash on Delivery",
        "cod": "Cash on Delivery",
        "cashh": "Cash on Delivery",
    }
    if lowered in mapping:
        return mapping[lowered]
    if text in VALID_PAYMENT_MODES:
        return text
    return text


def standardize_order_status(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    lowered = text.lower()
    if lowered == "delivered":
        return "DELIVERED"
    if lowered == "cancelled":
        return "CANCELLED"
    if lowered == "returned":
        return "RETURNED"
    return text


def standardize_loyalty_tier(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    normalized = text.lower()
    mapping = {
        "bronze": "Bronze",
        "silver": "Silver",
        "gold": "Gold",
        "platinum": "Platinum",
    }
    return mapping.get(normalized, text)


def mask_email(email: object) -> str:
    text = clean_text(email)
    if not text or "@" not in text:
        return "***"
    local, _, domain = text.partition("@")
    if len(local) <= 2:
        masked_local = local[:1] + "***"
    else:
        masked_local = local[:2] + "***"
    return f"{masked_local}@{domain}"


def mask_phone(phone: object) -> str:
    digits = re.sub(r"\D", "", clean_text(phone))
    if len(digits) < 4:
        return "**********"
    return digits[:2] + "******" + digits[-2:]


def parse_timestamp(value: object) -> Optional[datetime]:
    text = clean_text(value)
    if not text or text.upper() == "N/A":
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", ""))
    except ValueError:
        return None


def parse_float(value: object) -> Optional[float]:
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def expected_order_amount(quantity: float, unit_price: float, discount_pct: float) -> float:
    return round(quantity * unit_price * (1 - discount_pct / 100), 2)


def is_valid_email(value: object) -> bool:
    return bool(EMAIL_RE.match(clean_text(value)))


def is_valid_phone(value: object) -> bool:
    return len(re.sub(r"\D", "", clean_text(value))) == 10


def is_valid_pincode(value: object) -> bool:
    return bool(re.fullmatch(r"\d{6}", clean_text(value)))
