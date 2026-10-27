"""
include/nammamart_masking.py
-----------------------------
PII Masking and data standardisation utility functions for NammaMart pipelines.
Complies with India's Digital Personal Data Protection (DPDP) Act expectations.
"""

import re


def mask_email(email: str) -> str:
    """
    Masks customer email addresses.
    Example: 'prakash.reddy226@example.com' -> 'pr***@example.com'
    Preserves first 2 characters of username, hides the rest with '***', preserves domain.
    """
    if not email or str(email).strip() == "" or str(email).strip().upper() in ("N/A", "NULL", "NONE"):
        return "masked@example.com"
    
    clean_email = str(email).strip()
    if "@" not in clean_email:
        return "masked@example.com"
    
    try:
        username, domain = clean_email.split("@", 1)
        if len(username) <= 2:
            masked_user = username[0] + "***" if len(username) > 0 else "***"
        else:
            masked_user = username[:2] + "***"
        return f"{masked_user}@{domain}"
    except Exception:
        return "masked@example.com"


def mask_phone(phone: str) -> str:
    """
    Masks 10-digit Indian phone numbers.
    Example: '9891411739' -> '98******39'
    Strips leading '+91-' or whitespace, shows first 2 and last 2 digits, masks middle 6 digits.
    """
    if not phone or str(phone).strip() == "" or str(phone).strip().upper() in ("N/A", "NULL", "NONE"):
        return "99******00"
    
    digits = re.sub(r"\D", "", str(phone).strip())
    # Strip country code 91 if 12 digits
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
        
    if len(digits) >= 4:
        return f"{digits[:2]}******{digits[-2:]}"
    return "99******00"


def standardise_payment_mode(pm: str) -> str:
    """
    Standardises raw payment mode strings into canonical values:
    'UPI', 'Card', 'Cash on Delivery', 'Wallet'.
    """
    if not pm:
        return "Unknown"
    norm = str(pm).strip().lower()
    if norm in ("upi",):
        return "UPI"
    if norm in ("card",):
        return "Card"
    if norm in ("cash on delivery", "cod"):
        return "Cash on Delivery"
    if norm in ("wallet",):
        return "Wallet"
    return str(pm).strip()


def standardise_order_status(status: str) -> str:
    """
    Standardises raw order status strings into canonical values:
    'DELIVERED', 'CANCELLED', 'RETURNED'.
    """
    if not status:
        return "UNKNOWN"
    norm = str(status).strip().upper()
    if norm in ("DELIVERED", "CANCELLED", "RETURNED"):
        return norm
    return str(status).strip()


def standardise_loyalty_tier(tier: str) -> str:
    """
    Standardises customer loyalty tier into proper case:
    'Gold', 'Silver', 'Bronze', 'Platinum'.
    """
    if not tier:
        return "Bronze"
    norm = str(tier).strip().capitalize()
    if norm in ("Gold", "Silver", "Bronze", "Platinum"):
        return norm
    return norm
