"""
include/nammamart_masking.py
----------------------------
Privacy masking utility compliant with India's Digital Personal Data Protection (DPDP) Act 2023.
Masks customer Personal Identifiable Information (email and phone) for safe consumption in reporting/analytics tables.
"""

import re
import pandas as pd


def mask_email(email: str) -> str:
    """
    Masks email while keeping domain visible for reporting.
    Example: 'priya.hegde@example.com' -> 'pr***@example.com'
    """
    if pd.isna(email) or not email or str(email).strip().lower() in ('nan', 'none', ''):
        return ""
    
    email_str = str(email).strip()
    if '@' not in email_str:
        return "***"
    
    parts = email_str.split('@', 1)
    username, domain = parts[0], parts[1]
    
    if len(username) <= 2:
        masked_user = username[0] + "***" if len(username) >= 1 else "***"
    else:
        masked_user = username[:2] + "***"
        
    return f"{masked_user}@{domain}"


def mask_phone(phone) -> str:
    """
    Masks 10-digit phone keeping first 2 and last 2 digits.
    Example: 9876543221 -> '98******21'
    """
    if pd.isna(phone) or str(phone).strip().lower() in ('nan', 'none', ''):
        return ""
    
    digits = re.sub(r'\D', '', str(phone).strip())
    if len(digits) >= 10:
        return f"{digits[:2]}******{digits[-2:]}"
    elif len(digits) >= 4:
        return f"{digits[:2]}***{digits[-2:]}"
    else:
        return "******"
