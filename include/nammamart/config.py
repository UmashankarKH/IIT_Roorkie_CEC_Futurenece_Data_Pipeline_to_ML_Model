"""Shared settings for both NammaMart pipelines.

Paths are resolved from this file so the same code runs inside Airflow
(/usr/local/airflow) and from a plain terminal.
"""

from __future__ import annotations

from pathlib import Path

OWNER = "dinesh"

INCLUDE_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = INCLUDE_DIR.parent
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
SQL_DIR = INCLUDE_DIR / "sql"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / OWNER
WAREHOUSE = INCLUDE_DIR / f"warehouse_{OWNER}.duckdb"

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_FILE = "orders_2026-10-01_increment.csv"

# Business rules, straight from the client brief.
VALID_ORDER_STATUS = ("DELIVERED", "CANCELLED", "RETURNED")
VALID_PAYMENT_MODE = ("UPI", "Card", "Cash on Delivery", "Wallet")
REVENUE_STATUS = "DELIVERED"
DELIVERY_SLA_MINUTES = 15
DELIVERY_MAX_MINUTES = 120          # anything above this is not believable
AMOUNT_TOLERANCE = 0.01
DATA_WINDOW_END = "2026-10-02"      # exports cover Sept + the 1 Oct increment

# Payment mode spellings seen in the profile, mapped onto the allowed list.
PAYMENT_MODE_MAP = {
    "upi": "UPI", "upi ": "UPI", "u p i": "UPI",
    "card": "Card", "credit card": "Card", "debit card": "Card",
    "cod": "Cash on Delivery", "cash on delivery": "Cash on Delivery",
    "cash": "Cash on Delivery", "cashh": "Cash on Delivery",
    "wallet": "Wallet",
}

LOYALTY_TIERS = ("BRONZE", "SILVER", "GOLD", "PLATINUM")
