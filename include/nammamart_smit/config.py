"""
include/nammamart_smit/config.py
--------------------------------
Paths, file contracts and business rules shared by the ETL and ELT DAGs.
Every rule the client agreed on lives here, so a change (e.g. the 120-minute
delivery limit) is one reviewed edit, not a hunt through two pipelines.
"""

import os
import re
from datetime import date, datetime, timedelta

AIRFLOW_HOME   = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
INCLUDE_DIR    = os.path.join(AIRFLOW_HOME, "include")
DATA_DIR       = os.path.join(INCLUDE_DIR, "data", "nammamart")
SQL_DIR        = os.path.join(INCLUDE_DIR, "sql")
WAREHOUSE_DB   = os.path.join(INCLUDE_DIR, "warehouse_smit.duckdb")
QUARANTINE_DIR = os.path.join(INCLUDE_DIR, "quarantine", "smit")

# ── Source files and their expected columns (schema contract) ───────────────
SOURCES = {
    "orders": [
        "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
        "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
        "channel", "delivery_minutes",
    ],
    "payments": [
        "payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
        "payment_status", "gateway_ref",
    ],
    "customers": [
        "customer_id", "full_name", "email", "phone", "city", "area", "pincode",
        "signup_date", "loyalty_tier", "is_active",
    ],
    "products": [
        "product_id", "product_name", "category", "sub_category", "brand", "mrp",
        "cost_price", "unit", "is_perishable", "launch_date",
    ],
    "stores": [
        "store_id", "store_name", "zone", "area", "pincode", "open_date",
        "manager_name", "capacity_orders_per_day",
    ],
}
PRIMARY_KEYS = {
    "orders": "order_id", "payments": "payment_id", "customers": "customer_id",
    "products": "product_id", "stores": "store_id",
}
INCREMENT_PATTERN = re.compile(r"^orders_(\d{4}-\d{2}-\d{2})_increment\.csv$")

# ── Business rules agreed with ops / finance ─────────────────────────────────
VALID_ORDER_STATUS   = ["DELIVERED", "CANCELLED", "RETURNED"]
VALID_PAYMENT_MODE   = ["UPI", "Card", "Cash on Delivery", "Wallet"]
VALID_PAYMENT_STATUS = ["SUCCESS", "FAILED", "REFUNDED"]
VALID_LOYALTY_TIER   = ["Bronze", "Silver", "Gold", "Platinum"]
VALID_ZONE           = ["North", "South", "East", "West"]

# Known spellings seen in the exports -> canonical value (keys are lower-case, trimmed)
PAYMENT_MODE_ALIASES = {
    "upi": "UPI",
    "card": "Card",
    "wallet": "Wallet",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cashh": "Cash on Delivery",
}

MAX_DELIVERY_MINUTES    = 120   # above this the value is not believable (critical)
NORMAL_DELIVERY_MINUTES = 35    # 8-35 is normal; above is logged as a warning
SLA_MINUTES             = 15    # "within 15 minutes" KPI
AMOUNT_TOLERANCE        = 0.01
VOLUME_DROP_THRESHOLD   = 0.10  # warn if a file has < 10 % of last run's rows
PRICE_DRIFT_FACTOR      = 1.5   # warn if unit_price > 1.5 x product MRP
FRESHNESS_HOURS         = 24

# The September snapshot files are the export for this business date. Each
# 9 AM run processes the previous day's export, so "now" for the quality checks
# is business_date + 1 day at 09:00 IST. Using the business clock (not the
# laptop clock) keeps the checks reproducible when the data is replayed later.
BASE_BUSINESS_DATE = "2026-09-30"
RUN_HOUR_IST       = 9


def as_of_for(business_date: str) -> datetime:
    """The 9 AM IST run that processes a given business date's export."""
    d = date.fromisoformat(business_date) + timedelta(days=1)
    return datetime(d.year, d.month, d.day, RUN_HOUR_IST, 0, 0)


def business_date_for(file_name: str) -> str:
    match = INCREMENT_PATTERN.match(file_name)
    return match.group(1) if match else BASE_BUSINESS_DATE


def source_path(file_name: str) -> str:
    return os.path.join(DATA_DIR, file_name)
