"""
Paths, business rules and the data-quality check registry.

The CHECKS registry is the single source of truth for check names, dimensions
and severities. The ETL pipeline implements each check in pandas
(include/nammamart_divya/dq.py) and the ELT pipeline implements the same check
in SQL (include/sql/staging/*.sql) as a boolean column named f_<check_name>.
"""

import os
from pathlib import Path

# Works inside the Astro container and when run locally from the project root.
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
PROJECT_ROOT = AIRFLOW_HOME if (AIRFLOW_HOME / "include").exists() else Path(__file__).resolve().parents[2]

INCLUDE_DIR = PROJECT_ROOT / "include"
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
INCREMENT_DIR = DATA_DIR / "increments"          # drop orders_<date>_increment.csv files here
SQL_DIR = INCLUDE_DIR / "sql"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / "divya"
WAREHOUSE_DB = Path(os.environ.get("NAMMAMART_WAREHOUSE", INCLUDE_DIR / "warehouse_divya.duckdb"))

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

# Exact column contract per file; a missing column fails the extract loudly.
EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
               "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
               "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                  "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                 "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date",
               "manager_name", "capacity_orders_per_day"],
}

# ── Business rules agreed with the client ────────────────────────────────────
VALID_ORDER_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
VALID_PAYMENT_MODES = ["UPI", "Card", "Cash on Delivery", "Wallet"]
VALID_PAYMENT_STATUS = ["SUCCESS", "FAILED", "REFUNDED"]
VALID_ZONES = ["North", "South", "East", "West"]
VALID_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]

# Lower-cased, trimmed raw value -> standard value.
PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cashh": "Cash on Delivery",
    "wallet": "Wallet",
}

# Used to fill a blank product category (every sub_category belongs to one category).
SUBCATEGORY_TO_CATEGORY = {
    "Fresh Vegetables": "Fruits & Vegetables", "Fresh Fruits": "Fruits & Vegetables",
    "Herbs": "Fruits & Vegetables",
    "Milk": "Dairy & Bakery", "Curd & Paneer": "Dairy & Bakery", "Bread": "Dairy & Bakery",
    "Cheese": "Dairy & Bakery",
    "Rice": "Staples", "Atta": "Staples", "Dal": "Staples", "Oil": "Staples",
    "Soft Drinks": "Snacks & Beverages", "Tea & Coffee": "Snacks & Beverages",
    "Biscuits": "Snacks & Beverages", "Chips": "Snacks & Beverages",
    "Shampoo": "Personal Care", "Soap": "Personal Care", "Oral Care": "Personal Care",
    "Detergent": "Household", "Kitchen Supplies": "Household", "Cleaners": "Household",
}

MAX_DELIVERY_MINUTES = 120        # approved by Ops (see SUBMISSION.md, governance)
SLA_MINUTES = 15
AMOUNT_TOLERANCE = 0.01
VOLUME_DROP_THRESHOLD = 0.10      # warn if today's rows < 10% of the previous run
PRICE_DRIFT_FACTOR = 3.0          # warn if unit_price > 3x catalogue MRP
STALE_AFTER_DAYS = 1

# Business date of the latest export. Anything time-stamped after the end of this
# day is "future-dated". Override with the Airflow Variable nammamart_business_date.
DEFAULT_BUSINESS_DATE = "2026-10-01"

CRITICAL, WARNING = "Critical", "Warning"

# check_name -> (file, dimension, severity, failure_reason text)
CHECKS = {
    # orders
    "ord_customer_id_not_null":  ("orders", "Completeness", CRITICAL, "customer_id is null"),
    "ord_delivered_has_minutes": ("orders", "Completeness", CRITICAL, "DELIVERED order has no delivery_minutes"),
    "ord_order_id_unique":       ("orders", "Uniqueness",   CRITICAL, "duplicate order_id"),
    "ord_status_valid":          ("orders", "Validity",     CRITICAL, "order_status not in allowed list"),
    "ord_payment_mode_valid":    ("orders", "Validity",     CRITICAL, "payment_mode not in allowed list"),
    "ord_ts_parseable":          ("orders", "Validity",     CRITICAL, "order_ts is not a valid timestamp"),
    "ord_discount_in_range":     ("orders", "Validity",     CRITICAL, "discount_pct outside 0-100"),
    "ord_quantity_positive":     ("orders", "Accuracy",     CRITICAL, "quantity <= 0"),
    "ord_amount_matches_formula": ("orders", "Accuracy",    CRITICAL, "order_amount != quantity x unit_price x (1 - discount)"),
    "ord_delivery_under_limit":  ("orders", "Accuracy",     CRITICAL, "delivery_minutes >= 120"),
    "ord_store_exists":          ("orders", "Consistency",  CRITICAL, "store_id not in store master"),
    "ord_product_exists":        ("orders", "Consistency",  CRITICAL, "product_id not in clean product master"),
    "ord_customer_exists":       ("orders", "Consistency",  CRITICAL, "customer_id not in customer master"),
    "ord_ts_not_future":         ("orders", "Timeliness",   CRITICAL, "order_ts is in the future"),
    "ord_value_casing":          ("orders", "Consistency",  WARNING,  "order_status/payment_mode needed standardising"),
    "ord_price_drift":           ("orders", "Accuracy",     WARNING,  "unit_price more than 3x catalogue MRP"),
    "ord_volume_drop":           ("orders", "Completeness", WARNING,  "order volume dropped > 90% vs previous run"),
    "ord_freshness":             ("orders", "Timeliness",   WARNING,  "latest order_ts older than 1 day at run time"),
    # payments
    "pay_payment_id_unique":     ("payments", "Uniqueness",   CRITICAL, "duplicate payment_id"),
    "pay_order_exists":          ("payments", "Consistency",  CRITICAL, "order_id not in orders file"),
    "pay_mode_valid":            ("payments", "Validity",     CRITICAL, "payment_mode not in allowed list"),
    "pay_status_valid":          ("payments", "Validity",     CRITICAL, "payment_status not in allowed list"),
    "pay_ts_parseable":          ("payments", "Validity",     CRITICAL, "payment_ts is not a valid timestamp"),
    "pay_amount_positive":       ("payments", "Accuracy",     CRITICAL, "amount_paid <= 0"),
    "pay_ts_not_future":         ("payments", "Timeliness",   CRITICAL, "payment_ts is in the future"),
    "pay_gateway_ref_present":   ("payments", "Completeness", WARNING,  "non-COD payment has no gateway_ref"),
    "pay_value_casing":          ("payments", "Consistency",  WARNING,  "payment_mode needed standardising"),
    # customers
    "cust_id_not_null":          ("customers", "Completeness", CRITICAL, "customer_id is null"),
    "cust_id_unique":            ("customers", "Uniqueness",   CRITICAL, "duplicate customer_id"),
    "cust_city_present":         ("customers", "Completeness", WARNING,  "city is blank"),
    "cust_email_valid":          ("customers", "Validity",     WARNING,  "email format invalid"),
    "cust_phone_valid":          ("customers", "Validity",     WARNING,  "phone is not 10 digits"),
    "cust_pincode_valid":        ("customers", "Validity",     WARNING,  "pincode is not 6 digits"),
    "cust_tier_casing":          ("customers", "Consistency",  WARNING,  "loyalty_tier casing not standard"),
    "cust_signup_not_future":    ("customers", "Timeliness",   WARNING,  "signup_date is in the future"),
    # products
    "prod_id_unique":            ("products", "Uniqueness",   CRITICAL, "duplicate product_id (superseded row)"),
    "prod_mrp_positive":         ("products", "Accuracy",     CRITICAL, "mrp <= 0"),
    "prod_launch_not_future":    ("products", "Timeliness",   CRITICAL, "launch_date is in the future"),
    "prod_category_present":     ("products", "Completeness", WARNING,  "category blank (inferred from sub_category)"),
    "prod_cost_le_mrp":          ("products", "Accuracy",     WARNING,  "cost_price > mrp"),
    # stores
    "store_id_unique":           ("stores", "Uniqueness",   CRITICAL, "duplicate store_id"),
    "store_zone_present":        ("stores", "Completeness", WARNING,  "zone is blank (set to Unknown)"),
    "store_pincode_valid":       ("stores", "Validity",     WARNING,  "pincode is not 6 digits"),
    "store_capacity_positive":   ("stores", "Accuracy",     WARNING,  "capacity_orders_per_day <= 0"),
}


def checks_for(file_key):
    return {k: v for k, v in CHECKS.items() if v[0] == file_key}
