"""
include/nammamart/config.py
---------------------------
Central configuration for the NammaMart ETL and ELT pipelines.

Why this file exists
--------------------
Paths, allowed values and thresholds are kept in one place so that the DAGs, the
helper modules, the tests and the local runner scripts all agree on the same
values.  Nothing here is machine specific: the project root is discovered at
import time, so the same code runs inside the Astro/Docker container
(``/usr/local/airflow``) and on a laptop.

Java comparison (learning aid, not an exact equivalent):
this module plays the role of ``application.yml`` / a ``@ConfigurationProperties``
class in a Spring Boot application.
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root discovery
# ---------------------------------------------------------------------------
# Order of preference:
#   1. NAMMAMART_PROJECT_ROOT environment variable (explicit override)
#   2. AIRFLOW_HOME  (inside the Astro container this is /usr/local/airflow)
#   3. two levels above this file  (<root>/include/nammamart/config.py)


def _discover_project_root() -> Path:
    env_root = os.getenv("NAMMAMART_PROJECT_ROOT")
    if env_root:
        return Path(env_root).resolve()

    airflow_home = os.getenv("AIRFLOW_HOME")
    if airflow_home and (Path(airflow_home) / "include").is_dir():
        return Path(airflow_home).resolve()

    return Path(__file__).resolve().parents[2]


PROJECT_ROOT: Path = _discover_project_root()
INCLUDE_DIR: Path = PROJECT_ROOT / "include"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
STUDENT = os.getenv("NAMMAMART_STUDENT", "waseem")

DATA_DIR: Path = Path(os.getenv("NAMMAMART_DATA_DIR", INCLUDE_DIR / "data" / "nammamart"))
SQL_DIR: Path = INCLUDE_DIR / "sql"
QUARANTINE_DIR: Path = Path(
    os.getenv("NAMMAMART_QUARANTINE_DIR", INCLUDE_DIR / "quarantine" / STUDENT)
)
WAREHOUSE_PATH: Path = Path(
    os.getenv("NAMMAMART_WAREHOUSE", INCLUDE_DIR / f"warehouse_{STUDENT}.duckdb")
)
REPORTS_DIR: Path = Path(os.getenv("NAMMAMART_REPORTS_DIR", PROJECT_ROOT / "docs" / "evidence"))

# Source files -------------------------------------------------------------
SOURCE_FILES: dict[str, str] = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_FILE = "orders_2026-10-01_increment.csv"

# Natural (business) key of each dataset — used for dedupe, upsert and quarantine keys.
PRIMARY_KEYS: dict[str, str] = {
    "orders": "order_id",
    "payments": "payment_id",
    "customers": "customer_id",
    "products": "product_id",
    "stores": "store_id",
}

# ---------------------------------------------------------------------------
# Business rules from the client brief (assignment section 3)
# ---------------------------------------------------------------------------
ALLOWED_ORDER_STATUS = ("DELIVERED", "CANCELLED", "RETURNED")
ALLOWED_PAYMENT_MODE = ("UPI", "Card", "Cash on Delivery", "Wallet")
ALLOWED_PAYMENT_STATUS = ("SUCCESS", "FAILED", "PENDING", "REFUNDED")
ALLOWED_LOYALTY_TIER = ("BRONZE", "SILVER", "GOLD", "PLATINUM")
ALLOWED_ZONES = ("North", "South", "East", "West")
ALLOWED_CHANNELS = ("App", "Web", "Phone")

REVENUE_STATUS = "DELIVERED"          # only DELIVERED orders count as revenue
SLA_MINUTES = 15                      # "within 15 minutes" KPI
MAX_DELIVERY_MINUTES = 120            # above this a delivery time is not believable
MIN_DELIVERY_MINUTES = 0
AMOUNT_TOLERANCE = 0.01               # rupee tolerance when comparing money values

# Timeliness thresholds (Part D)
MAX_DATA_AGE_DAYS = 1                 # latest order should not be older than this at run time

# Reference date for every "must not be in the future" rule. The extract covers
# September plus the 1 October increment, so anything after the end of 1 October
# is future-dated. A fixed date keeps results identical on any day the pipeline
# is run; the freshness rule (RULE-035) deliberately still uses the real clock.
AS_OF: str = os.getenv("NAMMAMART_AS_OF", "2026-10-02 00:00:00")
# Volume check (Part F experiment 7): warn when a file drops by more than this share.
VOLUME_DROP_WARN_PCT = 90.0
EXPECTED_ROW_COUNTS: dict[str, int] = {
    "orders": 2412,
    "payments": 2228,
    "customers": 405,
    "products": 152,
    "stores": 15,
}

# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------
ETL_TABLES = {
    "fact_orders": "etl_fact_orders",
    "dim_customer": "etl_dim_customer",
    "dim_product": "etl_dim_product",
    "dim_store": "etl_dim_store",
    "reconciliation": "etl_payment_reconciliation",
    "daily_zone_revenue": "etl_daily_zone_revenue",
    "category_margin": "etl_category_margin",
}

ELT_RAW_TABLES = {name: f"raw_{name}" for name in SOURCE_FILES}
ELT_STAGING_TABLES = {name: f"stg_{name}" for name in SOURCE_FILES}
ELT_MART_TABLES = (
    "mart_daily_store_kpis",
    "mart_category_margin",
    "mart_delivery_sla",
    "mart_payment_reconciliation",
    "mart_top_customers",
)

DQ_RESULTS_TABLE = "dq_results"
PIPELINE_RUNS_TABLE = "pipeline_runs"
QUARANTINE_TABLE = "etl_quarantine"


def ensure_directories() -> None:
    """Create the output folders the pipelines write to (safe to call repeatedly)."""
    for folder in (QUARANTINE_DIR, REPORTS_DIR, WAREHOUSE_PATH.parent):
        folder.mkdir(parents=True, exist_ok=True)


def source_path(dataset: str) -> Path:
    """Return the full path of a source CSV, e.g. ``source_path("orders")``."""
    try:
        return DATA_DIR / SOURCE_FILES[dataset]
    except KeyError as exc:  # pragma: no cover - defensive
        raise KeyError(f"Unknown dataset '{dataset}'. Known: {sorted(SOURCE_FILES)}") from exc


def increment_path() -> Path:
    """Return the full path of the next-day incremental orders file."""
    return DATA_DIR / INCREMENT_FILE
