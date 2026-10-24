"""One place for names, paths and business constants."""
from pathlib import Path

NAME = "anikesh"          # <-- change to your own first name, lowercase, no spaces
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "include" / "data" / "nammamart"
QUARANTINE_DIR = ROOT / "include" / "quarantine" / NAME
SQL_DIR = ROOT / "include" / "sql"
WAREHOUSE = ROOT / "include" / f"warehouse_{NAME}.duckdb"

INCREMENT_FILE = "orders_2026-10-01_increment.csv"

ETL_DAG_ID = f"nammamart_etl_{NAME}"
ELT_DAG_ID = f"nammamart_elt_{NAME}"

ORDER_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
PAYMENT_MODE_MAP = {"upi": "UPI", "card": "Card",
                    "cash on delivery": "Cash on Delivery", "wallet": "Wallet"}
LOYALTY_TIERS = {"Bronze", "Silver", "Gold", "Platinum"}
ZONES = {"North", "South", "East", "West"}
MAX_DELIVERY_MIN = 120
SLA_MIN = 15