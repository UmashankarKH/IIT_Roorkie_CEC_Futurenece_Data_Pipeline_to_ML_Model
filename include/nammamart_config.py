"""Paths and constants for NammaMart assignment pipelines."""

STUDENT_NAME = "udaykiran_komati"
AIRFLOW_INCLUDE = "/usr/local/airflow/include"

DATA_DIR = f"{AIRFLOW_INCLUDE}/data/nammamart"
WAREHOUSE_DB = f"{AIRFLOW_INCLUDE}/warehouse_{STUDENT_NAME}.duckdb"
QUARANTINE_DIR = f"{AIRFLOW_INCLUDE}/quarantine/{STUDENT_NAME}"
SQL_DIR = f"{AIRFLOW_INCLUDE}/sql/{STUDENT_NAME}"

MASTER_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

INCREMENT_FILE = "orders_2026-10-01_increment.csv"

VALID_ORDER_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}

ETL_DAG_ID = f"nammamart_etl_{STUDENT_NAME}"
ELT_DAG_ID = f"nammamart_elt_{STUDENT_NAME}"

SCHEDULE_CRON = "0 9 * * 1-6"
TIMEZONE = "Asia/Kolkata"
