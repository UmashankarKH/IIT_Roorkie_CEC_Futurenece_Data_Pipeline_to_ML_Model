
"""
NammaMart ETL DAG:
Extract -> Validate -> Quarantine -> Transform -> Load
"""

import json
import logging
import re
from pathlib import Path
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
import yaml

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule


# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "global_config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file) or {}


def resolve_path(value):
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


DATA_DIR = resolve_path(config.get("DATA_DIR", "include/data"))
WAREHOUSE_DB = resolve_path(
    config.get("WAREHOUSE_DB_ETL", "include/warehouse/warehouse_etl.duckdb")
)
QUARANTINE_DIR = resolve_path(
    config.get("QUARANTINE_DIR", "include/quarantine")
)
LOG_DIR = ROOT / "include" / "logs"
SQL_DIR = ROOT / "include" / "scripts" / "etl"

SOURCES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

RETRIES = int(config.get("RETRIES", 2))
RETRY_DELAY = int(config.get("RETRY_DELAY", 5))
SCHEDULE = config.get("SCHEDULE", "0 9 * * 1-6")
EXECUTION_TIMEOUT = int(config.get("EXECUTION_TIMEOUT", 30))
TIMEZONE = config.get("TIMEZONE", "Asia/Kolkata")
START_DATE = config.get("START_DATE", "2026-01-01")
CATCHUP = config.get("catchup", False)

TAGS = ["assignment", "nammamart", "ETL"]


# -------------------------------------------------------------------
# EXECUTION LOGGING
# -------------------------------------------------------------------

def execution_paths():
    """Return paths associated with the current Airflow DAG run."""
    context = get_current_context()
    run_id = context["run_id"]
    safe_run_id = re.sub(r"[^a-zA-Z0-9_.-]+", "_", run_id)

    run_dir = LOG_DIR / safe_run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    return run_id, run_dir, run_dir / "pipeline.log"


def log_phase(phase, message, level="INFO"):
    """Append a phase-level message to this DAG run's log."""
    run_id, run_dir, log_path = execution_paths()

    record = {
        "timestamp": pendulum.now(TIMEZONE).to_iso8601_string(),
        "run_id": run_id,
        "phase": phase,
        "level": level,
        "message": message,
    }

    with open(log_path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"[{phase}] {message}")


def artifact_path(filename):
    """Store intermediate files under this execution's logs directory."""
    _, run_dir, _ = execution_paths()
    return run_dir / filename


def read_sql(name, **values):
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    return sql.format(**values) if values else sql


def normalize_dataframe(df):
    """Normalize column names and trim string values."""
    df = df.copy()
    df.columns = [
        re.sub(
            r"_+",
            "_",
            re.sub(r"[^a-zA-Z0-9]+", "_", str(c).strip().lower()),
        ).strip("_")
        for c in df.columns
    ]

    for column in df.select_dtypes(include=["object", "string"]).columns:
        df[column] = df[column].astype("string").str.strip()

    return df


def save_frame(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def mask_pii(df):
    """Mask email and phone/mobile columns before loading dimensions."""
    df = df.copy()

    for column in df.columns:
        if "email" in column.lower():
            def mask_email(value):
                if pd.isna(value) or "@" not in str(value):
                    return "***" if pd.notna(value) else value
                local, domain = str(value).split("@", 1)
                return local[:2] + "***@" + domain

            df[column] = df[column].map(mask_email)

        elif "phone" in column.lower() or "mobile" in column.lower():
            def mask_phone(value):
                if pd.isna(value):
                    return value
                digits = re.sub(r"\D", "", str(value))
                return "*" * max(len(digits) - 4, 0) + digits[-4:]

            df[column] = df[column].map(mask_phone)

    return df


# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------

@dag(
    dag_id="nammamart_etl",
    start_date=pendulum.parse(str(START_DATE), tz=TIMEZONE),
    schedule=SCHEDULE,
    catchup=CATCHUP,
    default_args={
        "owner": "nammamart",
        "retries": RETRIES,
        "retry_delay": timedelta(minutes=RETRY_DELAY),
        "execution_timeout": timedelta(minutes=EXECUTION_TIMEOUT),
    },
    tags=TAGS,
    doc_md=__doc__,
)
def nammamart_etl():

    # ---------------------------------------------------------------
    # EXTRACT
    # Independent source files can be extracted in parallel.
    # ---------------------------------------------------------------

    def extract_task(table, filename):
        @task(task_id=f"extract_{table}")
        def _extract():
            try:
                source_path = DATA_DIR / filename

                if not source_path.exists():
                    raise FileNotFoundError(
                        f"Source file not found: {source_path}"
                    )

                df = pd.read_csv(
                    source_path,
                    dtype="string",
                    keep_default_na=False,
                )
                df = normalize_dataframe(df)

                output_path = artifact_path(f"{table}_raw.parquet")
                save_frame(df, output_path)

                log_phase(
                    "EXTRACT",
                    f"{table}: extracted {len(df)} rows from {filename}",
                )

                return {
                    "table": table,
                    "path": str(output_path),
                    "rows": len(df),
                }

            except Exception as exc:
                log_phase("EXTRACT", f"{table}: {exc}", "ERROR")
                raise

        return _extract

    # ---------------------------------------------------------------
    # VALIDATE
    # ---------------------------------------------------------------

    def validate_task(table):
        @task(task_id=f"validate_{table}")
        def _validate(meta):
            try:
                df = pd.read_parquet(meta["path"])
                reasons = pd.Series("", index=df.index, dtype="string")

                key_map = {
                    "orders": "order_id",
                    "payments": "payment_id",
                    "customers": "customer_id",
                    "products": "product_id",
                    "stores": "store_id",
                }

                key = key_map[table]

                if key not in df.columns:
                    raise ValueError(
                        f"{table}: missing required key column '{key}'. "
                        f"Available columns: {list(df.columns)}"
                    )

                missing_key = (
                    df[key].isna()
                    | df[key].astype("string").str.strip().eq("")
                )
                reasons.loc[missing_key] = f"missing_{key}"

                duplicate_key = df[key].duplicated(keep="first") & ~missing_key
                reasons.loc[
                    duplicate_key & reasons.eq("")
                ] = f"duplicate_{key}"

                if table == "orders":
                    if "customer_id" in df.columns:
                        missing_customer = (
                            df["customer_id"].isna()
                            | df["customer_id"].eq("")
                        )
                        reasons.loc[
                            missing_customer & reasons.eq("")
                        ] = "missing_customer_id"

                    if "quantity" in df.columns:
                        quantity = pd.to_numeric(
                            df["quantity"], errors="coerce"
                        )
                        invalid = quantity.isna() | (quantity <= 0)
                        reasons.loc[
                            invalid & reasons.eq("")
                        ] = "invalid_quantity"

                    if "discount_pct" in df.columns:
                        discount = pd.to_numeric(
                            df["discount_pct"], errors="coerce"
                        )
                        invalid = (
                            discount.isna()
                            | (discount < 0)
                            | (discount > 100)
                        )
                        reasons.loc[
                            invalid & reasons.eq("")
                        ] = "invalid_discount_pct"

                    if "order_status" in df.columns:
                        allowed = {"DELIVERED", "CANCELLED", "RETURNED"}
                        invalid = ~df["order_status"].str.upper().isin(allowed)
                        reasons.loc[
                            invalid & reasons.eq("")
                        ] = "invalid_order_status"

                    if "payment_mode" in df.columns:
                        allowed = {
                            "UPI", "CARD", "CASH ON DELIVERY", "WALLET"
                        }
                        invalid = ~df["payment_mode"].str.upper().isin(allowed)
                        reasons.loc[
                            invalid & reasons.eq("")
                        ] = "invalid_payment_mode"

                    if "order_ts" in df.columns:
                        timestamp = pd.to_datetime(
                            df["order_ts"], errors="coerce", utc=True
                        )
                        invalid = timestamp.isna()
                        reasons.loc[
                            invalid & reasons.eq("")
                        ] = "invalid_order_ts"

                    required = {
                        "quantity", "unit_price",
                        "discount_pct", "order_amount",
                    }

                    if required.issubset(df.columns):
                        quantity = pd.to_numeric(
                            df["quantity"], errors="coerce"
                        )
                        price = pd.to_numeric(
                            df["unit_price"], errors="coerce"
                        )
                        discount = pd.to_numeric(
                            df["discount_pct"], errors="coerce"
                        )
                        amount = pd.to_numeric(
                            df["order_amount"], errors="coerce"
                        )

                        expected = (quantity * price * (1 - discount / 100)).round(2)
                        mismatch = (amount - expected).abs() > 0.02

                        reasons.loc[
                            mismatch & reasons.eq("")
                        ] = "order_amount_formula_mismatch"

                elif table == "products":
                    if "category" in df.columns:
                        missing = (
                            df["category"].isna() | df["category"].eq("")
                        )
                        reasons.loc[
                            missing & reasons.eq("")
                        ] = "missing_category"

                    for column in ["mrp", "cost_price"]:
                        if column in df.columns:
                            value = pd.to_numeric(
                                df[column], errors="coerce"
                            )
                            invalid = value.isna() | (value <= 0)
                            reasons.loc[
                                invalid & reasons.eq("")
                            ] = f"invalid_{column}"

                    if {"mrp", "cost_price"}.issubset(df.columns):
                        mrp = pd.to_numeric(df["mrp"], errors="coerce")
                        cost = pd.to_numeric(
                            df["cost_price"], errors="coerce"
                        )
                        reasons.loc[
                            (cost > mrp) & reasons.eq("")
                        ] = "cost_price_exceeds_mrp"

                elif table == "payments" and "amount_paid" in df.columns:
                    amount = pd.to_numeric(
                        df["amount_paid"], errors="coerce"
                    )
                    invalid = amount.isna() | (amount < 0)
                    reasons.loc[
                        invalid & reasons.eq("")
                    ] = "invalid_amount_paid"

                failed = df.loc[reasons.ne("")].copy()
                failed["failure_reason"] = reasons.loc[reasons.ne("")]
                valid = df.loc[reasons.eq("")].copy()

                valid_path = artifact_path(f"{table}_valid.parquet")
                failed_path = artifact_path(f"{table}_failed.parquet")

                save_frame(valid, valid_path)
                save_frame(failed, failed_path)

                log_phase(
                    "VALIDATE",
                    f"{table}: checked={len(df)}, valid={len(valid)}, "
                    f"failed={len(failed)}",
                )

                return {
                    **meta,
                    "valid_path": str(valid_path),
                    "failed_path": str(failed_path),
                    "valid_rows": len(valid),
                    "failed_rows": len(failed),
                }

            except Exception as exc:
                log_phase("VALIDATE", f"{table}: {exc}", "ERROR")
                raise

        return _validate

    # ---------------------------------------------------------------
    # QUARANTINE
    # ---------------------------------------------------------------

    @task
    def quarantine_file(meta):
        try:
            failed = pd.read_parquet(meta["failed_path"])

            if not failed.empty:
                QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

                _, _, _ = execution_paths()
                run_id, _, _ = execution_paths()
                safe_run_id = re.sub(r"[^a-zA-Z0-9_.-]+", "_", run_id)

                output = (
                    QUARANTINE_DIR
                    / f"{meta['table']}_failed_{safe_run_id}.csv"
                )
                failed.to_csv(output, index=False)

                log_phase(
                    "QUARANTINE",
                    f"{meta['table']}: saved {len(failed)} failed rows",
                )
            else:
                log_phase(
                    "QUARANTINE",
                    f"{meta['table']}: no failed rows",
                )

            return {
                "table": meta["table"],
                "failed_rows": len(failed),
            }

        except Exception as exc:
            log_phase("QUARANTINE", f"{meta['table']}: {exc}", "ERROR")
            raise

    # ---------------------------------------------------------------
    # TRANSFORM
    # ---------------------------------------------------------------

    @task
    def transform_validated(metas):
        try:
            frames = {
                meta["table"]: pd.read_parquet(meta["valid_path"])
                for meta in metas
            }

            orders = frames["orders"].copy()
            customers = mask_pii(
                frames["customers"].drop_duplicates(
                    "customer_id", keep="first"
                )
            )
            products = frames["products"].drop_duplicates(
                "product_id", keep="first"
            )
            stores = frames["stores"].drop_duplicates(
                "store_id", keep="first"
            )

            for column in [
                "quantity", "unit_price",
                "discount_pct", "order_amount", "delivery_minutes",
            ]:
                if column in orders.columns:
                    orders[column] = pd.to_numeric(
                        orders[column], errors="coerce"
                    )

            if "order_ts" in orders.columns:
                orders["order_ts"] = pd.to_datetime(
                    orders["order_ts"], errors="coerce", utc=True
                )
                orders["order_date"] = (
                    orders["order_ts"].dt.strftime("%Y-%m-%d")
                )

            if {
                "quantity", "unit_price", "discount_pct"
            }.issubset(orders.columns):
                orders["net_revenue"] = (
                    orders["quantity"]
                    * orders["unit_price"]
                    * (1 - orders["discount_pct"] / 100)
                ).round(2)

            if {
                "quantity", "product_id"
            }.issubset(orders.columns) and {
                "product_id", "cost_price"
            }.issubset(products.columns):
                products["cost_price"] = pd.to_numeric(
                    products["cost_price"], errors="coerce"
                )
                orders = orders.merge(
                    products[["product_id", "cost_price"]],
                    on="product_id",
                    how="left",
                    suffixes=("", "_product"),
                )

                if "net_revenue" in orders.columns:
                    orders["gross_margin"] = (
                        orders["net_revenue"]
                        - orders["quantity"] * orders["cost_price"]
                    ).round(2)

            dimensions = {
                "customers": customers,
                "products": products,
                "stores": stores,
            }

            dimension_columns = {
                "customers": ["customer_id", "loyalty_tier", "city", "area"],
                "products": ["product_id", "product_name", "category"],
                "stores": ["store_id", "store_name", "zone", "area"],
            }

            for name, dimension in dimensions.items():
                key = dimension_columns[name][0]
                columns = [
                    column for column in dimension_columns[name]
                    if column in dimension.columns
                ]
                if key in orders.columns and len(columns) > 1:
                    extras = [
                        column for column in columns
                        if column != key and column not in orders.columns
                    ]
                    if extras:
                        orders = orders.merge(
                            dimension[[key] + extras].drop_duplicates(key),
                            on=key,
                            how="left",
                        )

            output_path = artifact_path("orders_transformed.parquet")
            save_frame(orders, output_path)

            for name, frame in dimensions.items():
                save_frame(
                    frame,
                    artifact_path(f"{name}_dimension.parquet"),
                )

            log_phase(
                "TRANSFORM",
                f"Transformed {len(orders)} order rows",
            )

            return {
                "orders_path": str(output_path),
                "tables": ["orders", "customers", "products", "stores"],
            }

        except Exception as exc:
            log_phase("TRANSFORM", str(exc), "ERROR")
            raise

    # ---------------------------------------------------------------
    # LOAD
    # ---------------------------------------------------------------

    @task
    def load_to_warehouse(meta):
        con = None

        try:
            WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(str(WAREHOUSE_DB))

            orders = pd.read_parquet(meta["orders_path"])
            con.register("incoming_orders", orders)

            con.execute("""
                CREATE TABLE IF NOT EXISTS etl_fact_orders AS
                SELECT * FROM incoming_orders WHERE 1=0
            """)

            existing_columns = [
                row[1] for row in con.execute(
                    "PRAGMA table_info('etl_fact_orders')"
                ).fetchall()
            ]

            if existing_columns != list(orders.columns):
                raise ValueError(
                    "The existing fact table schema differs from the "
                    "transformed orders schema. Handle the schema change "
                    "before loading."
                )

            if "order_id" not in orders.columns:
                raise ValueError("order_id is required for deduplication")

            con.execute(read_sql("insert_fact_orders.sql"))

            loaded_count = con.execute(
                "SELECT COUNT(*) FROM etl_fact_orders"
            ).fetchone()[0]

            con.unregister("incoming_orders")
            con.close()
            con = None

            log_phase(
                "LOAD",
                f"Warehouse load complete. Total fact rows={loaded_count}",
            )

            return {"orders_loaded": len(orders)}

        except Exception as exc:
            if con is not None:
                con.close()

            log_phase("LOAD", str(exc), "ERROR")
            raise

    # ---------------------------------------------------------------
    # DAG WIRING
    # ---------------------------------------------------------------

    extracted = {
        table: extract_task(table, filename)()
        for table, filename in SOURCES.items()
    }

    validated = {
        table: validate_task(table)(extracted[table])
        for table in SOURCES
    }

    quarantined = [
        quarantine_file(validated[table])
        for table in SOURCES
    ]

    transformed = transform_validated(
        [validated[table] for table in SOURCES]
    )

    loaded = load_to_warehouse(transformed)

    # Ensure quarantine tasks finish before transformation/loading.
    for quarantine_task in quarantined:
        quarantine_task >> transformed

    loaded


nammamart_etl()
