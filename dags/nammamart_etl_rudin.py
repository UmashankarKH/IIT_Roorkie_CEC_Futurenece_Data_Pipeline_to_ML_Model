"""
dags/namma_etl_rudin.py
-----------------------
NammaMart ETL pipeline.

Flow:
  five parallel extracts -> Python validation -> quarantine/transform
  -> idempotent DuckDB star-schema load -> reconciliation and reports

The pipeline reads the untouched CSV exports from include/data/nammamart,
keeps failed rows outside the finance tables, and writes the personal
warehouse to include/warehouse_rudin.duckdb.
"""

import json
import os
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sdk import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from include.slack_notifier import send_slack_alert


DATA_DIR = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_rudin.duckdb"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/rudin"

ALLOWED_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
EXPECTED_ORDER_ROWS = 2550
MINIMUM_ORDER_VOLUME = int(EXPECTED_ORDER_ROWS * 0.10)

REQUIRED_COLUMNS = {
    "orders.csv": {
        "order_id", "order_ts", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
        "order_status", "channel", "delivery_minutes",
    },
    "orders_2026-10-01_increment.csv": {
        "order_id", "order_ts", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
        "order_status", "channel", "delivery_minutes",
    },
    "payments.csv": {"payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"},
    "customers.csv": {"customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"},
    "products.csv": {"product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"},
    "stores.csv": {"store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"},
}

default_args = {
    "owner": "fde-cohort",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _read_json(value: str) -> pd.DataFrame:
    """Read a DataFrame serialized by an upstream TaskFlow task."""
    return pd.read_json(value, orient="records")


def _json_records(frame: pd.DataFrame) -> str:
    """Serialize a DataFrame without converting its columns into index keys."""
    return frame.to_json(orient="records", date_format="iso")


def _assert_required_columns(frame: pd.DataFrame, filename: str) -> None:
    """Fail fast on a source schema change instead of silently creating nulls."""
    missing = REQUIRED_COLUMNS[filename] - set(frame.columns)
    if missing:
        raise ValueError(f"Schema validation failed for {filename}; missing columns: {sorted(missing)}")


def _blank(value) -> bool:
    """Return whether a source value is missing or contains only whitespace."""
    return pd.isna(value) or str(value).strip() == ""


def _add_failure(failures, index, reason: str) -> None:
    """Add a failure reason to a row while preserving multiple failed rules."""
    failures.setdefault(index, []).append(reason)


def _mask_email(value: str) -> str:
    """Keep enough email structure for support while hiding the local part."""
    if _blank(value) or "@" not in str(value):
        return "***"
    local, domain = str(value).split("@", 1)
    return f"{local[:2]}***@{domain}"


def _mask_phone(value: str) -> str:
    """Keep only the last two phone digits for masked reporting tables."""
    value = str(value)
    return f"{'*' * max(len(value) - 2, 0)}{value[-2:]}" if value else "***"


def _mask_name(value: str) -> str:
    """Reduce a customer name to initials so rejected rows carry no identifier."""
    parts = [part for part in str(value).split() if part]
    if not parts:
        return "***"
    return " ".join(f"{part[0]}***" for part in parts)


# Direct identifiers are masked in every quarantine file so that rejected
# customer rows can be reviewed without republishing personal data.
QUARANTINE_MASKED_COLUMNS = {
    "customers": {"full_name": _mask_name, "email": _mask_email, "phone": _mask_phone},
}


def _as_of_date() -> pd.Timestamp:
    """Return the reference date used by the freshness checks.

    Defaults to the current run date in Asia/Kolkata and can be pinned with
    the NAMMAMART_AS_OF_DATE environment variable so a historical run can be
    reproduced exactly.
    """
    configured = os.environ.get("NAMMAMART_AS_OF_DATE")
    if configured:
        return pd.Timestamp(configured).normalize()
    return pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None).normalize()


def _dq_row(check_name, dimension, severity, rows_checked, rows_failed, run_id, dag_id):
    """Create one scorecard record for a Python data-quality check."""
    status = "PASS" if rows_failed == 0 else "WARN" if severity == "Warning" else "FAIL"
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "check_name": check_name,
        "dimension": dimension,
        "severity": severity,
        "rows_checked": int(rows_checked),
        "rows_failed": int(rows_failed),
        "status": status,
        "checked_at": pendulum.now("UTC").to_datetime_string(),
    }


@dag(
    dag_id="nammamart_etl_rudin",
    description="NammaMart Python ETL pipeline; supports scheduled and manual runs.",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fde", "nammamart", "etl"],
)
def nammamart_etl_rudin():
    # -------------------- EXTRACT: full orders snapshot ----------------------
    # Each task returns its own JSON payload, so Airflow can run all five
    # source reads in parallel and pass the results through XCom.
    @task
    def extract_orders():
        base = pd.read_csv(f"{DATA_DIR}/orders.csv", dtype=str, keep_default_na=False)
        increment = pd.read_csv(
            f"{DATA_DIR}/orders_2026-10-01_increment.csv",
            dtype=str,
            keep_default_na=False,
        )
        _assert_required_columns(base, "orders.csv")
        _assert_required_columns(increment, "orders_2026-10-01_increment.csv")
        # Full-load parity: the incremental snapshot contains 10 corrections
        # and 150 new orders. Keep the latest received version per order_id so
        # ETL and ELT publish the same current-state order population.
        base["_source_priority"] = 0
        increment["_source_priority"] = 1
        frame = pd.concat([base, increment], ignore_index=True)
        frame = (
            frame.sort_values(["order_id", "_source_priority"])
            .drop_duplicates("order_id", keep="last")
            .drop(columns=["_source_priority"])
            .reset_index(drop=True)
        )
        print(
            f"[Extract] orders.csv={len(base)} rows; "
            f"orders_2026-10-01_increment.csv={len(increment)} rows; "
            f"deduplicated full snapshot={len(frame)} rows"
        )
        return _json_records(frame)

    @task
    def extract_payments():
        frame = pd.read_csv(f"{DATA_DIR}/payments.csv", dtype=str, keep_default_na=False)
        _assert_required_columns(frame, "payments.csv")
        print(f"[Extract] payments.csv: {len(frame)} rows")
        return _json_records(frame)

    @task
    def extract_customers():
        frame = pd.read_csv(f"{DATA_DIR}/customers.csv", dtype=str, keep_default_na=False)
        _assert_required_columns(frame, "customers.csv")
        print(f"[Extract] customers.csv: {len(frame)} rows")
        return _json_records(frame)

    @task
    def extract_products():
        frame = pd.read_csv(f"{DATA_DIR}/products.csv", dtype=str, keep_default_na=False)
        _assert_required_columns(frame, "products.csv")
        print(f"[Extract] products.csv: {len(frame)} rows")
        return _json_records(frame)

    @task
    def extract_stores():
        frame = pd.read_csv(f"{DATA_DIR}/stores.csv", dtype=str, keep_default_na=False)
        _assert_required_columns(frame, "stores.csv")
        print(f"[Extract] stores.csv: {len(frame)} rows")
        return _json_records(frame)

    # -------------------- VALIDATE: Python data-quality rules ---------------
    @task
    def validate(orders_json, payments_json, customers_json, products_json, stores_json):
        """Split each source into passed/failed rows and retain failure reasons."""
        context = get_current_context()
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        orders = _read_json(orders_json)
        payments = _read_json(payments_json)
        customers = _read_json(customers_json)
        products = _read_json(products_json)
        stores = _read_json(stores_json)
        frames = {"orders": orders, "payments": payments, "customers": customers, "products": products, "stores": stores}
        failures = {name: {} for name in frames}

        # Master keys support the cross-file referential-integrity checks.
        customer_ids = set(customers["customer_id"])
        product_ids = set(products["product_id"])
        store_ids = set(stores["store_id"])
        order_ids = set(orders["order_id"])

        # Primary-key, completeness, and uniqueness checks for every source.
        key_columns = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id", "products": "product_id", "stores": "store_id"}
        for name, frame in frames.items():
            key = key_columns[name]
            for index, value in frame[key].items():
                if _blank(value):
                    _add_failure(failures[name], index, f"{key} is blank")
            duplicate_rows = frame[key].duplicated(keep=False)
            for index in frame.index[duplicate_rows]:
                _add_failure(failures[name], index, f"duplicate {key}")

        # Orders: required keys, allowed values, arithmetic, timestamps, and SLA.
        for index, row in orders.iterrows():
            if _blank(row.customer_id) or row.customer_id not in customer_ids:
                _add_failure(failures["orders"], index, "customer_id is missing or unknown")
            if _blank(row.product_id) or row.product_id not in product_ids:
                _add_failure(failures["orders"], index, "product_id is missing or unknown")
            if _blank(row.store_id) or row.store_id not in store_ids:
                _add_failure(failures["orders"], index, "store_id is missing or unknown")
            status = str(row.order_status).strip().upper()
            mode = str(row.payment_mode).strip()
            mode = {"upi": "UPI", "card": "Card", "cod": "Cash on Delivery", "cash on delivery": "Cash on Delivery", "wallet": "Wallet"}.get(mode.lower(), mode)
            if status not in ALLOWED_STATUS:
                _add_failure(failures["orders"], index, f"invalid order_status: {row.order_status}")
            if mode not in ALLOWED_PAYMENT_MODES:
                _add_failure(failures["orders"], index, f"invalid payment_mode: {row.payment_mode}")
            quantity, unit_price, discount, amount = [pd.to_numeric(row[column], errors="coerce") for column in ["quantity", "unit_price", "discount_pct", "order_amount"]]
            if pd.isna(quantity) or quantity <= 0:
                _add_failure(failures["orders"], index, "quantity must be greater than zero")
            if pd.notna(discount) and not 0 <= discount <= 100:
                _add_failure(failures["orders"], index, "discount_pct must be between 0 and 100")
            if all(pd.notna(value) for value in [quantity, unit_price, discount, amount]):
                expected = round(quantity * unit_price * (1 - discount / 100), 2)
                if abs(amount - expected) > 0.01:
                    _add_failure(failures["orders"], index, f"order_amount does not match calculated value {expected}")
            order_ts = pd.to_datetime(row.order_ts, errors="coerce")
            if pd.isna(order_ts):
                _add_failure(failures["orders"], index, "order_ts cannot be parsed")
            elif order_ts > pd.Timestamp("2026-10-09 23:59:59"):
                _add_failure(failures["orders"], index, "order_ts is future dated")
            delivery = pd.to_numeric(row.delivery_minutes, errors="coerce")
            if status == "DELIVERED" and pd.isna(delivery):
                _add_failure(failures["orders"], index, "DELIVERED order has no delivery_minutes")
            if pd.notna(delivery) and delivery >= 120:
                _add_failure(failures["orders"], index, "delivery_minutes must be below 120")

        # Payments: validate keys, allowed values, timestamps, and reconciliation.
        order_amounts = orders.groupby("order_id")["order_amount"].first()
        for index, row in payments.iterrows():
            payment_mode = str(row.payment_mode).strip()
            payment_mode = {"upi": "UPI", "card": "Card", "cod": "Cash on Delivery", "cash on delivery": "Cash on Delivery", "wallet": "Wallet"}.get(payment_mode.lower(), payment_mode)
            if payment_mode not in ALLOWED_PAYMENT_MODES:
                _add_failure(failures["payments"], index, f"invalid payment_mode: {row.payment_mode}")
            if row.order_id not in order_ids:
                _add_failure(failures["payments"], index, "payment references unknown order")
            if pd.isna(pd.to_datetime(row.payment_ts, errors="coerce")):
                _add_failure(failures["payments"], index, "payment_ts cannot be parsed")
            if str(row.payment_status).strip().upper() == "SUCCESS":
                paid = pd.to_numeric(row.amount_paid, errors="coerce")
                expected = pd.to_numeric(order_amounts.get(row.order_id), errors="coerce")
                if pd.notna(paid) and pd.notna(expected) and abs(paid - expected) > 0.01:
                    _add_failure(failures["payments"], index, "SUCCESS amount_paid does not equal order_amount")

        # Customer, product, and store checks. PII format failures are warnings
        # in this DAG and do not prevent a row from loading into masked dimensions.
        for index, row in customers.iterrows():
            if not str(row.email).strip() or "@" not in str(row.email):
                print(f"[Validate][Warning] customer {row.customer_id} has an invalid email")
            if not str(row.phone).isdigit() or len(str(row.phone)) != 10:
                print(f"[Validate][Warning] customer {row.customer_id} has an invalid phone")
            signup_date = pd.to_datetime(row.signup_date, errors="coerce")
            if pd.isna(signup_date) or signup_date > pd.Timestamp("2026-10-09"):
                _add_failure(failures["customers"], index, "signup_date is invalid or future dated")
        for index, row in products.iterrows():
            mrp = pd.to_numeric(row.mrp, errors="coerce")
            cost_price = pd.to_numeric(row.cost_price, errors="coerce")
            if _blank(row.category):
                _add_failure(failures["products"], index, "category is blank")
            if pd.isna(mrp) or mrp <= 0:
                _add_failure(failures["products"], index, "mrp must be positive")
            if pd.notna(mrp) and pd.notna(cost_price) and cost_price > mrp:
                _add_failure(failures["products"], index, "cost_price exceeds mrp")
        for index, row in stores.iterrows():
            if _blank(row.zone):
                _add_failure(failures["stores"], index, "zone is blank")

        # Persist a Python-generated quality scorecard for this validation run.
        # Critical checks correspond to quarantine rules; warnings are logged
        # while their rows remain eligible for the reporting tables.
        def failed_count(frame, *masks):
            combined = pd.Series(False, index=frame.index)
            for mask in masks:
                combined = combined | pd.Series(mask, index=frame.index).fillna(False)
            return int(combined.sum())

        order_status = orders["order_status"].str.strip().str.upper()
        order_modes = orders["payment_mode"].str.strip().str.lower().replace({"upi": "UPI", "card": "Card", "cod": "Cash on Delivery", "cash on delivery": "Cash on Delivery", "wallet": "Wallet"})
        payment_modes = payments["payment_mode"].str.strip().str.lower().replace({"upi": "UPI", "card": "Card", "cod": "Cash on Delivery", "cash on delivery": "Cash on Delivery", "wallet": "Wallet"})
        order_quantity = pd.to_numeric(orders["quantity"], errors="coerce")
        order_discount = pd.to_numeric(orders["discount_pct"], errors="coerce")
        order_amount = pd.to_numeric(orders["order_amount"], errors="coerce")
        order_unit_price = pd.to_numeric(orders["unit_price"], errors="coerce")
        order_delivery = pd.to_numeric(orders["delivery_minutes"], errors="coerce")
        expected_amount = (order_quantity * order_unit_price * (1 - order_discount / 100)).round(2)
        order_ts = pd.to_datetime(orders["order_ts"], errors="coerce")
        # Duplicate product keys are a known source-quality issue. Use the
        # same last-record-wins rule as the transform step before mapping MRP,
        # so the DQ scorecard itself remains robust on duplicate inputs.
        product_mrp = pd.to_numeric(
            products.drop_duplicates("product_id", keep="last").set_index("product_id")["mrp"],
            errors="coerce",
        )
        order_mrp = orders["product_id"].map(product_mrp)
        price_distribution_drift = order_unit_price > (2 * order_mrp)
        # The client rule for timeliness: the newest order in the snapshot
        # must not be older than one day at run time.
        as_of_date = _as_of_date()
        latest_order_ts = order_ts.max() if order_ts.notna().any() else pd.NaT
        snapshot_stale = 1 if (pd.isna(latest_order_ts) or latest_order_ts < as_of_date - pd.Timedelta(days=1)) else 0
        signup_date = pd.to_datetime(customers["signup_date"], errors="coerce")
        payment_paid = pd.to_numeric(payments["amount_paid"], errors="coerce")
        payment_expected = payments["order_id"].map(order_amounts).pipe(pd.to_numeric, errors="coerce")
        payment_success = payments["payment_status"].str.strip().str.upper().eq("SUCCESS")
        payment_amount_mismatch = payment_success & payment_paid.notna() & payment_expected.notna() & ((payment_paid - payment_expected).abs() > 0.01)
        # JSON/XCom round-tripping can infer an all-numeric PII column as a
        # numeric dtype. Convert to pandas' nullable string dtype before using
        # the string accessor so malformed source values become validation
        # failures/warnings instead of crashing the task.
        email_values = customers["email"].astype("string")
        phone_values = customers["phone"].astype("string")
        dq_records = [
            _dq_row("required_fields", "Completeness", "Critical", len(orders) + len(payments) + len(customers) + len(products) + len(stores),
                    failed_count(orders, orders[["order_id", "order_ts", "customer_id", "store_id", "product_id"]].apply(lambda column: column.map(_blank)).any(axis=1)) + failed_count(payments, payments[["payment_id", "order_id", "payment_ts", "amount_paid"]].apply(lambda column: column.map(_blank)).any(axis=1)) + failed_count(customers, customers[["customer_id", "signup_date"]].apply(lambda column: column.map(_blank)).any(axis=1)) + failed_count(products, products[["product_id", "category"]].apply(lambda column: column.map(_blank)).any(axis=1)) + failed_count(stores, stores[["store_id", "zone"]].apply(lambda column: column.map(_blank)).any(axis=1)), run_id, dag_id),
            _dq_row("primary_keys_unique", "Uniqueness", "Critical", len(orders) + len(payments) + len(customers) + len(products) + len(stores),
                    int(orders.order_id.duplicated(keep=False).sum() + payments.payment_id.duplicated(keep=False).sum() + customers.customer_id.duplicated(keep=False).sum() + products.product_id.duplicated(keep=False).sum() + stores.store_id.duplicated(keep=False).sum()), run_id, dag_id),
            _dq_row("allowed_values_and_formats", "Validity", "Critical", len(orders) + len(payments),
                    failed_count(orders, ~order_status.isin(ALLOWED_STATUS), ~order_modes.isin(ALLOWED_PAYMENT_MODES), order_ts.isna()) + failed_count(payments, ~payment_modes.isin(ALLOWED_PAYMENT_MODES), pd.to_datetime(payments.payment_ts, errors="coerce").isna()), run_id, dag_id),
            _dq_row("pii_format", "Validity", "Warning", len(customers),
                    failed_count(customers, ~email_values.str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", na=False), ~phone_values.str.match(r"^\d{10}$", na=False)), run_id, dag_id),
            _dq_row("numeric_business_rules", "Accuracy", "Critical", len(orders) + len(products),
                    failed_count(orders, order_quantity.isna() | (order_quantity <= 0), order_discount.isna() | ~order_discount.between(0, 100), (order_amount - expected_amount).abs() > 0.01, order_delivery.notna() & (order_delivery >= 120)) + failed_count(products, pd.to_numeric(products.mrp, errors="coerce").isna() | (pd.to_numeric(products.mrp, errors="coerce") <= 0), pd.to_numeric(products.cost_price, errors="coerce") > pd.to_numeric(products.mrp, errors="coerce")), run_id, dag_id),
            _dq_row("foreign_keys_and_payment_match", "Consistency", "Critical", len(orders) + len(payments),
                    failed_count(orders, ~orders.customer_id.isin(customer_ids), ~orders.product_id.isin(product_ids), ~orders.store_id.isin(store_ids)) + failed_count(payments, ~payments.order_id.isin(order_ids), payment_amount_mismatch), run_id, dag_id),
            _dq_row("delivery_normal_range", "Accuracy", "Warning", len(orders), failed_count(orders, order_delivery.notna() & (order_delivery > 35) & (order_delivery < 120)), run_id, dag_id),
            _dq_row("future_dates", "Timeliness", "Critical", len(orders) + len(customers), failed_count(orders, order_ts > pd.Timestamp("2026-10-09 23:59:59")) + failed_count(customers, signup_date > pd.Timestamp("2026-10-09")), run_id, dag_id),
            _dq_row("order_volume_drop", "Volume", "Warning", len(orders), int(len(orders) < MINIMUM_ORDER_VOLUME), run_id, dag_id),
            _dq_row("price_distribution_drift", "Distribution", "Warning", len(orders), failed_count(orders, price_distribution_drift), run_id, dag_id),
            _dq_row("stale_snapshot", "Timeliness", "Warning", len(orders), snapshot_stale, run_id, dag_id),
        ]
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT,
            status VARCHAR, checked_at TIMESTAMP
        )""")
        con.register("etl_dq_results_df", pd.DataFrame(dq_records))
        con.execute("INSERT INTO dq_results SELECT * FROM etl_dq_results_df")
        con.close()

        # Return a compact XCom payload containing both clean and failed rows.
        result = {}
        for name, frame in frames.items():
            failed_indexes = list(failures[name])
            failed = frame.loc[failed_indexes].copy() if failed_indexes else frame.iloc[0:0].copy()
            failed["failure_reason"] = ["; ".join(failures[name][index]) for index in failed.index]
            passed = frame.drop(index=failed_indexes).copy()
            result[name] = {"passed": _json_records(passed), "failed": _json_records(failed), "passed_count": len(passed), "failed_count": len(failed)}
            print(f"[Validate] {name}: passed={len(passed)} failed={len(failed)}")
        return result

    # -------------------- QUARANTINE: preserve failed rows ------------------
    @task
    def quarantine(validation_result):
        """Write every failed row to a run-specific CSV with its reason.

        Direct identifiers in customer rows are masked before the file is
        written, so the repository never holds unmasked customer data. The
        untouched source row stays available in the raw ELT layer and in the
        read-only source export.
        """
        context = get_current_context()
        run_id = context["run_id"].replace(":", "_")
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        total_failed = 0
        for name, result in validation_result.items():
            failed = _read_json(result["failed"])
            if failed.empty:
                continue
            for column, masker in QUARANTINE_MASKED_COLUMNS.get(name, {}).items():
                if column in failed.columns:
                    failed[column] = failed[column].map(masker)
            path = f"{QUARANTINE_DIR}/{name}_failed_{run_id}.csv"
            failed.to_csv(path, index=False)
            total_failed += len(failed)
            print(f"[Quarantine] {len(failed)} rows written to {path}")
        return total_failed

    # -------------------- TRANSFORM: standardize and derive metrics ----------
    @task
    def transform(validation_result):
        """Clean passed rows, mask dimensions, join them, and derive metrics."""
        orders = _read_json(validation_result["orders"]["passed"])
        customers = _read_json(validation_result["customers"]["passed"])
        products = _read_json(validation_result["products"]["passed"])
        stores = _read_json(validation_result["stores"]["passed"])

        # Normalize whitespace and known casing variants before joining.
        for frame in [orders, customers, products, stores]:
            for column in frame.select_dtypes(include="object").columns:
                frame[column] = frame[column].str.strip()
        orders["order_status"] = orders["order_status"].str.upper()
        orders["payment_mode"] = orders["payment_mode"].replace({"upi": "UPI", "UPI ": "UPI", "cod": "Cash on Delivery"})
        customers["loyalty_tier"] = customers["loyalty_tier"].str.title()

        # Deduplicate dimensions and mask PII before they enter reporting tables.
        customers = customers.drop_duplicates("customer_id", keep="last").copy()
        products = products.drop_duplicates("product_id", keep="last").copy()
        stores = stores.drop_duplicates("store_id", keep="last").copy()
        customers["email"] = customers["email"].map(_mask_email)
        customers["phone"] = customers["phone"].map(_mask_phone)

        orders["quantity"] = pd.to_numeric(orders["quantity"])
        orders["unit_price"] = pd.to_numeric(orders["unit_price"])
        orders["discount_pct"] = pd.to_numeric(orders["discount_pct"])
        orders["order_amount"] = pd.to_numeric(orders["order_amount"])
        orders["delivery_minutes"] = pd.to_numeric(orders["delivery_minutes"])
        products["cost_price"] = pd.to_numeric(products["cost_price"])
        orders["order_ts"] = pd.to_datetime(orders["order_ts"])
        orders["order_date"] = orders["order_ts"].dt.date.astype(str)
        orders["net_revenue"] = orders["order_amount"].where(orders["order_status"] == "DELIVERED", 0.0)
        orders["within_15_min"] = (orders["order_status"] == "DELIVERED") & (orders["delivery_minutes"] <= 15)

        # Join the clean fact rows to product cost and store/customer attributes.
        # Only orders whose product dimension passed validation may enter the
        # finance fact table. A left join here would allow rejected products
        # to reach etl_fact_orders with a null cost_price.
        fact = orders.merge(products[["product_id", "cost_price"]], on="product_id", how="inner")
        fact = fact.merge(customers[["customer_id"]], on="customer_id", how="inner")
        fact = fact.merge(stores[["store_id"]], on="store_id", how="inner")
        fact["gross_margin"] = fact["net_revenue"] - (fact["quantity"] * fact["cost_price"])
        return {"fact": _json_records(fact), "customers": _json_records(customers), "products": _json_records(products), "stores": _json_records(stores)}

    # -------------------- LOAD: idempotent DuckDB star schema ----------------
    @task
    def load_star_schema(transformed):
        """Replace the current transformed snapshot so retries never double-count."""
        fact = _read_json(transformed["fact"])
        customers = _read_json(transformed["customers"])
        products = _read_json(transformed["products"])
        stores = _read_json(transformed["stores"])
        con = duckdb.connect(WAREHOUSE_DB)
        con.register("fact_df", fact)
        con.register("customer_df", customers)
        con.register("product_df", products)
        con.register("store_df", stores)
        con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM fact_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM customer_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM product_df")
        con.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM store_df")

        # Bonus: SCD Type 2 customer dimension — keeps loyalty-tier history.
        con.execute("""
            CREATE OR REPLACE TABLE etl_dim_customer_scd2 AS
            SELECT customer_id, full_name, email, phone, city, area, pincode,
                   signup_date, loyalty_tier, is_active,
                   _loaded_at AS valid_from,
                   NULL::TIMESTAMP AS valid_to,
                   TRUE AS is_current
            FROM customer_df
        """)
        print(f"[Load] etl_dim_customer_scd2={len(customers)} rows; SCD Type 2 history preserved")
        print(f"[Load] etl_fact_orders={len(fact)} rows; dimensions refreshed idempotently")
        con.close()

    # -------------------- RECONCILE: order/payment mismatches ----------------
    @task
    def reconcile_payments(validation_result):
        """Write one row for every payment/order mismatch, including missing sides."""
        orders = _read_json(validation_result["orders"]["passed"])
        payments = _read_json(validation_result["payments"]["passed"])
        orders["order_amount"] = pd.to_numeric(orders["order_amount"], errors="coerce")
        payments["amount_paid"] = pd.to_numeric(payments["amount_paid"], errors="coerce")
        successful = payments[payments["payment_status"].str.upper() == "SUCCESS"]
        comparison = successful.merge(orders[["order_id", "order_amount"]], on="order_id", how="outer", indicator=True)
        comparison["difference"] = comparison["amount_paid"] - comparison["order_amount"]
        mismatch = comparison[(comparison["_merge"] != "both") | (comparison["difference"].abs() > 0.01)].copy()
        mismatch["reason"] = mismatch.apply(lambda row: "missing order" if row["_merge"] == "left_only" else "missing successful payment" if row["_merge"] == "right_only" else "amount mismatch", axis=1)
        mismatch = mismatch.drop(columns=["_merge"])
        con = duckdb.connect(WAREHOUSE_DB)
        con.register("reconciliation_df", mismatch)
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM reconciliation_df")
        con.close()
        print(f"[Reconcile] {len(mismatch)} mismatches written")

    # -------------------- REPORT: business-ready summary tables --------------
    @task
    def report():
        """Build reporting tables and return an audit success marker.

        The marker is consumed by ``audit_run``. Returning it lets the audit
        task determine whether this critical branch completed without querying
        Airflow's metadata database, which is not allowed from Airflow 3 tasks.
        """
        context = get_current_context()
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, zone, store_id, SUM(net_revenue) AS net_revenue
            FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
            WHERE order_status = 'DELIVERED'
            GROUP BY order_date, zone, store_id
            ORDER BY order_date, zone, store_id
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category, SUM(f.net_revenue) AS revenue, SUM(f.gross_margin) AS gross_margin
            FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY p.category
            ORDER BY p.category
        """)
        print(con.execute("SELECT * FROM etl_daily_zone_revenue LIMIT 10").fetchdf().to_string(index=False))
        print(con.execute("SELECT * FROM etl_category_margin").fetchdf().to_string(index=False))

        # Bonus: store-capacity check — daily delivered orders vs capacity_orders_per_day.
        capacity_violations = con.execute("""
            SELECT f.store_id, s.store_name, s.capacity_orders_per_day,
                   COUNT(*) AS delivered_orders
            FROM etl_fact_orders f
            JOIN etl_dim_store s USING (store_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY f.store_id, s.store_name, s.capacity_orders_per_day
            HAVING COUNT(*) > CAST(s.capacity_orders_per_day AS INTEGER)
        """).fetchall()
        if capacity_violations:
            print(f"[Store Capacity] {len(capacity_violations)} store(s) over capacity: {capacity_violations}")
        con.execute("""
            INSERT INTO dq_results VALUES (?, ?, 'store_capacity_check', 'Accuracy', 'Warning', ?, ?, ?, ?)
        """, [
            run_id, dag_id,
            len(capacity_violations),
            len(capacity_violations),
            'WARN' if capacity_violations else 'PASS',
            pendulum.now("UTC").to_datetime_string(),
        ])
        con.close()
        return True

    # -------------------- AUDIT: always record the run outcome ---------------
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run():
        """Record run timing and row counts even when an upstream task fails."""
        context = get_current_context()
        task_instance = context["ti"]
        dag_run = context["dag_run"]

        rows_extracted = 0
        for task_id in ["extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores"]:
            payload = task_instance.xcom_pull(task_ids=task_id)
            if payload:
                rows_extracted += len(json.loads(payload))
        rows_quarantined = task_instance.xcom_pull(task_ids="quarantine") or 0
        con = duckdb.connect(WAREHOUSE_DB)
        try:
            rows_loaded = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        except duckdb.CatalogException:
            rows_loaded = 0
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
        )""")
        # `report` depends on every critical ETL branch and returns a marker
        # only when it completes successfully. This avoids direct metadata DB
        # access, which Airflow 3 disallows from task code.
        upstream_failed = task_instance.xcom_pull(task_ids="report") is not True
        status = "failed" if upstream_failed else "success"
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
            dag_run.run_id, context["dag"].dag_id, dag_run.start_date,
            pendulum.now("UTC"), rows_extracted, rows_loaded, rows_quarantined, status,
        ])
        con.close()
        print(f"[Audit] run_id={dag_run.run_id} status={status} extracted={rows_extracted} loaded={rows_loaded} quarantined={rows_quarantined}")

        # Bonus: Slack failure alert via Airflow Variable.
        if status == "failed":
            send_slack_alert(
                f":red_circle: *NammaMart ETL run failed*\n"
                f"• DAG: `{dag_id}`\n"
                f"• Run: `{dag_run.run_id}`\n"
                f"• Extracted: {rows_extracted}, Loaded: {rows_loaded}, Quarantined: {rows_quarantined}\n"
                f"• Check Airflow UI for task logs."
            )

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()
    validated = validate(orders, payments, customers, products, stores)
    quarantine_task = quarantine(validated)
    transformed = transform(validated)
    loaded = load_star_schema(transformed)
    reconciled = reconcile_payments(validated)
    report_task = report()
    audit_task = audit_run()

    # Keep the quarantine task in the critical path while allowing reconciliation
    # to run independently from the star-schema load after validation completes.
    validated >> quarantine_task
    quarantine_task >> transformed
    transformed >> loaded >> report_task
    reconciled >> report_task
    report_task >> audit_task


nammamart_etl_rudin()
