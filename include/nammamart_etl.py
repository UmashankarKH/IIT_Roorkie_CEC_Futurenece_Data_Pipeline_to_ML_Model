"""Helpers for the NammaMart ETL assignment DAG."""

from __future__ import annotations

import re
import shutil
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pendulum


INCLUDE_DIR = Path("/usr/local/airflow/include")
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / "sol"
# Intermediate pass/fail sets can contain source PII, so keep them outside the
# checked-out repository tree and remove them after a successful reporting run.
WORK_DIR = Path("/tmp/nammamart_etl_sol")
WAREHOUSE_PATH = INCLUDE_DIR / "warehouse_sol.duckdb"

SCHEMAS = {
    "orders.csv": [
        "order_id", "order_ts", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount",
        "payment_mode", "order_status", "channel", "delivery_minutes",
    ],
    "payments.csv": [
        "payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
        "payment_status", "gateway_ref",
    ],
    "customers.csv": [
        "customer_id", "full_name", "email", "phone", "city", "area",
        "pincode", "signup_date", "loyalty_tier", "is_active",
    ],
    "products.csv": [
        "product_id", "product_name", "category", "sub_category", "brand",
        "mrp", "cost_price", "unit", "is_perishable", "launch_date",
    ],
    "stores.csv": [
        "store_id", "store_name", "zone", "area", "pincode", "open_date",
        "manager_name", "capacity_orders_per_day",
    ],
}

KEYS = {
    "orders.csv": "order_id",
    "payments.csv": "payment_id",
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "stores.csv": "store_id",
}


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("_.-") or "manual_run"


def extract_file(filename: str) -> dict[str, Any]:
    """Read and schema-check a source file; return only a small XCom manifest."""
    path = DATA_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"Required source file is missing: {path}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    expected = SCHEMAS[filename]
    if list(frame.columns) != expected:
        raise ValueError(
            f"Schema mismatch for {filename}. Expected {expected}; got {list(frame.columns)}"
        )
    print(f"[Extract] {filename}: {len(frame)} source rows")
    return {"file": filename, "source_path": str(path), "rows": len(frame)}


def _read_frame(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _normalise(frame: pd.DataFrame, filename: str) -> pd.DataFrame:
    result = frame.copy()
    for column in result.columns:
        result[column] = result[column].map(lambda value: value.strip() if isinstance(value, str) else value)
    if "payment_mode" in result:
        modes = {
            "upi": "UPI",
            "card": "Card",
            "cash on delivery": "Cash on Delivery",
            "cod": "Cash on Delivery",
            "wallet": "Wallet",
        }
        result["payment_mode"] = result["payment_mode"].map(
            lambda value: modes.get(value.casefold(), value) if value else value
        )
    if "order_status" in result:
        result["order_status"] = result["order_status"].str.upper()
    if "payment_status" in result:
        result["payment_status"] = result["payment_status"].str.upper()
    if "channel" in result:
        result["channel"] = result["channel"].str.title()
    if "category" in result:
        result["category"] = result["category"].str.title()
    if "loyalty_tier" in result:
        result["loyalty_tier"] = result["loyalty_tier"].str.title()
    if "zone" in result:
        result["zone"] = result["zone"].str.title()
    if "is_active" in result:
        result["is_active"] = result["is_active"].str.lower().map(
            {"y": "true", "n": "false", "true": "true", "false": "false"}
        ).fillna("")
    if "is_perishable" in result:
        result["is_perishable"] = result["is_perishable"].str.lower()
    return result


def _decimal(value: Any) -> Decimal | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _mask_email(value: str) -> str:
    if not value:
        return ""
    if "@" not in value:
        return "***"
    local, domain = value.split("@", 1)
    visible = local[:2] if len(local) > 2 else local[:1]
    return f"{visible}***@{domain}"


def _mask_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) != 10:
        return "***MASKED***" if digits else ""
    return f"{digits[:2]}******{digits[-2:]}"


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def validate_file(
    manifest: dict[str, Any],
    run_id: str,
    master_manifests: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Apply row-level checks and save pass/fail sets outside XCom."""
    filename = manifest["file"]
    frame = _normalise(_read_frame(manifest["source_path"]), filename)
    key = KEYS[filename]
    reasons: list[list[str]] = [[] for _ in range(len(frame))]
    warnings: list[list[str]] = [[] for _ in range(len(frame))]

    def fail(mask: pd.Series, message: str) -> None:
        for index in frame.index[mask.fillna(False)]:
            reasons[index].append(message)

    def warn(mask: pd.Series, message: str) -> None:
        for index in frame.index[mask.fillna(False)]:
            warnings[index].append(message)

    # Every file must have its primary key. Repeated fact/payment keys are all
    # rejected; dimension duplicates keep the first source row deterministically.
    fail(frame[key].eq(""), f"{key} is missing")
    duplicate = frame[key].ne("") & frame[key].duplicated(keep=False)
    if filename in {"customers.csv", "products.csv"}:
        duplicate = frame[key].ne("") & frame[key].duplicated(keep="first")
    fail(duplicate, f"duplicate {key}; a canonical dimension row is retained where applicable")

    if filename == "orders.csv":
        required = [
            "customer_id", "store_id", "product_id", "order_ts", "quantity",
            "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
        ]
        for column in required:
            fail(frame[column].eq(""), f"{column} is required")

        fail(~frame["order_status"].isin(["DELIVERED", "CANCELLED", "RETURNED"]), "invalid order_status")
        fail(~frame["payment_mode"].isin(["UPI", "Card", "Cash on Delivery", "Wallet"]), "invalid payment_mode")
        if "test" in frame.columns:
            fail(frame["channel"].str.casefold().str.contains("test", na=False), "test order/channel")
        fail(frame["order_id"].str.casefold().str.contains("test", na=False), "test order identifier")

        quantity = frame["quantity"].map(_decimal)
        unit_price = frame["unit_price"].map(_decimal)
        discount = frame["discount_pct"].map(_decimal)
        amount = frame["order_amount"].map(_decimal)
        fail(quantity.map(lambda value: value is None or value <= 0), "quantity must be numeric and greater than zero")
        fail(discount.map(lambda value: value is None or value < 0 or value > 100), "discount_pct must be between 0 and 100")
        bad_formula = pd.Series(False, index=frame.index)
        for index in frame.index:
            q, price, pct, actual = quantity[index], unit_price[index], discount[index], amount[index]
            if price is None or actual is None:
                bad_formula[index] = True
            elif q is not None and pct is not None:
                expected = (q * price * (Decimal("1") - pct / Decimal("100"))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                bad_formula[index] = actual.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) != expected
        fail(bad_formula, "order_amount does not match the discounted quantity x unit_price formula")

        parsed = pd.to_datetime(frame["order_ts"], errors="coerce", format="mixed")
        fail(parsed.isna(), "order_ts is missing or invalid")
        today = pendulum.now("Asia/Kolkata").date()
        future = parsed.map(lambda value: False if pd.isna(value) else value.date() > today)
        fail(future, f"order_ts is after the current business date {today}")

        delivery = frame["delivery_minutes"].map(_decimal)
        delivered = frame["order_status"].eq("DELIVERED")
        fail(delivered & frame["delivery_minutes"].eq(""), "DELIVERED order requires delivery_minutes")
        fail(delivery.map(lambda value: value is not None and (value < 0 or value > 120)), "delivery_minutes is outside the plausible 0..120 minute range")
        warn(delivery.map(lambda value: value is not None and (value < 8 or value > 35) and 0 <= value <= 120), "delivery_minutes is outside the normal 8..35 minute range")

        if master_manifests:
            for column, master_file in [
                ("customer_id", "customers.csv"),
                ("product_id", "products.csv"),
                ("store_id", "stores.csv"),
            ]:
                master = _read_frame(master_manifests[master_file]["passed_path"])
                valid_ids = set(master[KEYS[master_file]])
                fail(frame[column].ne("") & ~frame[column].isin(valid_ids), f"{column} is not present in the validated {master_file} dimension")

    elif filename == "payments.csv":
        for column in ["order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status"]:
            fail(frame[column].eq(""), f"{column} is required")
        fail(~frame["payment_mode"].isin(["UPI", "Card", "Cash on Delivery", "Wallet"]), "invalid payment_mode")
        fail(~frame["payment_status"].isin(["SUCCESS", "FAILED", "REFUNDED"]), "invalid payment_status")
        paid = frame["amount_paid"].map(_decimal)
        fail(paid.map(lambda value: value is None or value < 0), "amount_paid must be a non-negative number")
        parsed = pd.to_datetime(frame["payment_ts"], errors="coerce", format="mixed")
        fail(parsed.isna(), "payment_ts is missing or invalid")
        if master_manifests and "orders.csv" in master_manifests:
            clean_orders = _read_frame(master_manifests["orders.csv"]["passed_path"])
            order_by_id = clean_orders.drop_duplicates("order_id").set_index("order_id")
            fail(frame["order_id"].ne("") & ~frame["order_id"].isin(order_by_id.index), "order_id is not present in the validated orders")
            amount_mismatch = pd.Series(False, index=frame.index)
            mode_mismatch = pd.Series(False, index=frame.index)
            for index, payment in frame.iterrows():
                order_id = payment["order_id"]
                if order_id not in order_by_id.index:
                    continue
                order = order_by_id.loc[order_id]
                payment_amount = paid[index]
                order_amount = _decimal(order["order_amount"])
                if payment["payment_status"] == "SUCCESS" and (
                    payment_amount is None
                    or order_amount is None
                    or payment_amount.quantize(Decimal("0.01")) != order_amount.quantize(Decimal("0.01"))
                ):
                    amount_mismatch[index] = True
                if payment["payment_mode"] != order["payment_mode"]:
                    mode_mismatch[index] = True
            fail(amount_mismatch, "SUCCESS payment amount does not equal the validated order_amount")
            fail(mode_mismatch, "payment_mode does not match the validated order")

    elif filename == "customers.csv":
        fail(frame["loyalty_tier"].eq(""), "loyalty_tier is required")
        fail(~frame["loyalty_tier"].isin(["Bronze", "Silver", "Gold", "Platinum"]), "invalid loyalty_tier")
        fail(~frame["is_active"].isin(["true", "false"]), "is_active must be Y/N or true/false")
        parsed = pd.to_datetime(frame["signup_date"], errors="coerce", format="mixed")
        fail(parsed.isna(), "signup_date is missing or invalid")
        today = pendulum.now("Asia/Kolkata").date()
        fail(parsed.map(lambda value: False if pd.isna(value) else value.date() > today), f"signup_date is after the current business date {today}")
        email_bad = frame["email"].ne("") & ~frame["email"].str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        warn(email_bad, "email does not match a basic email address format")
        phone_digits = frame["phone"].str.replace(r"\D", "", regex=True)
        warn(frame["phone"].ne("") & phone_digits.str.len().ne(10), "phone does not contain exactly 10 digits")
        pin = frame["pincode"]
        warn(pin.ne("") & ~pin.str.fullmatch(r"\d{6}"), "pincode is not six digits")

    elif filename == "products.csv":
        fail(frame["category"].eq(""), "category is required")
        launch_date = pd.to_datetime(frame["launch_date"], errors="coerce", format="mixed")
        fail(launch_date.isna(), "launch_date is missing or invalid")
        fail(~frame["is_perishable"].str.lower().isin(["true", "false"]), "is_perishable must be true or false")
        mrp = frame["mrp"].map(_decimal)
        cost = frame["cost_price"].map(_decimal)
        fail(mrp.map(lambda value: value is None or value <= 0), "mrp must be numeric and greater than zero")
        fail(cost.map(lambda value: value is None or value < 0), "cost_price must be numeric and non-negative")
        fail(pd.Series([m is not None and c is not None and c > m for m, c in zip(mrp, cost)], index=frame.index), "cost_price exceeds mrp")

    elif filename == "stores.csv":
        fail(frame["zone"].eq(""), "zone is required")
        fail(~frame["zone"].isin(["North", "South", "East", "West"]), "zone is not one of North, South, East, West")
        open_date = pd.to_datetime(frame["open_date"], errors="coerce", format="mixed")
        fail(open_date.isna(), "open_date is missing or invalid")
        pin = frame["pincode"]
        warn(pin.ne("") & ~pin.str.fullmatch(r"\d{6}"), "pincode is not six digits")

    passed_mask = pd.Series([not row_reasons for row_reasons in reasons], index=frame.index)
    passed = frame.loc[passed_mask].copy()
    failed = frame.loc[~passed_mask].copy()
    if len(failed):
        failed["failure_reason"] = ["; ".join(reasons[index]) for index in failed.index]

    run_part = safe_run_id(run_id)
    work_dir = WORK_DIR / run_part
    work_dir.mkdir(parents=True, exist_ok=True)
    passed_path = work_dir / f"{filename}.passed.csv"
    failed_path = work_dir / f"{filename}.failed.csv"
    _write_csv(passed, passed_path)
    _write_csv(failed, failed_path)
    warning_count = sum(bool(messages) for messages in warnings)
    print(
        f"[Validate] {filename}: passed={len(passed)}, failed={len(failed)}, "
        f"warning_rows={warning_count}"
    )
    return {
        "file": filename,
        "run_id": run_part,
        "passed_path": str(passed_path),
        "failed_path": str(failed_path),
        "passed_count": len(passed),
        "failed_count": len(failed),
        "warning_count": warning_count,
    }


def quarantine_files(results: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    """Publish failed source rows with a failure_reason column."""
    run_part = safe_run_id(run_id)
    target_dir = QUARANTINE_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    entries = []
    for result in results:
        failed = _read_frame(result["failed_path"])
        if result["file"] == "customers.csv":
            if "full_name" in failed:
                failed["full_name"] = failed["full_name"].map(lambda value: "***" if value else "")
            if "email" in failed:
                failed["email"] = failed["email"].map(_mask_email)
            if "phone" in failed:
                failed["phone"] = failed["phone"].map(_mask_phone)
        target = target_dir / f"{result['file']}_failed_{run_part}.csv"
        if "failure_reason" not in failed:
            failed["failure_reason"] = pd.Series(dtype=str)
        failed.to_csv(target, index=False)
        count = len(failed)
        total += count
        entries.append({"file": result["file"], "path": str(target), "rows": count})
        print(f"[Quarantine] {result['file']}: {count} rows -> {target}")
    return {"run_id": run_part, "total_rows": total, "files": entries}


def transform_files(
    order_result: dict[str, Any],
    customer_result: dict[str, Any],
    product_result: dict[str, Any],
    store_result: dict[str, Any],
    run_id: str,
) -> dict[str, Any]:
    """Build masked dimensions and a validated, joined order fact extract."""
    run_part = safe_run_id(run_id)
    work_dir = WORK_DIR / run_part
    customers = _read_frame(customer_result["passed_path"])
    products = _read_frame(product_result["passed_path"])
    stores = _read_frame(store_result["passed_path"])
    orders = _read_frame(order_result["passed_path"])

    # Defensive dimension deduplication: validators already retain one canonical
    # record, and this guarantees one dimension row per business key at load time.
    customers = customers.drop_duplicates("customer_id", keep="first")
    products = products.drop_duplicates("product_id", keep="first")
    stores = stores.drop_duplicates("store_id", keep="first")

    customer_dim = pd.DataFrame({
        "customer_id": customers["customer_id"],
        "masked_email": customers["email"].map(_mask_email),
        "masked_phone": customers["phone"].map(_mask_phone),
        "city": customers["city"],
        "area": customers["area"],
        "pincode": customers["pincode"],
        "signup_date": pd.to_datetime(customers["signup_date"], errors="coerce").dt.date,
        "loyalty_tier": customers["loyalty_tier"],
        "is_active": customers["is_active"].str.lower().map({"true": True, "false": False}),
    })
    product_dim = pd.DataFrame({
        "product_id": products["product_id"],
        "product_name": products["product_name"],
        "category": products["category"],
        "sub_category": products["sub_category"],
        "brand": products["brand"],
        "mrp": pd.to_numeric(products["mrp"], errors="coerce"),
        "cost_price": pd.to_numeric(products["cost_price"], errors="coerce"),
        "unit": products["unit"],
        "is_perishable": products["is_perishable"].str.lower().map({"true": True, "false": False}),
        "launch_date": pd.to_datetime(products["launch_date"], errors="coerce").dt.date,
    })
    store_dim = pd.DataFrame({
        "store_id": stores["store_id"],
        "store_name": stores["store_name"],
        "zone": stores["zone"],
        "area": stores["area"],
        "pincode": stores["pincode"],
        "open_date": pd.to_datetime(stores["open_date"], errors="coerce").dt.date,
        "capacity_orders_per_day": pd.to_numeric(stores["capacity_orders_per_day"], errors="coerce").astype("Int64"),
    })

    # Perform the required dimensional joins before selecting the fact grain.
    joined = orders.merge(customer_dim[["customer_id", "loyalty_tier"]], on="customer_id", how="inner")
    joined = joined.merge(product_dim[["product_id", "category", "cost_price"]], on="product_id", how="inner")
    joined = joined.merge(store_dim[["store_id", "zone"]], on="store_id", how="inner")
    joined["order_ts"] = pd.to_datetime(joined["order_ts"], errors="coerce", format="mixed")
    joined["order_date"] = joined["order_ts"].dt.date
    joined["quantity"] = pd.to_numeric(joined["quantity"], errors="coerce")
    joined["unit_price"] = pd.to_numeric(joined["unit_price"], errors="coerce")
    joined["discount_pct"] = pd.to_numeric(joined["discount_pct"], errors="coerce")
    joined["order_amount"] = pd.to_numeric(joined["order_amount"], errors="coerce").round(2)
    joined["delivery_minutes"] = pd.to_numeric(joined["delivery_minutes"], errors="coerce")
    delivered = joined["order_status"].eq("DELIVERED")
    joined["net_revenue"] = joined["order_amount"].where(delivered, 0.0).round(2)
    joined["gross_margin"] = (
        joined["net_revenue"] - joined["quantity"] * joined["cost_price"]
    ).where(delivered, 0.0).round(2)
    joined["within_15_min"] = delivered & joined["delivery_minutes"].le(15)

    fact_columns = [
        "order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
        "order_status", "channel", "delivery_minutes", "net_revenue", "gross_margin",
        "within_15_min",
    ]
    fact = joined[fact_columns].copy()
    paths = {}
    for name, frame in [
        ("etl_fact_orders.csv", fact),
        ("etl_dim_customer.csv", customer_dim),
        ("etl_dim_product.csv", product_dim),
        ("etl_dim_store.csv", store_dim),
    ]:
        path = work_dir / name
        frame.to_csv(path, index=False)
        paths[name] = str(path)
    print(f"[Transform] Joined {len(fact)} clean order rows; masked customer email and phone in dimension")
    return {"run_id": run_part, "paths": paths, "fact_rows": len(fact)}


def load_star_schema(transformed: dict[str, Any]) -> dict[str, Any]:
    """Replace this DAG's output tables from the validated full source snapshot."""
    paths = transformed["paths"]
    fact = pd.read_csv(paths["etl_fact_orders.csv"], parse_dates=["order_ts"])
    customer = pd.read_csv(paths["etl_dim_customer.csv"], parse_dates=["signup_date"])
    product = pd.read_csv(paths["etl_dim_product.csv"], parse_dates=["launch_date"])
    store = pd.read_csv(paths["etl_dim_store.csv"], parse_dates=["open_date"])
    for frame, column in [(fact, "order_date"), (customer, "signup_date"), (product, "launch_date"), (store, "open_date")]:
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
    for column in ["is_active"]:
        if column in customer:
            customer[column] = customer[column].astype("boolean")
    if "is_perishable" in product:
        product["is_perishable"] = product["is_perishable"].astype("boolean")
    if "within_15_min" in fact:
        fact["within_15_min"] = fact["within_15_min"].astype("boolean")

    connection = duckdb.connect(str(WAREHOUSE_PATH))
    try:
        connection.execute("BEGIN TRANSACTION")
        for table, frame in [
            ("etl_fact_orders", fact),
            ("etl_dim_customer", customer),
            ("etl_dim_product", product),
            ("etl_dim_store", store),
        ]:
            connection.register("incoming_frame", frame)
            connection.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM incoming_frame")
            connection.unregister("incoming_frame")
        connection.execute("COMMIT")
        counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ["etl_fact_orders", "etl_dim_customer", "etl_dim_product", "etl_dim_store"]
        }
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    print(f"[Load] Idempotent DuckDB snapshot replaced at {WAREHOUSE_PATH}: {counts}")
    return {"warehouse_path": str(WAREHOUSE_PATH), "counts": counts, "run_id": transformed["run_id"]}


def reconcile_payments(load_result: dict[str, Any], payment_manifest: dict[str, Any]) -> dict[str, Any]:
    """Write one reconciliation row per clean-order/payment mismatch."""
    connection = duckdb.connect(str(WAREHOUSE_PATH))
    try:
        clean_orders = connection.execute(
            "SELECT order_id, order_amount, payment_mode, order_status FROM etl_fact_orders"
        ).fetchdf()
    finally:
        connection.close()
    source_payments = _normalise(_read_frame(payment_manifest["source_path"]), "payments.csv")
    clean_order_ids = set(clean_orders["order_id"].astype(str))
    by_order: dict[str, pd.DataFrame] = {
        str(order_id): group for order_id, group in source_payments.groupby("order_id", dropna=False)
    }
    reconciliation: list[dict[str, Any]] = []

    def add(order_id: str, payment_id: str, order_amount: Any, paid_amount: Any, reason: str) -> None:
        reconciliation.append({
            "order_id": order_id,
            "payment_id": payment_id,
            "order_amount": order_amount,
            "amount_paid": paid_amount,
            "reason": reason,
        })

    for _, order in clean_orders.iterrows():
        order_id = str(order["order_id"])
        status = str(order["order_status"])
        payments = by_order.get(order_id)
        if payments is None or payments.empty:
            if status != "CANCELLED":
                add(order_id, "", order["order_amount"], None, "no payment row for non-cancelled order")
            continue
        if status == "CANCELLED":
            for _, payment in payments.iterrows():
                add(order_id, str(payment.get("payment_id", "")), order["order_amount"], payment.get("amount_paid"), "cancelled order has a payment row")
            continue
        for _, payment in payments.iterrows():
            reasons = []
            if len(payments) > 1:
                reasons.append("multiple payment rows for order")
            if str(payment.get("payment_status", "")).upper() != "SUCCESS":
                reasons.append(f"payment status is {payment.get('payment_status', '')}")
            paid = _decimal(payment.get("amount_paid"))
            expected = _decimal(order["order_amount"])
            if paid is None or expected is None or paid.quantize(Decimal("0.01")) != expected.quantize(Decimal("0.01")):
                reasons.append("payment amount does not equal order_amount")
            if str(payment.get("payment_mode", "")) != str(order["payment_mode"]):
                reasons.append("payment_mode does not match order")
            if reasons:
                add(order_id, str(payment.get("payment_id", "")), order["order_amount"], payment.get("amount_paid"), "; ".join(reasons))

    for _, payment in source_payments.iterrows():
        order_id = str(payment.get("order_id", ""))
        if order_id not in clean_order_ids:
            add(order_id, str(payment.get("payment_id", "")), None, payment.get("amount_paid"), "payment has no validated clean order")

    columns = ["order_id", "payment_id", "order_amount", "amount_paid", "reason"]
    result = pd.DataFrame(reconciliation, columns=columns)
    connection = duckdb.connect(str(WAREHOUSE_PATH))
    try:
        connection.register("reconciliation_frame", result)
        connection.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM reconciliation_frame")
        connection.unregister("reconciliation_frame")
    finally:
        connection.close()
    print(f"[Reconcile] {len(result)} order/payment mismatch rows written")
    return {"mismatch_rows": len(result), "run_id": load_result["run_id"]}


def build_reports(reconcile_result: dict[str, Any], validation_results: list[dict[str, Any]]) -> dict[str, Any]:
    connection = duckdb.connect(str(WAREHOUSE_PATH))
    try:
        connection.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT f.order_date, s.zone, s.store_id, s.store_name,
                   SUM(f.net_revenue) AS net_revenue
            FROM etl_fact_orders f
            JOIN etl_dim_store s USING (store_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY f.order_date, s.zone, s.store_id, s.store_name
            ORDER BY f.order_date, s.zone, s.store_id
        """)
        connection.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category,
                   SUM(f.net_revenue) AS revenue,
                   SUM(f.gross_margin) AS gross_margin
            FROM etl_fact_orders f
            JOIN etl_dim_product p USING (product_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY p.category
            ORDER BY p.category
        """)
        revenue = connection.execute("SELECT * FROM etl_daily_zone_revenue").fetchdf()
        margin = connection.execute("SELECT * FROM etl_category_margin").fetchdf()
        counts = {
            "rows_extracted": sum(int(result["passed_count"] + result["failed_count"]) for result in validation_results),
            "rows_loaded": connection.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0],
            "rows_quarantined": sum(int(result["failed_count"]) for result in validation_results),
        }
        connection.close()
    except Exception:
        connection.close()
        raise
    print("[Report] etl_daily_zone_revenue")
    print(revenue.to_string(index=False))
    print("[Report] etl_category_margin")
    print(margin.to_string(index=False))
    work_dir = WORK_DIR / safe_run_id(reconcile_result["run_id"])
    shutil.rmtree(work_dir, ignore_errors=True)
    print(f"[Report] run counts: {counts}")
    return {"counts": counts, "mismatches": reconcile_result["mismatch_rows"]}
