"""Shared paths, data-quality rules, masking and audit helpers for NammaMart."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


INCLUDE_DIR = Path(__file__).resolve().parent
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
WAREHOUSE_DB = INCLUDE_DIR / "warehouse_nammamart.duckdb"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / "Tusshar_Tikoo"
SQL_DIR = INCLUDE_DIR / "sql" / "nammamart"

ALLOWED_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
ALLOWED_PAYMENT_STATUSES = {"SUCCESS", "FAILED", "REFUNDED"}
ALLOWED_ZONES = {"North", "South", "East", "West"}

EXPECTED_COLUMNS = {
    "orders": {
        "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
        "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
        "channel", "delivery_minutes",
    },
    "orders_increment": {
        "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
        "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
        "channel", "delivery_minutes",
    },
    "payments": {
        "payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
        "payment_status", "gateway_ref",
    },
    "customers": {
        "customer_id", "full_name", "email", "phone", "city", "area", "pincode",
        "signup_date", "loyalty_tier", "is_active",
    },
    "products": {
        "product_id", "product_name", "category", "sub_category", "brand", "mrp",
        "cost_price", "unit", "is_perishable", "launch_date",
    },
    "stores": {
        "store_id", "store_name", "zone", "area", "pincode", "open_date",
        "manager_name", "capacity_orders_per_day",
    },
}


def safe_run_id(run_id: str) -> str:
    """Make an Airflow run ID safe to include in a local filename."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("._") or "manual"


def read_source(path: str | Path) -> pd.DataFrame:
    """Read CSV values as strings so validation sees the source values."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def mask_email(value: Any) -> str:
    value = str(value or "").strip()
    if "@" not in value:
        return "***"
    local, domain = value.rsplit("@", 1)
    if not local or not domain:
        return "***"
    return f"{local[:2]}***@{domain}"


def mask_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) != 10:
        return "***"
    return f"{digits[:2]}******{digits[-2:]}"


def standardize_payment_mode(value: Any) -> str:
    raw = str(value or "").strip()
    lookup = raw.casefold()
    return {
        "upi": "UPI",
        "card": "Card",
        "cash on delivery": "Cash on Delivery",
        "cod": "Cash on Delivery",
        "wallet": "Wallet",
    }.get(lookup, raw)


def ensure_audit_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR,
            dag_id VARCHAR,
            check_name VARCHAR,
            dimension VARCHAR,
            severity VARCHAR,
            rows_checked BIGINT,
            rows_failed BIGINT,
            status VARCHAR,
            checked_at TIMESTAMPTZ
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR,
            dag_id VARCHAR,
            start_time TIMESTAMPTZ,
            end_time TIMESTAMPTZ,
            rows_extracted BIGINT,
            rows_loaded BIGINT,
            rows_quarantined BIGINT,
            status VARCHAR
        )
        """
    )


def append_dq_results(con, run_id: str, dag_id: str, checks: list[dict[str, Any]]) -> None:
    ensure_audit_tables(con)
    con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    checked_at = datetime.now(timezone.utc)
    values = []
    for check in checks:
        failed = int(check["rows_failed"])
        severity = check["severity"]
        status = "PASS" if failed == 0 else ("WARNING" if severity == "Warning" else "FAIL")
        values.append(
            (
                run_id,
                dag_id,
                check["check_name"],
                check["dimension"],
                severity,
                int(check["rows_checked"]),
                failed,
                status,
                checked_at,
            )
        )
    if values:
        con.executemany(
            "INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", values
        )


def _trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in out.columns:
        out[column] = out[column].map(lambda value: value.strip() if isinstance(value, str) else value)
    return out


def _mask_values(df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out:
            if column == "email":
                out[column] = out[column].map(mask_email)
            elif column == "phone":
                out[column] = out[column].map(mask_phone)
    if "full_name" in out:
        out = out.drop(columns=["full_name"])
    return out


def validate_frame(
    name: str,
    source_file: str,
    frame: pd.DataFrame,
    *,
    customers: pd.DataFrame | None = None,
    products: pd.DataFrame | None = None,
    stores: pd.DataFrame | None = None,
    orders: pd.DataFrame | None = None,
    payments: pd.DataFrame | None = None,
    valid_payments: pd.DataFrame | None = None,
    checked_at: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    """Apply the ETL rules and return passed rows, failed rows and check metrics."""
    expected_columns = EXPECTED_COLUMNS[name]
    actual_columns = set(frame.columns) - {"_source_file", "_source_row_number"}
    missing_columns = sorted(expected_columns - actual_columns)
    unexpected_columns = sorted(actual_columns - expected_columns)
    if missing_columns or unexpected_columns:
        raise ValueError(
            f"Schema mismatch for {source_file}: "
            f"missing columns={missing_columns}; unexpected columns={unexpected_columns}"
        )
    df = _trim_strings(frame)
    if "order_status" in df:
        df["order_status"] = df["order_status"].str.upper()
    if "payment_status" in df:
        df["payment_status"] = df["payment_status"].str.upper()
    if "payment_mode" in df:
        df["payment_mode"] = df["payment_mode"].map(standardize_payment_mode)
    if "zone" in df:
        df["zone"] = df["zone"].str.title()
    if "loyalty_tier" in df:
        df["loyalty_tier"] = df["loyalty_tier"].str.title()

    now = checked_at or datetime.now(timezone.utc)
    critical_fail = pd.Series(False, index=df.index)
    reasons: dict[Any, list[str]] = {index: [] for index in df.index}
    warnings: dict[Any, list[str]] = {index: [] for index in df.index}
    checks: list[dict[str, Any]] = []

    def add_check(
        rule: str,
        dimension: str,
        severity: str,
        mask: pd.Series,
        *,
        message: str | None = None,
        rows_checked: int | None = None,
    ) -> None:
        nonlocal critical_fail
        mask = mask.reindex(df.index, fill_value=False).fillna(False).astype(bool)
        checks.append(
            {
                "check_name": f"{name}_{rule}",
                "dimension": dimension,
                "severity": severity,
                "rows_checked": len(df) if rows_checked is None else rows_checked,
                "rows_failed": int(mask.sum()),
            }
        )
        if message:
            target = reasons if severity == "Critical" else warnings
            for index in df.index[mask]:
                target[index].append(message)
        if severity == "Critical":
            critical_fail |= mask

    def col(name_: str) -> pd.Series:
        if name_ not in df:
            return pd.Series("", index=df.index)
        return df[name_].astype(str).str.strip()

    def numeric(name_: str) -> pd.Series:
        return pd.to_numeric(col(name_), errors="coerce")

    def parsed_date(name_: str) -> pd.Series:
        return pd.to_datetime(col(name_), errors="coerce", utc=True)

    def required(column: str, rule: str | None = None) -> None:
        add_check(
            rule or f"{column}_required",
            "Completeness",
            "Critical",
            col(column).eq(""),
            message=f"{column} is required",
        )

    key_column = {
        "orders": "order_id",
        "orders_increment": "order_id",
        "payments": "payment_id",
        "customers": "customer_id",
        "products": "product_id",
        "stores": "store_id",
    }[name]
    required(key_column)

    duplicate_mask = col(key_column).ne("") & col(key_column).duplicated(keep=False)
    # Master and payment tables retain their first occurrence; every repeated
    # order is ambiguous and is rejected as a group.
    if name in {"customers", "products", "stores", "payments"}:
        duplicate_mask = col(key_column).ne("") & col(key_column).duplicated(keep="first")
    add_check(
        f"{key_column}_unique",
        "Uniqueness",
        "Critical",
        duplicate_mask,
        message=f"duplicate {key_column}",
    )

    if name in {"orders", "orders_increment"}:
        required("customer_id")
        required("product_id")
        required("store_id")
        order_date = parsed_date("order_ts")
        add_check("order_ts_parse", "Validity", "Critical", order_date.isna(), message="order_ts is missing or invalid")
        add_check("no_future_order_ts", "Timeliness", "Critical", order_date.notna() & (order_date > pd.Timestamp(now)), message="order_ts is in the future")
        status = col("order_status")
        add_check("order_status_allowed", "Validity", "Critical", ~status.isin(ALLOWED_STATUSES), message="order_status is not allowed")
        mode = col("payment_mode")
        add_check("payment_mode_allowed", "Validity", "Critical", ~mode.isin(ALLOWED_PAYMENT_MODES), message="payment_mode is not allowed")
        quantity, unit_price = numeric("quantity"), numeric("unit_price")
        discount, amount = numeric("discount_pct"), numeric("order_amount")
        add_check("quantity_positive", "Accuracy", "Critical", quantity.isna() | (quantity <= 0), message="quantity must be greater than zero")
        add_check("discount_range", "Accuracy", "Critical", discount.isna() | (discount < 0) | (discount > 100), message="discount_pct must be between 0 and 100")
        expected = (quantity * unit_price * (1 - discount / 100)).round(2)
        add_check("order_amount_formula", "Accuracy", "Critical", amount.isna() | expected.isna() | ((expected - amount).abs() > 0.01), message="order_amount does not match the quantity, price and discount formula")
        delivery = numeric("delivery_minutes")
        delivered = status.eq("DELIVERED")
        add_check("delivered_has_delivery_time", "Completeness", "Critical", delivered & delivery.isna(), message="DELIVERED order needs delivery_minutes")
        add_check("delivery_under_120_minutes", "Accuracy", "Critical", delivery.notna() & (delivery >= 120), message="delivery_minutes must be below 120")

        references = (
            ("customer_id", customers, "customer_id"),
            ("product_id", products, "product_id"),
            ("store_id", stores, "store_id"),
        )
        for order_column, master, master_key in references:
            ids = set() if master is None else set(master[master_key].astype(str).str.strip())
            values = col(order_column)
            add_check(
                f"{order_column}_exists",
                "Consistency",
                "Critical",
                values.ne("") & ~values.isin(ids),
                message=f"{order_column} does not exist in its master file",
            )

        if payments is not None:
            payment_frame = _trim_strings(payments)
            successful = payment_frame[payment_frame.get("payment_status", pd.Series(index=payment_frame.index, dtype=str)).astype(str).str.upper().eq("SUCCESS")].copy()
            successful["amount_num"] = pd.to_numeric(successful.get("amount_paid", pd.Series(index=successful.index, dtype=str)), errors="coerce")
            successful["order_id"] = successful.get("order_id", pd.Series(index=successful.index, dtype=str)).astype(str).str.strip()
            successful["payment_mode_std"] = successful.get("payment_mode", pd.Series(index=successful.index, dtype=str)).map(standardize_payment_mode)
            amount_by_order: dict[str, list[tuple[float | None, str]]] = {}
            for _, payment in successful.iterrows():
                paid_amount = float(payment["amount_num"]) if pd.notna(payment["amount_num"]) else None
                amount_by_order.setdefault(payment["order_id"], []).append(
                    (paid_amount, str(payment["payment_mode_std"]))
                )
            valid_success_orders: set[str] = set()
            if valid_payments is not None and not valid_payments.empty:
                valid_successes = valid_payments[
                    valid_payments.get("payment_status", pd.Series(index=valid_payments.index, dtype=str))
                    .astype(str).str.upper().eq("SUCCESS")
                ]
                valid_success_orders = set(
                    valid_successes.get("order_id", pd.Series(index=valid_successes.index, dtype=str))
                    .astype(str).str.strip()
                )
            mismatch = []
            missing_payment = []
            for index, row in df.iterrows():
                values = amount_by_order.get(str(row.get("order_id", "")).strip(), [])
                target = amount.loc[index]
                needs_payment = str(row.get("order_status", "")).strip().upper() == "DELIVERED"
                missing_payment.append(
                    needs_payment and str(row.get("order_id", "")).strip() not in valid_success_orders
                )
                mismatch.append(
                    bool(values)
                    and any(
                        value is None
                        or pd.isna(target)
                        or abs(value - float(target)) > 0.01
                        or mode != standardize_payment_mode(row.get("payment_mode", ""))
                        for value, mode in values
                    )
                )
            add_check("successful_payment_matches", "Consistency", "Critical", pd.Series(mismatch, index=df.index), message="successful payment amount or mode does not match the order")
            add_check("delivered_payment_present", "Consistency", "Warning", pd.Series(missing_payment, index=df.index), message="no successful payment has arrived for this delivered order yet")

        valid_dates = order_date[order_date.notna() & (order_date <= pd.Timestamp(now))]
        if len(df) and len(valid_dates):
            stale = (pd.Timestamp(now) - valid_dates.max()).total_seconds() > 24 * 60 * 60
            add_check(
                "latest_order_within_24h",
                "Timeliness",
                "Warning",
                pd.Series([stale] * len(df), index=df.index),
                message="latest order is more than 24 hours old",
            )

    elif name == "payments":
        required("order_id")
        status = col("payment_status")
        add_check("payment_status_allowed", "Validity", "Critical", ~status.isin(ALLOWED_PAYMENT_STATUSES), message="payment_status is not allowed")
        mode = col("payment_mode")
        add_check("payment_mode_allowed", "Validity", "Critical", ~mode.isin(ALLOWED_PAYMENT_MODES), message="payment_mode is not allowed")
        payment_date = parsed_date("payment_ts")
        add_check("payment_ts_parse", "Validity", "Critical", payment_date.isna(), message="payment_ts is missing or invalid")
        add_check("no_future_payment_ts", "Timeliness", "Critical", payment_date.notna() & (payment_date > pd.Timestamp(now)), message="payment_ts is in the future")
        amount = numeric("amount_paid")
        add_check("amount_nonnegative", "Accuracy", "Critical", amount.isna() | (amount < 0), message="amount_paid must be numeric and nonnegative")
        order_ids = set() if orders is None else set(orders.get("order_id", pd.Series(dtype=str)).astype(str).str.strip())
        add_check("order_id_exists", "Consistency", "Critical", col("order_id").ne("") & ~col("order_id").isin(order_ids), message="payment references an unknown order_id")
        if orders is not None and not orders.empty:
            order_lookup = orders.copy()
            order_lookup["order_id"] = order_lookup["order_id"].astype(str).str.strip()
            order_lookup = order_lookup.drop_duplicates("order_id", keep="last").set_index("order_id")
            mismatched = []
            cancelled_order_payment = []
            for index, row in df.iterrows():
                payment_order_id = str(row.get("order_id", "")).strip()
                target_order = order_lookup.loc[payment_order_id] if payment_order_id in order_lookup.index else None
                paid_amount = amount.loc[index]
                expected_amount = (
                    pd.to_numeric(pd.Series([target_order.get("order_amount")]), errors="coerce").iloc[0]
                    if target_order is not None
                    else None
                )
                expected_mode = standardize_payment_mode(target_order.get("payment_mode", "")) if target_order is not None else None
                expected_status = str(target_order.get("order_status", "")).strip().upper() if target_order is not None else None
                is_success = str(row.get("payment_status", "")).strip().upper() == "SUCCESS"
                cancelled_order_payment.append(target_order is not None and expected_status == "CANCELLED")
                mismatched.append(
                    bool(is_success and target_order is not None and (
                        pd.isna(paid_amount)
                        or pd.isna(expected_amount)
                        or abs(float(paid_amount) - float(expected_amount)) > 0.01
                        or standardize_payment_mode(row.get("payment_mode", "")) != expected_mode
                    ) and expected_status != "CANCELLED")
                )
            add_check(
                "successful_amount_matches_order",
                "Consistency",
                "Critical",
                pd.Series(mismatched, index=df.index),
                message="successful payment amount does not match order_amount",
            )
            add_check(
                "cancelled_order_has_no_payment",
                "Consistency",
                "Critical",
                pd.Series(cancelled_order_payment, index=df.index),
                message="payment exists for a cancelled order",
            )
        gateway_missing = col("gateway_ref").eq("")
        add_check("gateway_ref_present", "Completeness", "Warning", gateway_missing, message="gateway_ref is missing")

    elif name == "customers":
        add_check("email_present", "Completeness", "Warning", col("email").eq(""), message="email is missing")
        add_check("phone_present", "Completeness", "Warning", col("phone").eq(""), message="phone is missing")
        add_check("email_format", "Validity", "Warning", ~col("email").str.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", na=False), message="email format should be reviewed")
        add_check("phone_10_digits", "Validity", "Warning", ~col("phone").str.fullmatch(r"\d{10}", na=False), message="phone should contain 10 digits")
        add_check("pincode_6_digits", "Validity", "Critical", ~col("pincode").str.fullmatch(r"\d{6}", na=False), message="pincode must contain six digits")
        add_check("signup_date_parse", "Validity", "Critical", parsed_date("signup_date").isna(), message="signup_date is missing or invalid")
        signup = parsed_date("signup_date")
        add_check("no_future_signup_date", "Timeliness", "Critical", signup.notna() & (signup > pd.Timestamp(now)), message="signup_date is in the future")
        add_check("loyalty_tier_allowed", "Validity", "Warning", ~col("loyalty_tier").str.casefold().isin({"bronze", "silver", "gold", "platinum"}), message="loyalty_tier should be reviewed")
        add_check("city_present", "Completeness", "Warning", col("city").eq(""), message="city is missing")

    elif name == "products":
        required("product_name")
        required("category")
        mrp, cost = numeric("mrp"), numeric("cost_price")
        add_check("mrp_positive", "Accuracy", "Critical", mrp.isna() | (mrp <= 0), message="mrp must be greater than zero")
        add_check("cost_not_above_mrp", "Accuracy", "Critical", cost.isna() | mrp.isna() | (cost > mrp), message="cost_price must not exceed mrp")
        add_check("launch_date_parse", "Validity", "Critical", parsed_date("launch_date").isna(), message="launch_date is missing or invalid")

    elif name == "stores":
        required("store_name")
        required("zone")
        add_check("zone_allowed", "Validity", "Critical", ~col("zone").isin(ALLOWED_ZONES), message="zone must be North, South, East or West")
        add_check("pincode_6_digits", "Validity", "Critical", ~col("pincode").str.fullmatch(r"\d{6}", na=False), message="pincode must contain six digits")
        capacity = numeric("capacity_orders_per_day")
        add_check("capacity_positive", "Accuracy", "Critical", capacity.isna() | (capacity <= 0), message="capacity_orders_per_day must be greater than zero")
        add_check("open_date_parse", "Validity", "Critical", parsed_date("open_date").isna(), message="open_date is missing or invalid")

    # Any row flagged critical is quarantined. Warnings remain available to reporting.
    df["failure_reason"] = ["; ".join(reasons[index]) for index in df.index]
    df["warning_reason"] = ["; ".join(warnings[index]) for index in df.index]
    df["dq_status"] = ["FAIL" if critical_fail.loc[index] else ("WARNING" if warnings[index] else "PASS") for index in df.index]
    failed = df[critical_fail].copy()
    passed = df[~critical_fail].copy()
    return passed, failed, checks


def frame_to_json(frame: pd.DataFrame) -> str:
    """Use compact JSON for Airflow XCom while keeping dates as ISO text."""
    return frame.to_json(orient="records", date_format="iso")


def frame_from_json(payload: str) -> pd.DataFrame:
    if not payload or payload == "[]":
        return pd.DataFrame()
    return pd.read_json(payload, orient="records", dtype=False)


def mask_customer_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = _mask_values(frame, ("email", "phone"))
    if "pincode" in out:
        out["pincode"] = out["pincode"].map(lambda value: "******" if re.fullmatch(r"\d{6}", str(value)) else "***")
    return out.drop(columns=["city", "area", "signup_date"], errors="ignore")

