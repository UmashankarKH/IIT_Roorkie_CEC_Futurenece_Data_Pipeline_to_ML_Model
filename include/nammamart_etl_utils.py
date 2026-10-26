from __future__ import annotations

import os
from datetime import datetime
from typing import Iterable, List, Tuple

import pandas as pd

VALID_ORDER_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
VALID_LOYALTY_TIERS = {"BRONZE", "SILVER", "GOLD", "PLATINUM"}
VALID_ZONES = {"North", "South", "East", "West"}


def clean_text(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def standardize_mode(value):
    value = clean_text(value)
    if value is None:
        return None
    normalized = value.strip().lower().replace("  ", " ")
    map_values = {
        "upi": "UPI",
        "card": "Card",
        "cash on delivery": "Cash on Delivery",
        "cashh": "Cash on Delivery",
        "cod": "Cash on Delivery",
        "wallet": "Wallet",
    }
    return map_values.get(normalized, value)


def standardize_loyalty(value):
    value = clean_text(value)
    if value is None:
        return None
    mapping = {
        "bronze": "BRONZE",
        "silver": "SILVER",
        "gold": "GOLD",
        "platinum": "PLATINUM",
    }
    return mapping.get(value.lower(), value.upper())


def standardize_zone(value):
    value = clean_text(value)
    if value is None:
        return None
    return value.title() if value.title() in VALID_ZONES else value


def mask_email(email):
    email = clean_text(email)
    if not email or "@" not in email:
        return email
    local, domain = email.split("@", 1)
    masked_local = local[:3] + "***" if len(local) > 3 else "***"
    return f"{masked_local}@{domain}"


def mask_phone(phone):
    phone = clean_text(phone)
    if not phone:
        return phone
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) < 6:
        return phone
    return digits[:2] + ("*" * (len(digits) - 4)) + digits[-2:]


def validate_dataframe(df, file_name):
    df = df.copy()
    reasons = []
    for column in df.columns:
        df[column] = df[column].apply(clean_text)
    if file_name == "orders.csv":
        for idx, row in df.iterrows():
            item_reasons = []
            if pd.isna(row.get("order_ts")) or str(row.get("order_ts", "")).strip() == "":
                item_reasons.append("missing order_ts")
            if pd.isna(row.get("customer_id")) or str(row.get("customer_id", "")).strip() == "":
                item_reasons.append("missing customer_id")
            if str(row.get("order_status", "")).strip() not in VALID_ORDER_STATUS:
                item_reasons.append("invalid order_status")
            status = str(row.get("order_status", "")).strip()
            if status == "DELIVERED":
                if pd.isna(row.get("delivery_minutes")) or str(row.get("delivery_minutes", "")).strip() == "":
                    item_reasons.append("missing delivery_minutes")
            qty = pd.to_numeric(row.get("quantity"), errors="coerce")
            if pd.isna(qty) or qty <= 0:
                item_reasons.append("invalid quantity")
            price = pd.to_numeric(row.get("unit_price"), errors="coerce")
            disc = pd.to_numeric(row.get("discount_pct"), errors="coerce")
            calc = float(price or 0) * float(qty or 0) * (1 - float(disc or 0) / 100.0)
            actual = pd.to_numeric(row.get("order_amount"), errors="coerce")
            if pd.notna(actual):
                if round(float(actual), 2) != round(float(calc), 2):
                    item_reasons.append("order_amount mismatch")
            if row.get("payment_mode") is not None:
                normalized = standardize_mode(row.get("payment_mode"))
                if normalized not in VALID_PAYMENT_MODES:
                    item_reasons.append("invalid payment_mode")
            if item_reasons:
                reasons.append({"row_index": int(idx), "failure_reason": "; ".join(item_reasons)})
        return df, pd.DataFrame(reasons)

    if file_name == "payments.csv":
        for idx, row in df.iterrows():
            item_reasons = []
            if pd.isna(row.get("order_id")) or str(row.get("order_id", "")).strip() == "":
                item_reasons.append("missing order_id")
            amount = pd.to_numeric(row.get("amount_paid"), errors="coerce")
            if pd.isna(amount) or float(amount) < 0:
                item_reasons.append("invalid amount_paid")
            normalized = standardize_mode(row.get("payment_mode"))
            if normalized not in VALID_PAYMENT_MODES:
                item_reasons.append("invalid payment_mode")
            if row.get("payment_status") not in {"SUCCESS", "FAILED", "PENDING"}:
                item_reasons.append("invalid payment_status")
            if item_reasons:
                reasons.append({"row_index": int(idx), "failure_reason": "; ".join(item_reasons)})
        return df, pd.DataFrame(reasons)

    if file_name == "customers.csv":
        for idx, row in df.iterrows():
            item_reasons = []
            if pd.isna(row.get("customer_id")) or str(row.get("customer_id", "")).strip() == "":
                item_reasons.append("missing customer_id")
            if row.get("email") is not None and "@" not in str(row.get("email")):
                item_reasons.append("invalid email")
            if row.get("phone") is not None and len("".join(ch for ch in str(row.get("phone")) if ch.isdigit())) < 10:
                item_reasons.append("invalid phone")
            tier = standardize_loyalty(row.get("loyalty_tier"))
            if tier is not None and tier not in VALID_LOYALTY_TIERS:
                item_reasons.append("invalid loyalty_tier")
            if item_reasons:
                reasons.append({"row_index": int(idx), "failure_reason": "; ".join(item_reasons)})
        return df, pd.DataFrame(reasons)

    if file_name == "products.csv":
        for idx, row in df.iterrows():
            item_reasons = []
            if pd.isna(row.get("product_id")) or str(row.get("product_id", "")).strip() == "":
                item_reasons.append("missing product_id")
            if pd.isna(row.get("category")) or str(row.get("category", "")).strip() == "":
                item_reasons.append("missing category")
            mrp = pd.to_numeric(row.get("mrp"), errors="coerce")
            cost = pd.to_numeric(row.get("cost_price"), errors="coerce")
            if pd.isna(mrp) or float(mrp) <= 0:
                item_reasons.append("invalid mrp")
            if pd.notna(cost) and pd.notna(mrp) and float(cost) > float(mrp):
                item_reasons.append("cost_price exceeds mrp")
            if item_reasons:
                reasons.append({"row_index": int(idx), "failure_reason": "; ".join(item_reasons)})
        return df, pd.DataFrame(reasons)

    if file_name == "stores.csv":
        for idx, row in df.iterrows():
            item_reasons = []
            if pd.isna(row.get("store_id")) or str(row.get("store_id", "")).strip() == "":
                item_reasons.append("missing store_id")
            if pd.isna(row.get("zone")) or str(row.get("zone", "")).strip() == "":
                item_reasons.append("missing zone")
            if standardize_zone(row.get("zone")) not in VALID_ZONES:
                item_reasons.append("invalid zone")
            if item_reasons:
                reasons.append({"row_index": int(idx), "failure_reason": "; ".join(item_reasons)})
        return df, pd.DataFrame(reasons)

    return df, pd.DataFrame(reasons)


def flatten_json_payload(payload):
    return {key: pd.DataFrame.from_records(value) for key, value in payload.items()}


def build_fact_orders(orders, customers, products, stores, run_id: str):
    cust = customers.copy()
    prod = products.copy()
    store = stores.copy()
    for df in (cust, prod, store):
        for column in df.columns:
            df[column] = df[column].apply(clean_text)

    cust["email"] = cust["email"].apply(mask_email)
    cust["phone"] = cust["phone"].apply(mask_phone)
    cust["loyalty_tier"] = cust["loyalty_tier"].apply(standardize_loyalty)

    prod["category"] = prod["category"].apply(clean_text)
    store["zone"] = store["zone"].apply(standardize_zone)

    merged = orders.merge(cust[["customer_id", "full_name", "email", "phone", "loyalty_tier"]], on="customer_id", how="left")
    merged = merged.merge(prod[["product_id", "category", "cost_price", "brand"]], on="product_id", how="left")
    merged = merged.merge(store[["store_id", "store_name", "zone", "manager_name"]], on="store_id", how="left")

    merged["order_ts"] = pd.to_datetime(merged["order_ts"], errors="coerce")
    merged["order_date"] = merged["order_ts"].dt.strftime("%Y-%m-%d")
    merged["net_revenue"] = (
        pd.to_numeric(merged["quantity"], errors="coerce")
        * pd.to_numeric(merged["unit_price"], errors="coerce")
        * (1 - pd.to_numeric(merged["discount_pct"], errors="coerce") / 100)
    )
    merged["net_revenue"] = merged["net_revenue"].round(2)
    merged["gross_margin"] = (
        merged["net_revenue"] - pd.to_numeric(merged["quantity"], errors="coerce") * pd.to_numeric(merged["cost_price"], errors="coerce")
    ).round(2)
    merged["within_15_min"] = pd.to_numeric(merged["delivery_minutes"], errors="coerce") <= 15
    merged["run_id"] = run_id

    fact_cols = [
        "order_id", "order_date", "order_ts", "customer_id", "store_id", "store_name", "zone", "product_id",
        "category", "quantity", "unit_price", "discount_pct", "order_amount", "net_revenue", "gross_margin",
        "payment_mode", "order_status", "channel", "delivery_minutes", "within_15_min", "full_name", "email",
        "phone", "loyalty_tier", "manager_name", "brand", "run_id"
    ]
    fact = merged[[col for col in fact_cols if col in merged.columns]].copy()
    return fact


def write_duckdb_table(con, table_name, df, key_columns=None):
    if df.empty:
        return
    if key_columns:
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df WHERE 1=0")
        con.register("df", df)
        con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df")
    else:
        con.register("df", df)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df WHERE 1=0")
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")


def ensure_duckdb(db_path: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return db_path


def reconcile_payments(orders_df, payments_df):
    orders_clean = orders_df.copy()
    payments_clean = payments_df.copy()
    orders_clean["order_id"] = orders_clean["order_id"].astype(str)
    payments_clean["order_id"] = payments_clean["order_id"].astype(str)
    payments_clean["amount_paid"] = pd.to_numeric(payments_clean["amount_paid"], errors="coerce")
    orders_clean["order_amount"] = pd.to_numeric(orders_clean["order_amount"], errors="coerce")

    merged = orders_clean[["order_id", "order_amount", "payment_mode", "order_status"]].merge(
        payments_clean[["order_id", "payment_mode", "amount_paid", "payment_status"]],
        on="order_id",
        how="outer",
        suffixes=("_order", "_payment"),
    )
    mismatch_rows = []
    for _, row in merged.iterrows():
        order_amount = row.get("order_amount")
        payment_amount = row.get("amount_paid")
        payment_status = row.get("payment_status")
        if pd.isna(order_amount) and pd.isna(payment_amount):
            continue
        if pd.notna(order_amount) and pd.notna(payment_amount) and round(float(order_amount), 2) != round(float(payment_amount), 2):
            mismatch_rows.append({
                "order_id": row.get("order_id"),
                "order_amount": order_amount,
                "amount_paid": payment_amount,
                "reason": "amount mismatch",
            })
        elif payment_status == "SUCCESS" and pd.notna(order_amount) and pd.notna(payment_amount) and round(float(order_amount), 2) == round(float(payment_amount), 2):
            continue
        elif payment_status == "FAILED" and pd.notna(order_amount):
            mismatch_rows.append({
                "order_id": row.get("order_id"),
                "order_amount": order_amount,
                "amount_paid": payment_amount,
                "reason": "payment failed",
            })
    return pd.DataFrame(mismatch_rows)


def add_audit_row(con, run_id, dag_id, status, rows_extracted, rows_loaded, rows_quarantined):
    con.execute("CREATE TABLE IF NOT EXISTS pipeline_runs (run_id TEXT, dag_id TEXT, start_time TIMESTAMP, end_time TIMESTAMP, rows_extracted INT, rows_loaded INT, rows_quarantined INT, status TEXT)")
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    con.execute(
        "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [run_id, dag_id, ts, ts, rows_extracted, rows_loaded, rows_quarantined, status],
    )
