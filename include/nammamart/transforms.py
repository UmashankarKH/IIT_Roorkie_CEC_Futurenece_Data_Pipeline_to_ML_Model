"""Standardising, masking and derived columns for the ETL pipeline.

Masking happens here, before anything is written to the warehouse, so no
reporting table ever holds a raw email or phone number.
"""

from __future__ import annotations

import pandas as pd

from . import config as cfg


def mask_email(value: object) -> str | None:
    """pr***@example.com - keeps the domain so support can still tell who it is."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if "@" not in text:
        return "***"
    local, _, domain = text.partition("@")
    keep = local[:2] if len(local) > 2 else local[:1]
    return f"{keep}{'*' * 3}@{domain}"


def mask_phone(value: object) -> str | None:
    """98******21 - enough to match a call log, not enough to dial."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) < 4:
        return "*" * len(digits) if digits else "***"
    return f"{digits[:2]}{'*' * (len(digits) - 4)}{digits[-2:]}"


def mask_name(value: object) -> str | None:
    """Priya H. - the reporting tables do not need full names."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parts = str(value).strip().split()
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} {parts[-1][0]}."


def standardise_payment_mode(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip().str.lower()
    return cleaned.map(cfg.PAYMENT_MODE_MAP).fillna(series)


def standardise_orders(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_id"] = out["order_id"].astype(str).str.strip()
    out["order_status"] = out["order_status"].astype(str).str.strip().str.upper()
    out["payment_mode"] = standardise_payment_mode(out["payment_mode"])
    out["channel"] = out["channel"].astype(str).str.strip().str.title()
    out["order_ts"] = pd.to_datetime(out["order_ts"], errors="coerce", format="mixed")
    out["order_date"] = out["order_ts"].dt.date
    for col in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    # Recompute rather than trust the export: the profile found 18 rows that disagree.
    out["net_revenue"] = (out["quantity"] * out["unit_price"]
                          * (1 - out["discount_pct"] / 100)).round(2)
    within = out["delivery_minutes"].le(cfg.DELIVERY_SLA_MINUTES).astype("object")
    out["within_15_min"] = within.where(out["delivery_minutes"].notna(), None)
    return out


def standardise_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["customer_id"] = out["customer_id"].astype(str).str.strip()
    out["loyalty_tier"] = out["loyalty_tier"].astype(str).str.strip().str.upper()
    out["city"] = out["city"].astype(str).str.strip().str.title().replace({"Nan": None})
    out["area"] = out["area"].astype(str).str.strip().str.title()
    out["is_active"] = out["is_active"].astype(str).str.strip().str.upper().eq("Y")
    out["signup_date"] = pd.to_datetime(out["signup_date"], errors="coerce").dt.date
    out["customer_name_masked"] = out["full_name"].map(mask_name)
    out["email_masked"] = out["email"].map(mask_email)
    out["phone_masked"] = out["phone"].map(mask_phone)
    # The raw PII columns are dropped here and never reach the warehouse.
    return out.drop(columns=["full_name", "email", "phone"])


def standardise_products(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["product_id"] = out["product_id"].astype(str).str.strip()
    out["category"] = out["category"].astype(str).str.strip().str.title().replace({"Nan": None})
    out["sub_category"] = out["sub_category"].astype(str).str.strip().str.title()
    out["brand"] = out["brand"].astype(str).str.strip()
    for col in ("mrp", "cost_price"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["is_perishable"] = out["is_perishable"].astype(str).str.strip().str.lower().isin(
        ["true", "1", "yes", "y"])
    out["launch_date"] = pd.to_datetime(out["launch_date"], errors="coerce").dt.date
    return out


def standardise_stores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["store_id"] = out["store_id"].astype(str).str.strip()
    out["zone"] = out["zone"].astype(str).str.strip().str.title().replace({"Nan": None})
    out["area"] = out["area"].astype(str).str.strip().str.title()
    out["manager_name_masked"] = out["manager_name"].map(mask_name)
    out["capacity_orders_per_day"] = pd.to_numeric(out["capacity_orders_per_day"],
                                                   errors="coerce")
    out["open_date"] = pd.to_datetime(out["open_date"], errors="coerce").dt.date
    return out.drop(columns=["manager_name"])


def standardise_payments(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["payment_id"] = out["payment_id"].astype(str).str.strip()
    out["order_id"] = out["order_id"].astype(str).str.strip()
    out["payment_mode"] = standardise_payment_mode(out["payment_mode"])
    out["payment_status"] = out["payment_status"].astype(str).str.strip().str.upper()
    out["amount_paid"] = pd.to_numeric(out["amount_paid"], errors="coerce")
    out["payment_ts"] = pd.to_datetime(out["payment_ts"], errors="coerce", format="mixed")
    return out


PII_MASKS = {
    "full_name": mask_name,
    "manager_name": mask_name,
    "email": mask_email,
    "phone": mask_phone,
}


def mask_pii_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Mask any raw PII column present, in place of dropping it.

    Used on the quarantine output. A rejected customer row is still a real person's
    record, and none of the rules that reject one (duplicate id, future signup date)
    need the name, email or phone to be readable in order to be fixed. Without this
    the quarantine CSVs were the one place in the repo holding unmasked contact
    details, which is exactly the problem the client hired us to stop.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    for column, masker in PII_MASKS.items():
        if column in out.columns:
            out[column] = out[column].map(masker)
    return out


def deduplicate(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Last row wins: the exports are append-only, so later rows are the correction."""
    return df.drop_duplicates(subset=[key], keep="last")


def build_fact_orders(orders: pd.DataFrame, customers: pd.DataFrame,
                      products: pd.DataFrame, stores: pd.DataFrame) -> pd.DataFrame:
    """Join the clean orders onto the dimensions and derive the reporting columns."""
    fact = (orders
            .merge(products[["product_id", "category", "sub_category", "cost_price"]],
                   on="product_id", how="left")
            .merge(stores[["store_id", "store_name", "zone"]], on="store_id", how="left")
            .merge(customers[["customer_id", "loyalty_tier"]], on="customer_id", how="left"))

    # A dimension row that failed quality leaves nulls here: the order still counts as
    # revenue, but its margin is unknown and the row says so.
    missing_dim = fact["cost_price"].isna() | fact["category"].isna()
    if missing_dim.any():
        note = "Warning: product master row quarantined, margin unknown"
        fact.loc[missing_dim, "dq_warnings"] = (
            fact.loc[missing_dim, "dq_warnings"].fillna("").replace("", note)
        )
    fact["gross_margin"] = (fact["net_revenue"] - fact["quantity"] * fact["cost_price"]).round(2)
    # Only delivered orders count as revenue, per the client's rule.
    fact["is_revenue"] = fact["order_status"].eq(cfg.REVENUE_STATUS)
    fact["revenue_amount"] = fact["net_revenue"].where(fact["is_revenue"], 0.0)
    fact["margin_amount"] = fact["gross_margin"].where(fact["is_revenue"], 0.0)

    columns = ["order_id", "order_ts", "order_date", "customer_id", "store_id", "store_name",
               "zone", "product_id", "category", "sub_category", "loyalty_tier", "quantity",
               "unit_price", "discount_pct", "net_revenue", "cost_price", "gross_margin",
               "is_revenue", "revenue_amount", "margin_amount", "payment_mode", "order_status",
               "channel", "delivery_minutes", "within_15_min", "dq_warnings"]
    return fact[[c for c in columns if c in fact.columns]]


def reconcile_payments(fact: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    """One row per order/payment mismatch, with the reason finance needs."""
    successful = payments[payments["payment_status"] == "SUCCESS"]
    paid = (successful.groupby("order_id", as_index=False)
            .agg(amount_paid=("amount_paid", "sum"), payment_count=("payment_id", "count")))

    merged = fact[["order_id", "order_status", "net_revenue", "payment_mode"]].merge(
        paid, on="order_id", how="outer", indicator=True)

    def classify(row: pd.Series) -> str | None:
        if row["_merge"] == "right_only":
            return "payment with no matching clean order"
        if row["order_status"] == cfg.REVENUE_STATUS and pd.isna(row["amount_paid"]):
            return "delivered order with no successful payment"
        if row["order_status"] == "CANCELLED" and pd.notna(row["amount_paid"]):
            return "cancelled order that was still paid"
        if pd.notna(row["amount_paid"]) and pd.notna(row["net_revenue"]):
            gap = round(row["amount_paid"] - row["net_revenue"], 2)
            if abs(gap) > cfg.AMOUNT_TOLERANCE:
                return f"paid {gap:+.2f} against the order amount"
        if pd.notna(row["payment_count"]) and row["payment_count"] > 1:
            return "more than one successful payment for the order"
        return None

    merged["mismatch_reason"] = merged.apply(classify, axis=1)
    out = merged[merged["mismatch_reason"].notna()].copy()
    out["variance"] = (out["amount_paid"].fillna(0) - out["net_revenue"].fillna(0)).round(2)
    return out[["order_id", "order_status", "net_revenue", "amount_paid", "payment_count",
                "variance", "mismatch_reason"]]
