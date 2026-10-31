"""ETL Transform: clean values, mask personal data, build the dimension tables."""
import pandas as pd

from include.masking import mask_email, mask_phone


def build_dim_customer(df):
    d = df.copy()
    d["customer_id"] = d["customer_id"].str.strip()
    d["loyalty_tier"] = d["loyalty_tier"].str.strip().str.title()
    d["email_masked"] = d["email"].map(mask_email)
    d["phone_masked"] = d["phone"].map(mask_phone)
    # full_name, raw email and raw phone are NOT carried into the warehouse
    return d[["customer_id", "email_masked", "phone_masked", "city", "area",
              "pincode", "signup_date", "loyalty_tier", "is_active"]]


def build_dim_product(df):
    d = df.copy()
    for c in ["product_id", "product_name", "category", "sub_category", "brand", "unit"]:
        d[c] = d[c].str.strip()
    d["mrp"] = pd.to_numeric(d["mrp"])
    d["cost_price"] = pd.to_numeric(d["cost_price"])
    return d[["product_id", "product_name", "category", "sub_category", "brand",
              "mrp", "cost_price", "unit", "is_perishable", "launch_date"]]


def build_dim_store(df):
    d = df.copy()
    d["store_id"] = d["store_id"].str.strip()
    d["zone"] = d["zone"].str.strip().str.title()
    d["capacity_orders_per_day"] = pd.to_numeric(d["capacity_orders_per_day"]).astype(int)
    return d[["store_id", "store_name", "zone", "area", "pincode", "open_date",
              "manager_name", "capacity_orders_per_day"]]




# ---------- FACT table ----------
from include.nammamart_config import PAYMENT_MODE_MAP


def build_fact_orders(orders_passed, dim_product):
    d = orders_passed.copy()
    for c in ["order_id", "customer_id", "store_id", "product_id"]:
        d[c] = d[c].str.strip()
    d["order_ts"] = pd.to_datetime(d["order_ts"].str.strip(), errors="coerce")
    d["order_date"] = d["order_ts"].dt.date
    d["order_status"] = d["order_status"].str.strip().str.upper()
    d["payment_mode"] = d["payment_mode"].str.strip().str.lower().map(PAYMENT_MODE_MAP)
    for c in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        d[c] = pd.to_numeric(d[c].str.strip(), errors="coerce")

    cost = dim_product.set_index("product_id")["cost_price"]
    delivered = d["order_status"].eq("DELIVERED")
    cost_price = d["product_id"].map(cost)

    d["net_revenue"] = d["order_amount"].where(delivered, 0.0)
    d["gross_margin"] = (d["order_amount"] - d["quantity"] * cost_price).where(delivered, 0.0)
    d["within_15_min"] = delivered & (d["delivery_minutes"] <= 15)

    return d[["order_id", "order_ts", "order_date", "customer_id", "store_id", "product_id",
              "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
              "order_status", "channel", "delivery_minutes",
              "net_revenue", "gross_margin", "within_15_min"]]