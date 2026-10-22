"""
include/nammamart_smit/profile_data.py
--------------------------------------
Part 0: profile every source file before writing pipeline code.
For each file: row count, duplicate keys, and per column the blanks, distinct
values, min / max (numbers and dates) or the value counts (category columns).

Run locally:  python include/nammamart_smit/profile_data.py include/data/nammamart
"""

import os
import sys

import pandas as pd

FILES = {
    "orders.csv": "order_id",
    "orders_2026-10-01_increment.csv": "order_id",
    "payments.csv": "payment_id",
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "stores.csv": "store_id",
}
CATEGORY_COLUMNS = ["order_status", "payment_mode", "payment_status", "channel", "loyalty_tier",
                    "zone", "category", "unit", "is_perishable", "is_active", "city"]
DATE_COLUMNS = ["order_ts", "payment_ts", "signup_date", "launch_date", "open_date"]
PII_COLUMNS = ["full_name", "email", "phone"]   # never print values, only counts


def profile_file(path, key):
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    dup_keys = df[df.duplicated(key, keep=False)][key].nunique()
    print(f"\n#### {os.path.basename(path)}\n")
    print(f"Rows: {len(df):,} · distinct {key}: {df[key].nunique():,} · duplicated keys: {dup_keys} · "
          f"exact duplicate rows: {df.duplicated().sum()}\n")
    print("| column | blanks | distinct | min | max | values |")
    print("|---|---|---|---|---|---|")
    for col in df.columns:
        values = df[col].str.strip()
        blanks = int((values == "").sum())
        low = high = shown = ""
        if col in CATEGORY_COLUMNS:
            shown = ", ".join(f"`{k}` {v}" for k, v in df[col].value_counts().items())
        elif col in DATE_COLUMNS:
            parsed = pd.to_datetime(values, errors="coerce", format="mixed")
            bad = int((parsed.isna() & (values != "")).sum())
            low, high = parsed.min(), parsed.max()
            shown = f"{bad} unparseable" if bad else ""
        elif col not in PII_COLUMNS:
            numbers = pd.to_numeric(values, errors="coerce")
            if numbers.notna().mean() > 0.9:
                low, high = numbers.min(), numbers.max()
        print(f"| {col} | {blanks} | {df[col].nunique():,} | {low} | {high} | {shown} |")


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "include/data/nammamart"
    for file_name, key in FILES.items():
        profile_file(os.path.join(folder, file_name), key)
