"""
Data Profiling Script for NammaMart Dataset (Part 0)
Performs comprehensive quality assessment across all 6 files:
- Row counts and schemas
- Missing values (Null counts)
- Primary key duplicates
- Referential integrity checks
- Business rule validations
- Summary of issues per quality dimension
"""

import os
import pandas as pd
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "nammamart")

def run_profiling():
    files = {
        "orders": "orders.csv",
        "payments": "payments.csv",
        "customers": "customers.csv",
        "products": "products.csv",
        "stores": "stores.csv",
        "increment": "orders_2026-10-01_increment.csv",
    }
    
    dfs = {}
    for key, fname in files.items():
        fpath = os.path.join(DATA_DIR, fname)
        dfs[key] = pd.read_csv(fpath)
        print(f"Loaded {fname}: {dfs[key].shape[0]} rows, {dfs[key].shape[1]} columns")

    orders = dfs["orders"]
    payments = dfs["payments"]
    customers = dfs["customers"]
    products = dfs["products"]
    stores = dfs["stores"]
    increment = dfs["increment"]

    print("\n" + "="*50)
    print("1. SUMMARY OF NULL COUNTS PER COLUMN")
    print("="*50)
    for name, df in dfs.items():
        nulls = df.isnull().sum()
        active_nulls = nulls[nulls > 0]
        print(f"\n[{name.upper()}] Total Rows: {len(df)}")
        if len(active_nulls) == 0:
            print("  No null values.")
        else:
            for col, count in active_nulls.items():
                print(f"  - {col}: {count} ({count/len(df)*100:.2f}%)")

    print("\n" + "="*50)
    print("2. PRIMARY KEY UNIQUENESS CHECKS")
    print("="*50)
    pk_map = {
        "orders": "order_id",
        "payments": "payment_id",
        "customers": "customer_id",
        "products": "product_id",
        "stores": "store_id",
    }
    for name, pk in pk_map.items():
        df = dfs[name]
        dups = df[df[pk].duplicated(keep=False)]
        print(f"[{name.upper()}] PK '{pk}': {len(dups)} duplicate rows ({df[pk].duplicated().sum()} duplicate entries)")

    print("\n" + "="*50)
    print("3. REFERENTIAL INTEGRITY (FOREIGN KEYS)")
    print("="*50)
    # orders -> customers
    missing_cust = orders[orders["customer_id"].notna() & (~orders["customer_id"].isin(customers["customer_id"]))]
    print(f"orders.customer_id -> customers.customer_id: {len(missing_cust)} orphan rows")

    # orders -> products
    missing_prod = orders[orders["product_id"].notna() & (~orders["product_id"].isin(products["product_id"]))]
    print(f"orders.product_id -> products.product_id: {len(missing_prod)} orphan rows ({missing_prod['product_id'].unique()})")

    # orders -> stores
    missing_store = orders[orders["store_id"].notna() & (~orders["store_id"].isin(stores["store_id"]))]
    print(f"orders.store_id -> stores.store_id: {len(missing_store)} orphan rows ({missing_store['store_id'].unique()})")

    # payments -> orders
    missing_order_pay = payments[payments["order_id"].notna() & (~payments["order_id"].isin(orders["order_id"]))]
    print(f"payments.order_id -> orders.order_id: {len(missing_order_pay)} orphan rows ({missing_order_pay['order_id'].unique()})")

    print("\n" + "="*50)
    print("4. CATEGORICAL & VALUE DISTRIBUTIONS")
    print("="*50)
    print("orders.order_status distinct:", orders["order_status"].unique().tolist())
    print("orders.payment_mode distinct:", orders["payment_mode"].unique().tolist())
    print("payments.payment_mode distinct:", payments["payment_mode"].unique().tolist())
    print("payments.payment_status distinct:", payments["payment_status"].unique().tolist())
    print("customers.loyalty_tier distinct:", customers["loyalty_tier"].unique().tolist())
    print("stores.zone distinct:", stores["zone"].unique().tolist())
    print("products.category distinct:", products["category"].unique().tolist())

    print("\n" + "="*50)
    print("5. BUSINESS RULES ACCURACY & NUMERIC CHECKS")
    print("="*50)
    # Formula check
    calc_amount = (orders["quantity"] * orders["unit_price"] * (1 - orders["discount_pct"] / 100.0)).round(2)
    amount_mismatch = orders[orders["order_amount"].notna() & ((orders["order_amount"] - calc_amount).abs() > 0.05)]
    print(f"orders order_amount formula mismatches: {len(amount_mismatch)}")

    # Quantity <= 0
    neg_qty = orders[orders["quantity"] <= 0]
    print(f"orders quantity <= 0: {len(neg_qty)} rows")

    # Discount pct bounds
    bad_disc = orders[(orders["discount_pct"] < 0) | (orders["discount_pct"] > 100)]
    print(f"orders discount_pct out of range (0-100%): {len(bad_disc)} rows")

    # Delivery minutes check
    deliv_null = orders[(orders["order_status"].str.upper() == "DELIVERED") & orders["delivery_minutes"].isna()]
    print(f"DELIVERED orders with null delivery_minutes: {len(deliv_null)}")
    deliv_high = orders[(orders["order_status"].str.upper() == "DELIVERED") & (orders["delivery_minutes"] > 120)]
    print(f"DELIVERED orders with delivery_minutes > 120: {len(deliv_high)}")

    # Product MRP / cost price
    neg_mrp = products[products["mrp"] <= 0]
    print(f"products with mrp <= 0: {len(neg_mrp)}")
    cp_gt_mrp = products[products["cost_price"] > products["mrp"]]
    print(f"products with cost_price > mrp: {len(cp_gt_mrp)}")

    # Stores missing zone and capacity 0
    store_null_zone = stores[stores["zone"].isna()]
    print(f"stores missing zone: {len(store_null_zone)} ({store_null_zone['store_id'].tolist()})")
    store_zero_cap = stores[stores["capacity_orders_per_day"] <= 0]
    print(f"stores with capacity <= 0: {len(store_zero_cap)} ({store_zero_cap['store_id'].tolist()})")

    # Customer PII validity
    bad_email = customers[customers["email"].notna() & (~customers["email"].str.contains(r"^[\w\.-]+@[\w\.-]+\.\w+$", regex=True))]
    print(f"customers with invalid email format: {len(bad_email)}")
    bad_phone = customers[customers["phone"].notna() & (~customers["phone"].astype(str).str.match(r"^\d{10}$"))]
    print(f"customers with invalid phone (<>10 digits): {len(bad_phone)}")
    bad_pin = customers[customers["pincode"].notna() & (~customers["pincode"].astype(str).str.match(r"^\d{6}$"))]
    print(f"customers with invalid pincode (<>6 digits): {len(bad_pin)}")

    # Timeliness
    future_orders = orders[orders["order_ts"].notna() & (orders["order_ts"].str.slice(0, 4) > "2026")]
    print(f"orders with future timestamp (>2026): {len(future_orders)}")
    future_cust = customers[customers["signup_date"].notna() & (customers["signup_date"].str.slice(0, 4) > "2026")]
    print(f"customers with future signup_date (>2026): {len(future_cust)}")

    print("\n" + "="*50)
    print("6. INCREMENT FILE ANALYSIS")
    print("="*50)
    overlap = increment[increment["order_id"].isin(orders["order_id"])]
    new_inc = increment[~increment["order_id"].isin(orders["order_id"])]
    print(f"Increment file total: {len(increment)} rows")
    print(f"  - Overlapping existing orders: {len(overlap)} (Status updates to RETURNED)")
    print(f"  - New orders: {len(new_inc)}")

if __name__ == "__main__":
    run_profiling()
