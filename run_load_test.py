import pandas as pd
from include.dq_checks import CHECKSETS, run_checks
from include.etl_transform import (build_dim_customer, build_dim_product,
                                   build_dim_store, build_fact_orders)
from include.etl_load import load_dim, load_fact
from include.nammamart_config import DATA_DIR, WAREHOUSE

NOW = pd.Timestamp("2026-10-24 09:00")
rd = lambda f: pd.read_csv(DATA_DIR / f, dtype=str, keep_default_na=False)

ctx, passed = {"now": NOW}, {}
for name, key in [("customers", "customer_ids"), ("products", "product_ids"), ("stores", "store_ids")]:
    df = rd(f"{name}.csv")
    passed[name], _, _ = run_checks(df, CHECKSETS[name](ctx), name, "t", "r1", NOW)
    ctx[key] = set(df.iloc[:, 0])

dim_customer = build_dim_customer(passed["customers"])
dim_product = build_dim_product(passed["products"])
dim_store = build_dim_store(passed["stores"])

orders = rd("orders.csv")
po, _, _ = run_checks(orders, CHECKSETS["orders"](ctx), "orders", "t", "r1", NOW)
fact = build_fact_orders(po, dim_product)

print("warehouse file:", WAREHOUSE)
print("etl_dim_customer:", load_dim(dim_customer, "etl_dim_customer"))
print("etl_dim_product :", load_dim(dim_product, "etl_dim_product"))
print("etl_dim_store   :", load_dim(dim_store, "etl_dim_store"))
print("etl_fact_orders :", load_fact(fact))