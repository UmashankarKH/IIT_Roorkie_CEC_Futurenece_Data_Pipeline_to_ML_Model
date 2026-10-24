import pandas as pd
from include.dq_checks import CHECKSETS, run_checks
from include.etl_transform import build_dim_product, build_fact_orders
from include.nammamart_config import DATA_DIR

NOW = pd.Timestamp("2026-10-24 09:00")
rd = lambda f: pd.read_csv(DATA_DIR / f, dtype=str, keep_default_na=False)

ctx = {"now": NOW}
products_passed = None
for name, key in [("customers", "customer_ids"), ("products", "product_ids"), ("stores", "store_ids")]:
    df = rd(f"{name}.csv")
    p, f, _ = run_checks(df, CHECKSETS[name](ctx), name, "t", "r1", NOW)
    ctx[key] = set(df.iloc[:, 0])
    if name == "products":
        products_passed = p

orders = rd("orders.csv")
po, fo, _ = run_checks(orders, CHECKSETS["orders"](ctx), "orders", "t", "r1", NOW)
fact = build_fact_orders(po, build_dim_product(products_passed))

delivered = fact[fact.order_status == "DELIVERED"]
print("fact rows:", len(fact))
print(fact.order_status.value_counts().to_string())
print("net revenue (delivered):", round(fact.net_revenue.sum(), 2))
print("gross margin:", round(fact.gross_margin.sum(), 2))
print("% delivered within 15 min:", round(100 * delivered.within_15_min.mean(), 1))
print()
print(fact.head(3).T.to_string())