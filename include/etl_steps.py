"""Runs the validate + transform steps in one call (the DAG will use the same logic)."""
import pandas as pd

from include.dq_checks import CHECKSETS, run_checks
from include.etl_transform import (build_dim_customer, build_dim_product,
                                   build_dim_store, build_fact_orders)
from include.nammamart_config import DATA_DIR


def read_csv(name):
    return pd.read_csv(DATA_DIR / name, dtype=str, keep_default_na=False)


def prepare(now, dag_id="local", run_id="local"):
    ctx = {"now": now}
    passed, failed, results = {}, {}, []

    for name, key in [("customers", "customer_ids"), ("products", "product_ids"),
                      ("stores", "store_ids")]:
        df = read_csv(f"{name}.csv")
        passed[name], failed[name], r = run_checks(df, CHECKSETS[name](ctx), name, dag_id, run_id, now)
        results += r
        ctx[key] = set(df.iloc[:, 0])      # FK checks use ALL master ids

    orders = read_csv("orders.csv")
    ctx["all_order_ids"] = set(orders.order_id)
    passed["orders"], failed["orders"], r = run_checks(orders, CHECKSETS["orders"](ctx),
                                                       "orders", dag_id, run_id, now)
    results += r
    ctx["order_amounts"] = dict(zip(passed["orders"].order_id, passed["orders"].order_amount))

    payments = read_csv("payments.csv")
    passed["payments"], failed["payments"], r = run_checks(payments, CHECKSETS["payments"](ctx),
                                                           "payments", dag_id, run_id, now)
    results += r

    dim_customer = build_dim_customer(passed["customers"])
    dim_product = build_dim_product(passed["products"])
    dim_store = build_dim_store(passed["stores"])
    fact = build_fact_orders(passed["orders"], dim_product)

    return dict(passed=passed, failed=failed, results=results, dim_customer=dim_customer,
                dim_product=dim_product, dim_store=dim_store, fact=fact,
                payments_raw=payments, all_order_ids=ctx["all_order_ids"])