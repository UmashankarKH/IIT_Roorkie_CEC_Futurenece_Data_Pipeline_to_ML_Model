"""ETL incremental load: validate the increment, upsert clean rows into etl_fact_orders."""
import duckdb
import pandas as pd

from include.dq_checks import CHECKSETS, run_checks
from include.etl_load import bad_order_ids, load_fact, purge_fact
from include.etl_steps import read_csv
from include.etl_transform import build_fact_orders
from include.nammamart_config import DATA_DIR, INCREMENT_FILE, WAREHOUSE
from include.schema_contract import assert_schema


def apply_increment(now, dag_id, run_id):
    if not (DATA_DIR / INCREMENT_FILE).exists():
        return dict(applied=False, rows=0, passed=0, quarantined=0, inserted=0, updated=0,
                    purged=0, failed=pd.DataFrame(), results=[])
    inc = read_csv(INCREMENT_FILE)
    assert_schema("orders", inc.columns)
    ctx = {"now": now,
           "customer_ids": set(read_csv("customers.csv").customer_id),
           "product_ids": set(read_csv("products.csv").product_id),
           "store_ids": set(read_csv("stores.csv").store_id)}
    passed, failed, results = run_checks(inc, CHECKSETS["orders"](ctx), "orders_increment",
                                         dag_id, run_id, now)
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    dim_product = con.execute("SELECT * FROM etl_dim_product").df()
    existing = {r[0] for r in con.execute("SELECT order_id FROM etl_fact_orders").fetchall()}
    con.close()
    fact = build_fact_orders(passed, dim_product)
    updated = int(fact.order_id.isin(existing).sum())
    inserted = len(fact) - updated
    load_fact(fact)                      # delete matching order_id, then insert = upsert
    purged = purge_fact(bad_order_ids(failed, passed))
    return dict(applied=True, rows=len(inc), passed=len(passed), quarantined=len(failed),
                inserted=inserted, updated=updated, purged=purged, failed=failed, results=results)
