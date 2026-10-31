"""ELT incremental load: append the increment to raw, clean it, MERGE into stg_orders on order_id."""
import duckdb
import pandas as pd

from include.elt_raw import load_raw
from include.elt_sql import run_sql
from include.nammamart_config import DATA_DIR, INCREMENT_FILE, WAREHOUSE
from include.schema_contract import assert_schema


def apply_increment(run_id):
    path = DATA_DIR / INCREMENT_FILE
    if not path.exists():
        return {"applied": False, "rows": 0, "inserted": 0, "updated": 0}
    assert_schema("orders", pd.read_csv(path, nrows=0).columns)
    rows = load_raw("raw_orders", INCREMENT_FILE, run_id)          # raw: append as received
    run_sql("stg_orders_inc.sql", run_id)                          # staging: same rules as orders
    con = duckdb.connect(str(WAREHOUSE))
    updated = con.execute("select count(*) from stg_orders_inc "
                          "where order_id in (select order_id from stg_orders)").fetchone()[0]
    inserted = con.execute("select count(*) from stg_orders_inc "
                           "where order_id not in (select order_id from stg_orders)").fetchone()[0]
    con.close()
    run_sql("merge_orders_increment.sql", run_id)                  # MERGE on order_id
    return {"applied": True, "rows": rows, "inserted": inserted, "updated": updated}
