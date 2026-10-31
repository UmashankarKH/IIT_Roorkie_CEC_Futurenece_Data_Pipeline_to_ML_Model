import duckdb
import pandas as pd
from include.etl_incremental import apply_increment
from include.etl_load import load_dim, load_fact
from include.etl_steps import prepare
from include.nammamart_config import WAREHOUSE

NOW = pd.Timestamp.now()
RUN = "incetl1"
SEPT = "order_date < DATE '2026-10-01' AND order_status = 'DELIVERED'"

out = prepare(NOW, "etl_inc_test", RUN)
load_dim(out["dim_product"], "etl_dim_product")
print("base fact rows loaded:", load_fact(out["fact"]))

con = duckdb.connect(str(WAREHOUSE), read_only=True)
before_sept = con.execute(f"select round(sum(net_revenue),2) from etl_fact_orders where {SEPT}").fetchone()[0]
old_status = dict(con.execute("select order_id, order_status from etl_fact_orders").fetchall())
con.close()

for i in (1, 2):
    r = apply_increment(NOW, "etl_inc_test", RUN)
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    total = con.execute("select count(*) from etl_fact_orders").fetchone()[0]
    dups = con.execute("select count(*) - count(distinct order_id) from etl_fact_orders").fetchone()[0]
    con.close()
    print(f"apply #{i}: increment rows={r['rows']} clean={r['passed']} quarantined={r['quarantined']} "
          f"updated={r['updated']} inserted={r['inserted']} | fact rows={total} duplicates={dups}")

con = duckdb.connect(str(WAREHOUSE), read_only=True)
new_status = dict(con.execute("select order_id, order_status from etl_fact_orders").fetchall())
changes = [(k, v, new_status[k]) for k, v in old_status.items() if new_status[k] != v]
print()
print("orders whose status changed:", len(changes))
for c in changes:
    print("   ", c)
after_sept = con.execute(f"select round(sum(net_revenue),2) from etl_fact_orders where {SEPT}").fetchone()[0]
print()
print(f"September net revenue  before={before_sept}  after={after_sept}  difference={round(after_sept - before_sept, 2)}")
