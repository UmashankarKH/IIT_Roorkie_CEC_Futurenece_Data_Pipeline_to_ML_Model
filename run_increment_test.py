import duckdb
from include.elt_raw import RAW_TABLES, load_raw
from include.elt_sql import run_sql
from include.nammamart_config import INCREMENT_FILE, WAREHOUSE

RUN = "inc1"
SEPT = "order_date < DATE '2026-10-01' AND order_status = 'DELIVERED' AND dq_status <> 'REJECTED'"

# 1) September load: raw + staging
for table, csv_name in RAW_TABLES.items():
    load_raw(table, csv_name, RUN)
for f in ["stg_stores", "stg_products", "stg_customers", "stg_orders", "stg_payments"]:
    run_sql(f + ".sql", RUN)

con = duckdb.connect(str(WAREHOUSE))
before_rows = con.execute("select count(*) from stg_orders").fetchone()[0]
before_sept = con.execute(f"select round(sum(order_amount),2) from stg_orders where {SEPT}").fetchone()[0]
con.close()

# 2) Raw: append the increment file exactly as received
print("increment rows appended to raw_orders:", load_raw("raw_orders", INCREMENT_FILE, RUN))

# 3) Staging: clean the increment, then MERGE on order_id (run twice to prove idempotency)
run_sql("stg_orders_inc.sql", RUN)
con = duckdb.connect(str(WAREHOUSE))
n_updates = con.execute("select count(*) from stg_orders_inc where order_id in (select order_id from stg_orders)").fetchone()[0]
n_inserts = con.execute("select count(*) from stg_orders_inc where order_id not in (select order_id from stg_orders)").fetchone()[0]
old_status = dict(con.execute("select order_id, order_status from stg_orders where order_id in (select order_id from stg_orders_inc)").fetchall())
con.close()

for i in (1, 2):
    run_sql("merge_orders_increment.sql", RUN)
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    total = con.execute("select count(*) from stg_orders").fetchone()[0]
    dups = con.execute("select count(*) - count(distinct order_id) from stg_orders where order_id <> ''").fetchone()[0]
    con.close()
    print(f"after MERGE #{i}: stg_orders rows={total}  duplicate order_ids={dups}")

con = duckdb.connect(str(WAREHOUSE), read_only=True)
print()
print(f"rows before={before_rows}  updated={n_updates}  inserted={n_inserts}  expected after={before_rows + n_inserts}")
new_status = dict(con.execute("select order_id, order_status from stg_orders").fetchall())
changes = [(k, v, new_status[k]) for k, v in old_status.items() if new_status[k] != v]
print("orders whose status changed:", len(changes))
for c in changes:
    print("   ", c)
after_sept = con.execute(f"select round(sum(order_amount),2) from stg_orders where {SEPT}").fetchone()[0]
print()
print(f"September net revenue  before={before_sept}  after={after_sept}  difference={round(after_sept - before_sept, 2)}")
