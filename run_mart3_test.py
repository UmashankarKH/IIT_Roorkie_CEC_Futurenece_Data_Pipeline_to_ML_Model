import duckdb
from include.elt_sql import run_sql
from include.nammamart_config import WAREHOUSE

for f in ["stg_stores.sql", "stg_products.sql", "stg_customers.sql", "stg_orders.sql",
          "stg_payments.sql", "mart_payment_reconciliation.sql"]:
    run_sql(f, "local1")

con = duckdb.connect(str(WAREHOUSE), read_only=True)
q = """
select coalesce(e.reason, m.reason) reason, e.n etl_rows, m.n elt_rows, e.n = m.n as same
from (select reason, count(*) n from etl_payment_reconciliation group by 1) e
full outer join (select reason, count(*) n from mart_payment_reconciliation group by 1) m
  on e.reason = m.reason
order by 1
"""
print(con.execute(q).df().to_string(index=False))
print()
for t in ["etl_payment_reconciliation", "mart_payment_reconciliation"]:
    r = con.execute(f"select count(*), round(sum(abs(difference)), 2) from {t}").fetchone()
    print(f"{t:30} rows={r[0]}  total not reconciling={r[1]}")