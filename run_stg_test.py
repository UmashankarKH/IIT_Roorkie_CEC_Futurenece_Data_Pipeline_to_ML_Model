import duckdb
from include.elt_sql import run_sql
from include.nammamart_config import WAREHOUSE

for f in ["stg_stores.sql", "stg_products.sql", "stg_customers.sql", "stg_orders.sql", "stg_payments.sql"]:
    run_sql(f, "local1")

con = duckdb.connect(str(WAREHOUSE), read_only=True)
print("stg_payments rows:", con.execute("select count(*) from stg_payments").fetchone()[0])
print(con.execute("select dq_status, count(*) n from stg_payments group by 1 order by 1").df().to_string(index=False))
print("passed + warning:", con.execute("select count(*) from stg_payments where dq_status <> 'REJECTED'").fetchone()[0])
print()
print(con.execute("select failure_reason, count(*) n from stg_payments where dq_status = 'REJECTED' "
                  "group by 1 order by 2 desc").df().to_string(index=False))
print()
print(con.execute("select warning_reason, count(*) n from stg_payments where dq_status = 'WARNING' "
                  "group by 1 order by 2 desc").df().to_string(index=False))