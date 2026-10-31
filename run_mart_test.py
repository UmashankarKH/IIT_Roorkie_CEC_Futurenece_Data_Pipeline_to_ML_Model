import duckdb
from include.elt_sql import run_sql
from include.nammamart_config import WAREHOUSE

RUN = "local1"
for f in ["stg_stores.sql", "stg_products.sql", "stg_customers.sql", "stg_orders.sql",
          "stg_payments.sql", "mart_daily_store_kpis.sql", "mart_category_margin.sql"]:
    run_sql(f, RUN)

con = duckdb.connect(str(WAREHOUSE), read_only=True)
etl_rev = con.execute("select round(sum(net_revenue),2) from etl_fact_orders where order_status='DELIVERED'").fetchone()[0]
elt_rev = con.execute("select round(sum(net_revenue),2) from mart_daily_store_kpis").fetchone()[0]
etl_gm = con.execute("select round(sum(gross_margin),2) from etl_fact_orders").fetchone()[0]
elt_gm = con.execute("select round(sum(gross_margin),2) from mart_category_margin").fetchone()[0]
print(f"net revenue  ETL={etl_rev}  ELT={elt_rev}  match={etl_rev == elt_rev}")
print(f"gross margin ETL={etl_gm}  ELT={elt_gm}  match={etl_gm == elt_gm}")
print()
print(con.execute("select zone, round(sum(net_revenue),2) net_revenue from mart_daily_store_kpis "
                  "group by 1 order by 1").df().to_string(index=False))
print()
print(con.execute("select * from mart_category_margin order by category").df().to_string(index=False))