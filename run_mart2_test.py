import duckdb
from include.elt_sql import run_sql
from include.nammamart_config import WAREHOUSE

for f in ["mart_delivery_sla.sql", "mart_top_customers.sql"]:
    run_sql(f, "local1")

con = duckdb.connect(str(WAREHOUSE), read_only=True)

orders = con.execute("select sum(delivered_orders), sum(orders_within_15_min) from mart_delivery_sla").fetchone()
print(f"ELT % delivered within 15 min: {round(100 * orders[1] / orders[0], 1)}   (ETL was 28.1)")
print()
print(con.execute("select * from mart_delivery_sla order by avg_delivery_minutes limit 5").df().to_string(index=False))
print()

etl_top = [r[0] for r in con.execute(
    "select f.customer_id from etl_fact_orders f join etl_dim_customer c using (customer_id) "
    "where f.order_status = 'DELIVERED' group by 1 order by sum(f.order_amount) desc, 1 limit 10").fetchall()]
elt_top = [r[0] for r in con.execute("select customer_id from mart_top_customers order by spend_rank").fetchall()]
print("top 10 customers match ETL:", etl_top == elt_top)
print()
print(con.execute("select * from mart_top_customers order by spend_rank").df().to_string(index=False))