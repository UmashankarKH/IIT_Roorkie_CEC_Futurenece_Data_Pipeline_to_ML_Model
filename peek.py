import duckdb

con = duckdb.connect("include/warehouse_anikesh.duckdb", read_only=True)
q = ("select order_id, order_ts, order_status, net_revenue "
     "from etl_fact_orders order by order_ts desc limit 5")
print("newest orders in the ETL fact table:")
print(con.execute(q).df().to_string(index=False))
