import duckdb
from include.elt_raw import RAW_TABLES, load_raw
from include.nammamart_config import WAREHOUSE

for table, csv_name in RAW_TABLES.items():
    print(f"{table:14} loaded {load_raw(table, csv_name, 'local1')} rows")

con = duckdb.connect(str(WAREHOUSE), read_only=True)
print()
for table in RAW_TABLES:
    print(f"{table:14} total rows in table:", con.execute(f"select count(*) from {table}").fetchone()[0])
print()
print("raw_orders with blank customer_id:",
      con.execute("select count(*) from raw_orders where trim(customer_id) = ''").fetchone()[0])
print("raw_orders duplicate order_ids   :",
      con.execute("select count(*) - count(distinct order_id) from raw_orders").fetchone()[0])
print(con.execute("select * from raw_orders limit 2").df().T.to_string())