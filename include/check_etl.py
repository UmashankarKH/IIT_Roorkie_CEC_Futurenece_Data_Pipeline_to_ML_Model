"""Quick health check of the ETL warehouse. Usage: python include/check_etl.py include/warehouse_chandini.duckdb"""
import sys

import duckdb
import pandas as pd

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 200)
con = duckdb.connect(sys.argv[1], read_only=True)
print("TABLES AND ROW COUNTS")
for (t,) in con.execute("SHOW TABLES").fetchall():
    print(f"  {t:32} {con.execute(f'select count(*) from {t}').fetchone()[0]}")
print("\nPIPELINE RUNS (latest 5)")
print(con.execute("select * from pipeline_runs order by start_time desc limit 5").df().to_string(index=False))
print("\nDQ SCORECARD, latest run: checks by dimension and status")
print(con.execute("""select dimension, status, count(*) as checks, sum(rows_failed) as rows_failed
                     from dq_results
                     where run_id = (select run_id from pipeline_runs where dag_id like '%etl%' order by start_time desc limit 1)
                       and dag_id like 'nammamart_etl%'
                     group by dimension, status order by dimension, status""").df().to_string(index=False))
