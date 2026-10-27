"""Print the result of every query in a .sql file.
Usage (inside `astro dev bash`):  python include/run_queries.py include/warehouse_chandini.duckdb include/sql/etl_business_questions.sql
"""
import sys

import duckdb
import pandas as pd

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 200)
con = duckdb.connect(sys.argv[1], read_only=True)
text = open(sys.argv[2], encoding="utf-8").read()
for block in [b.strip() for b in text.split(";") if b.strip()]:
    title = next((ln for ln in block.splitlines() if ln.startswith("--")), "-- query")
    print("\n" + title)
    print(con.execute(block).df().to_string(index=False))
