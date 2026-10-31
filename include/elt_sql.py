"""Runs a .sql file from include/sql against the warehouse. {{run_id}} is filled in."""
import duckdb

from include.nammamart_config import SQL_DIR, WAREHOUSE


def run_sql(filename, run_id):
    sql = (SQL_DIR / filename).read_text().replace("{{run_id}}", run_id.replace("'", "''"))
    con = duckdb.connect(str(WAREHOUSE))
    con.execute(sql)
    con.close()