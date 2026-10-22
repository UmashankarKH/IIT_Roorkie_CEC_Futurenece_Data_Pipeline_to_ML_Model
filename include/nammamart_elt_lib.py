"""ELT helpers: run the .sql files in include/sql/elt with {{placeholders}} filled in."""
import re

import nammamart_common as nc

RAW_TABLES = {"orders": "raw_orders", "payments": "raw_payments", "customers": "raw_customers",
              "products": "raw_products", "stores": "raw_stores", "orders_increment": "raw_orders"}


def run_sql_file(con, name, **params):
    sql = (nc.SQL_DIR / name).read_text(encoding="utf-8")
    for k, v in params.items():
        sql = sql.replace("{{" + k + "}}", str(v))
    left = re.findall(r"\{\{(\w+)\}\}", sql)
    if left:
        raise ValueError(f"{name}: unfilled placeholders {left}")
    con.execute(sql)


def load_raw(con, source, run_id):
    path = nc.file_path(source)
    run_sql_file(con, "10_raw_load.sql", table=RAW_TABLES[source], path=path.as_posix(),
                 source_file=path.name, run_id=run_id)
    return con.execute(f"SELECT count(*) FROM {RAW_TABLES[source]} WHERE _run_id = ? AND _source_file = ?",
                       [run_id, path.name]).fetchone()[0]
