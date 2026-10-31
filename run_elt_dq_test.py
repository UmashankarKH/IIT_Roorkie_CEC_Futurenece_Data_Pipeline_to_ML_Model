import duckdb
from include.elt_dq import write_elt_dq
from include.elt_sql import run_sql
from include.nammamart_config import ELT_DAG_ID, ETL_DAG_ID, WAREHOUSE

RUN = "local1"
for f in ["stg_stores.sql", "stg_products.sql", "stg_customers.sql", "stg_orders.sql", "stg_payments.sql"]:
    run_sql(f, RUN)
print("ELT checks recorded:", len(write_elt_dq(RUN)))

con = duckdb.connect(str(WAREHOUSE), read_only=True)
q = f"""
with etl as (select check_name, rows_failed from dq_results where dag_id = '{ETL_DAG_ID}'
             and run_id = (select run_id from dq_results where dag_id = '{ETL_DAG_ID}'
                           order by checked_at desc limit 1)),
elt as (select check_name, rows_failed from dq_results where dag_id = '{ELT_DAG_ID}' and run_id = '{RUN}')
select coalesce(etl.check_name, elt.check_name) check_name, etl.rows_failed etl_failed, elt.rows_failed elt_failed
from etl full outer join elt using (check_name)
where etl.rows_failed is distinct from elt.rows_failed order by 1
"""
diff = con.execute(q).df()
print()
print("checks where ETL and ELT disagree:", len(diff))
print(diff.to_string(index=False))
print()
print(con.execute(f"select dimension, count(*) checks from dq_results where dag_id = '{ELT_DAG_ID}' "
                  f"group by 1 order by 1").df().to_string(index=False))