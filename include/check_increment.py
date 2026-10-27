"""Run inside the Airflow box:  python include/check_increment.py include/warehouse_chandini.duckdb
Shows that the increment file was merged correctly in BOTH pipelines (read-only, changes nothing)."""
import sys
import duckdb

con = duckdb.connect(sys.argv[1], read_only=True)


def show(title, sql):
    print(f"\n-- {title}")
    print(con.execute(sql).df().to_string(index=False))


show("1. ETL vs ELT totals (every row should say MATCH)", "select * from elt_vs_etl_comparison")
show("2. Clean orders and duplicate order_ids (duplicates must be 0)", """
 select 'ETL fact' as pipeline, count(*) as clean_orders, count(*) - count(distinct order_id) as duplicate_ids from etl_fact_orders
 union all
 select 'ELT stg_orders (PASS)', count(*), count(*) - count(distinct order_id) from stg_orders where dq_status = 'PASS'""")
show("3. ELT increment merge: corrected vs new orders (expected 10 updated, 150 inserted)", """
 with inc as (select order_id from stg_orders where dq_status = 'PASS' and _source_file <> 'orders.csv'),
      base as (select distinct order_id from raw_orders where _source_file = 'orders.csv')
 select count(*) filter (where order_id in (select order_id from base)) as updated_orders,
        count(*) filter (where order_id not in (select order_id from base)) as inserted_orders
 from inc""")
show("4. The corrected orders: status in the original file vs now (ELT)", """
 select o.order_id, r.order_status as status_in_orders_csv, o.order_status as status_after_increment, o.order_amount
 from stg_orders o
 join (select order_id, max(order_status) as order_status from raw_orders where _source_file = 'orders.csv' group by 1) r using (order_id)
 where o.dq_status = 'PASS' and o._source_file <> 'orders.csv' and r.order_status <> o.order_status
 order by 1""")
show("5. Net revenue by month after the increment (ETL and ELT; before it, all of September was 1135628.80)", """
 with e as (select strftime(order_date, '%Y-%m') as month, round(sum(net_revenue), 2) as etl_net_revenue from etl_fact_orders group by 1),
      l as (select strftime(order_date, '%Y-%m') as month, round(sum(net_revenue), 2) as elt_net_revenue from mart_daily_store_kpis group by 1)
 select * from e join l using (month) order by 1""")
show("6. Raw table keeps every version (ELT raw_orders rows per run)", """
 select _run_id as run_id, _source_file as source_file, count(*) as rows from raw_orders group by 1, 2 order by 1, 2""")
