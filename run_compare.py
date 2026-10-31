import duckdb

con = duckdb.connect("include/warehouse_anikesh.duckdb", read_only=True)
q = lambda s: con.execute(s).fetchone()[0]

zone_diff = q("""
    select count(*) from
      (select zone, round(sum(net_revenue), 2) r from etl_daily_zone_revenue group by 1) e
      full outer join
      (select zone, round(sum(net_revenue), 2) r from mart_daily_store_kpis group by 1) m using (zone)
    where e.r is distinct from m.r""")

checks = [
    ("net revenue (delivered)",
     q("select round(sum(net_revenue),2) from etl_fact_orders where order_status='DELIVERED'"),
     q("select round(sum(net_revenue),2) from mart_daily_store_kpis")),
    ("gross margin",
     q("select round(sum(gross_margin),2) from etl_fact_orders"),
     q("select round(sum(gross_margin),2) from mart_category_margin")),
    ("clean orders",
     q("select count(*) from etl_fact_orders"),
     q("select count(*) from stg_orders where dq_status <> 'REJECTED'")),
    ("delivered orders",
     q("select count(*) from etl_fact_orders where order_status='DELIVERED'"),
     q("select sum(delivered_orders) from mart_daily_store_kpis")),
    ("orders within 15 min",
     q("select count(*) from etl_fact_orders where within_15_min"),
     q("select sum(orders_within_15_min) from mart_daily_store_kpis")),
    ("reconciliation rows",
     q("select count(*) from etl_payment_reconciliation"),
     q("select count(*) from mart_payment_reconciliation")),
    ("amount not reconciling",
     q("select round(sum(abs(difference)),2) from etl_payment_reconciliation"),
     q("select round(sum(abs(difference)),2) from mart_payment_reconciliation")),
    ("zones with different revenue", zone_diff, 0),
]

print(f"{'check':32}{'ETL':>14}{'ELT':>14}  result")
for name, etl, elt in checks:
    ok = abs(float(etl) - float(elt)) < 0.01
    print(f"{name:32}{etl:>14}{elt:>14}  {'MATCH' if ok else 'DIFFERENT'}")

print()
etl_q = q("select max(rows_quarantined) from pipeline_runs where dag_id like '%etl%' "
          "and start_time = (select max(start_time) from pipeline_runs where dag_id like '%etl%')")
elt_q = q("select max(rows_quarantined) from pipeline_runs where dag_id like '%elt%' "
          "and start_time = (select max(start_time) from pipeline_runs where dag_id like '%elt%')")
print(f"rows set aside in latest run:  ETL quarantined={etl_q}   ELT rejected in staging={elt_q}   difference={etl_q - elt_q}")
