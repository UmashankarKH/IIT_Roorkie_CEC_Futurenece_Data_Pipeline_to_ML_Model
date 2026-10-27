-- Do the ELT marts match the ETL tables? (Run both DAGs with the same load_increment setting first.)
CREATE OR REPLACE TABLE elt_vs_etl_comparison AS
SELECT metric, etl_value, elt_value, round(elt_value - etl_value, 2) AS difference,
       CASE WHEN abs(elt_value - etl_value) < 0.05 THEN 'MATCH' ELSE 'DIFFERENT' END AS status
FROM (
  SELECT 'delivered_orders' AS metric,
         (SELECT count(*) FROM etl_fact_orders WHERE order_status = 'DELIVERED') * 1.0 AS etl_value,
         (SELECT sum(delivered_orders) FROM mart_daily_store_kpis) * 1.0 AS elt_value
  UNION ALL
  SELECT 'net_revenue', (SELECT sum(net_revenue) FROM etl_fact_orders), (SELECT sum(net_revenue) FROM mart_daily_store_kpis)
  UNION ALL
  SELECT 'gross_margin', (SELECT sum(gross_margin) FROM etl_fact_orders), (SELECT sum(gross_margin) FROM mart_daily_store_kpis)
  UNION ALL
  SELECT 'category_revenue_total', (SELECT sum(revenue) FROM etl_category_margin), (SELECT sum(revenue) FROM mart_category_margin)
  UNION ALL
  SELECT 'orders_in_clean_tables', (SELECT count(*) FROM etl_fact_orders) * 1.0, (SELECT sum(orders_total) FROM mart_daily_store_kpis) * 1.0
  UNION ALL
  SELECT 'reconciliation_rows', (SELECT count(*) FROM etl_payment_reconciliation) * 1.0, (SELECT count(*) FROM mart_payment_reconciliation) * 1.0
  UNION ALL
  SELECT 'unreconciled_amount', (SELECT sum(unreconciled_amount) FROM etl_payment_reconciliation), (SELECT sum(unreconciled_amount) FROM mart_payment_reconciliation)
) t;
