CREATE OR REPLACE TABLE mart_etl_comparison AS
WITH etl AS (
    SELECT
        COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_order_count,
        ROUND(SUM(net_revenue) FILTER (WHERE order_status = 'DELIVERED'), 2) AS september_revenue,
        ROUND(SUM(gross_margin) FILTER (WHERE order_status = 'DELIVERED'), 2) AS september_gross_margin
    FROM etl_db.etl_fact_orders
    WHERE order_date >= DATE '2026-09-01' AND order_date < DATE '2026-10-01'
), elt AS (
    SELECT
        COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_order_count,
        ROUND(SUM(order_amount) FILTER (WHERE order_status = 'DELIVERED'), 2) AS september_revenue,
        ROUND(SUM(order_amount - quantity * p.cost_price) FILTER (WHERE order_status = 'DELIVERED'), 2) AS september_gross_margin
    FROM stg_orders o
    JOIN stg_products p USING (product_id)
    WHERE o.dq_status <> 'FAIL'
      AND p.dq_status <> 'FAIL'
      AND o.order_date >= DATE '2026-09-01' AND o.order_date < DATE '2026-10-01'
), late_returns AS (
    SELECT COUNT(*) AS changed_to_returned,
           ROUND(SUM(e.net_revenue), 2) AS revenue_removed,
           ROUND(SUM(e.gross_margin), 2) AS gross_margin_removed
    FROM etl_db.etl_fact_orders e
    JOIN stg_orders s USING (order_id)
    WHERE e.order_date >= DATE '2026-09-01' AND e.order_date < DATE '2026-10-01'
      AND e.order_status = 'DELIVERED'
      AND s.order_status = 'RETURNED'
      AND s._source_file = 'orders_2026-10-01_increment.csv'
      AND s.dq_status <> 'FAIL'
)
SELECT
    etl.delivered_order_count AS etl_delivered_orders,
    elt.delivered_order_count AS elt_delivered_orders,
    etl.september_revenue AS etl_september_revenue,
    elt.september_revenue AS elt_september_revenue,
    ROUND(elt.september_revenue - etl.september_revenue, 2) AS revenue_delta,
    etl.september_gross_margin AS etl_september_gross_margin,
    elt.september_gross_margin AS elt_september_gross_margin,
    ROUND(elt.september_gross_margin - etl.september_gross_margin, 2) AS gross_margin_delta,
    late_returns.changed_to_returned AS late_return_updates,
    late_returns.revenue_removed AS revenue_restated_by_late_returns,
    ROUND((elt.september_revenue - etl.september_revenue) + late_returns.revenue_removed, 2) AS revenue_delta_not_explained_by_late_returns,
    late_returns.gross_margin_removed AS gross_margin_restated_by_late_returns,
    ROUND((elt.september_gross_margin - etl.september_gross_margin) + late_returns.gross_margin_removed, 2) AS gross_margin_delta_not_explained_by_late_returns,
    CASE
        WHEN ABS((elt.september_revenue - etl.september_revenue) + late_returns.revenue_removed) <= 0.01
         AND ABS((elt.september_gross_margin - etl.september_gross_margin) + late_returns.gross_margin_removed) <= 0.01
        THEN 'ELT restated the October 1 RETURNED corrections; after accounting for those late returns, September revenue and margin reconcile to the ETL snapshot.'
        ELSE 'Totals differ beyond the late-return restatement; investigate the validation rules and source versions.'
    END AS comparison_note
FROM etl CROSS JOIN elt CROSS JOIN late_returns;
