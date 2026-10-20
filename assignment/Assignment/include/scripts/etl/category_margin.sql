CREATE OR REPLACE TABLE etl_category_margin AS
SELECT category,
       SUM(net_revenue) AS revenue,
       SUM(net_revenue) - SUM(gross_margin) AS cost,
       SUM(gross_margin) AS gross_margin
FROM etl_fact_orders
GROUP BY category;
