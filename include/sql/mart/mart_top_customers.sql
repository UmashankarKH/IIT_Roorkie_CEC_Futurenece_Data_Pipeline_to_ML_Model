-- Who the repeat spenders are, by loyalty tier. Names and contact details are the
-- masked versions from staging; this table is safe to hand to the marketing team.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    f.customer_id,
    c.customer_name_masked,
    c.city,
    f.loyalty_tier,
    count(*)                                        AS delivered_orders,
    round(sum(f.revenue_amount), 2)                 AS lifetime_revenue,
    round(avg(f.revenue_amount), 2)                 AS avg_order_value,
    round(sum(f.margin_amount), 2)                  AS lifetime_margin,
    min(f.order_date)                               AS first_order_date,
    max(f.order_date)                               AS last_order_date
FROM mart_fact_orders f
LEFT JOIN mart_dim_customer c ON c.customer_id = f.customer_id
WHERE f.is_revenue
GROUP BY ALL
HAVING count(*) > 1
ORDER BY lifetime_revenue DESC;
