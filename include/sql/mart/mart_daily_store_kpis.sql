-- Daily revenue and delivery KPIs per store. This is the table behind the COO's
-- "revenue by zone per day" question.
--
-- Cancelled and returned orders are excluded from revenue and from the delivery SLA,
-- but they are still counted separately so a store with a returns problem is visible.
-- The SLA denominator is delivered orders only - counting a cancelled order against
-- the 15-minute promise made the ELT number disagree with the ETL one, which is how
-- I noticed it.

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    order_date,
    zone,
    store_id,
    store_name,
    count(*)                                             AS orders_total,
    count_if(is_revenue)                                 AS delivered_orders,
    count_if(order_status = 'CANCELLED')                 AS cancelled_orders,
    count_if(order_status = 'RETURNED')                  AS returned_orders,
    round(sum(revenue_amount), 2)                        AS net_revenue,
    round(sum(margin_amount), 2)                         AS gross_margin,
    round(avg(CASE WHEN is_revenue THEN delivery_minutes END), 2)
                                                         AS avg_delivery_minutes,
    count_if(is_revenue AND delivery_minutes IS NOT NULL) AS sla_orders,
    count_if(is_revenue AND within_15_min)               AS within_sla,
    round(100.0 * count_if(is_revenue AND within_15_min)
          / nullif(count_if(is_revenue AND delivery_minutes IS NOT NULL), 0), 2)
                                                         AS pct_within_15_min
FROM mart_fact_orders
GROUP BY ALL
ORDER BY order_date, zone, store_id;
