-- Delivery SLA by zone and channel. The promise is 15 minutes, so the headline
-- number is the share of delivered orders that actually made it inside 15.
-- p90 matters more than the average here: the average hides the bad tail.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    zone,
    channel,
    count(*)                                              AS delivered_orders,
    round(avg(delivery_minutes), 1)                       AS avg_delivery_minutes,
    round(median(delivery_minutes), 1)                    AS median_delivery_minutes,
    round(quantile_cont(delivery_minutes, 0.90), 1)       AS p90_delivery_minutes,
    max(delivery_minutes)                                 AS worst_delivery_minutes,
    count_if(within_15_min)                               AS within_sla,
    count_if(NOT within_15_min)                           AS breached_sla,
    round(100.0 * count_if(within_15_min) / count(*), 1)  AS pct_within_15_min
FROM mart_fact_orders
WHERE is_revenue AND delivery_minutes IS NOT NULL
GROUP BY ALL
ORDER BY pct_within_15_min;
