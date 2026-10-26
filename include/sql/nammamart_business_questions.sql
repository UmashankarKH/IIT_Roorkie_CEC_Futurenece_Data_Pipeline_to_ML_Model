-- 1) Daily net revenue by zone and store (delivered orders only, after discount)
SELECT
    zone,
    store_id,
    store_name,
    ROUND(SUM(net_revenue), 2) AS net_revenue
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY zone, store_id, store_name
ORDER BY zone, store_name;

-- 2) Revenue and gross margin by product category (margin = revenue - quantity * cost_price)
SELECT
    category,
    ROUND(SUM(net_revenue), 2) AS revenue,
    ROUND(SUM(gross_margin), 2) AS gross_margin
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY category
ORDER BY revenue DESC;

-- 3) Average delivery time per store and % of delivered orders within 15 minutes
SELECT
    store_id,
    store_name,
    ROUND(AVG(delivery_minutes), 2) AS avg_delivery_minutes,
    ROUND(100.0 * AVG(CASE WHEN within_15_min THEN 1.0 ELSE 0.0 END), 2) AS pct_within_15_min
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY store_id, store_name
ORDER BY avg_delivery_minutes;

-- 4) Revenue by payment mode and total amount that does not reconcile with the payment gateway
SELECT
    payment_mode,
    ROUND(SUM(net_revenue), 2) AS revenue
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY revenue DESC;

SELECT
    ROUND(SUM(CASE WHEN reason IS NOT NULL THEN ABS(amount_paid - order_amount) ELSE 0 END), 2) AS unreconciled_amount
FROM etl_payment_reconciliation;

-- 5) Top 10 customers by spend, with loyalty tier (no raw email or phone shown)
SELECT
    customer_id,
    full_name,
    loyalty_tier,
    ROUND(SUM(net_revenue), 2) AS total_spend
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY customer_id, full_name, loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;

-- 6) Number of rejected rows today by file and by reason
SELECT
    file_name,
    failure_reason,
    COUNT(*) AS rejected_rows
FROM read_csv_auto('include/quarantine/jayaprakash/*.csv', union_by_name = true)
GROUP BY file_name, failure_reason
ORDER BY rejected_rows DESC;
