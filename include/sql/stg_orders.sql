CREATE OR REPLACE TABLE stg_orders AS
WITH typed AS (
 SELECT *, trim(order_id) AS order_id_n, try_cast(order_ts AS TIMESTAMP) AS order_ts_n,
        try_cast(quantity AS INTEGER) AS quantity_n, try_cast(unit_price AS DOUBLE) AS unit_price_n,
        try_cast(discount_pct AS DOUBLE) AS discount_pct_n, try_cast(order_amount AS DOUBLE) AS order_amount_n,
        try_cast(delivery_minutes AS DOUBLE) AS delivery_minutes_n,
        upper(trim(order_status)) AS status_n,
        CASE lower(trim(payment_mode))
          WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'CARD' WHEN 'wallet' THEN 'WALLET'
          WHEN 'cod' THEN 'CASH_ON_DELIVERY' WHEN 'cash on delivery' THEN 'CASH_ON_DELIVERY'
          WHEN 'cashh' THEN 'CASHH' ELSE upper(trim(payment_mode)) END AS payment_mode_n,
        row_number() OVER (PARTITION BY trim(order_id)
          ORDER BY CASE WHEN _source_file='orders_2026-10-01_increment.csv' THEN 0 ELSE 1 END, _loaded_at DESC) AS key_rank
 FROM raw_orders
), latest AS (
 SELECT * FROM typed WHERE key_rank=1
), checked AS (
 SELECT o.*,
        concat_ws(';',
          CASE WHEN nullif(trim(o.order_id_n),'') IS NULL THEN 'missing_order_id' END,
          CASE WHEN o.order_ts_n IS NULL THEN 'invalid_or_missing_order_ts' END,
          CASE WHEN o.quantity_n IS NULL OR o.quantity_n<=0 THEN 'non_positive_or_invalid_quantity' END,
          CASE WHEN o.discount_pct_n IS NULL OR o.discount_pct_n<0 OR o.discount_pct_n>100 THEN 'invalid_discount_pct' END,
          CASE WHEN o.status_n NOT IN ('DELIVERED','CANCELLED','RETURNED','PENDING','SHIPPED') THEN 'unexpected_order_status' END,
          CASE WHEN c.customer_id IS NULL THEN 'missing_or_unknown_customer' END,
          CASE WHEN p.product_id IS NULL THEN 'missing_or_unknown_product' END,
          CASE WHEN s.store_id IS NULL THEN 'missing_or_unknown_store' END
        ) AS failure_reason
 FROM latest o
 LEFT JOIN stg_customers c ON trim(o.customer_id)=c.customer_id
 LEFT JOIN stg_products p ON trim(o.product_id)=p.product_id
 LEFT JOIN stg_stores s ON trim(o.store_id)=s.store_id
)
SELECT order_id_n AS order_id, order_ts_n AS order_ts, cast(order_ts_n AS DATE) AS order_date,
       nullif(trim(customer_id),'') AS customer_id, nullif(trim(store_id),'') AS store_id,
       nullif(trim(product_id),'') AS product_id, quantity_n AS quantity, unit_price_n AS unit_price,
       discount_pct_n AS discount_pct, order_amount_n AS order_amount, payment_mode_n AS payment_mode,
       status_n AS order_status, nullif(trim(channel),'') AS channel, delivery_minutes_n AS delivery_minutes,
       CASE WHEN coalesce(failure_reason,'')='' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
       coalesce(failure_reason,'') AS failure_reason,
       _source_file, _run_id
FROM checked;

CREATE OR REPLACE TABLE stg_payments AS
WITH ranked AS (
 SELECT *, row_number() OVER (PARTITION BY trim(payment_id) ORDER BY _loaded_at DESC) rn
 FROM raw_payments WHERE nullif(trim(payment_id),'') IS NOT NULL
)
SELECT trim(payment_id) payment_id, trim(order_id) order_id, try_cast(payment_ts AS TIMESTAMP) payment_ts,
       CASE lower(trim(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'CARD'
         WHEN 'wallet' THEN 'WALLET' WHEN 'cod' THEN 'CASH_ON_DELIVERY'
         WHEN 'cash on delivery' THEN 'CASH_ON_DELIVERY' WHEN 'cashh' THEN 'CASHH'
         ELSE upper(trim(payment_mode)) END payment_mode,
       try_cast(amount_paid AS DOUBLE) amount_paid, upper(trim(payment_status)) payment_status,
       nullif(trim(gateway_ref),'') gateway_ref
FROM ranked WHERE rn=1;
