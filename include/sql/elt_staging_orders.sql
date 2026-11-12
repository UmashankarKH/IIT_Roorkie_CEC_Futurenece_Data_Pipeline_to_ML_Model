CREATE OR REPLACE TABLE stg_orders AS
          SELECT *, try_cast(order_ts AS TIMESTAMP) order_ts_cast,
          try_cast(quantity AS INTEGER) quantity_cast,
          try_cast(unit_price AS DOUBLE) unit_price_cast,
          try_cast(discount_pct AS DOUBLE) discount_pct_cast,
          try_cast(order_amount AS DOUBLE) order_amount_cast,
          try_cast(delivery_minutes AS DOUBLE) delivery_minutes_cast,
          upper(trim(order_status)) order_status_std,
          CASE
            WHEN coalesce(trim(order_id),'')='' THEN 'Missing order_id'
            WHEN coalesce(trim(customer_id),'')='' THEN 'Missing customer_id'
            WHEN coalesce(trim(product_id),'')='' THEN 'Missing product_id'
            WHEN coalesce(trim(store_id),'')='' THEN 'Missing store_id'
            WHEN try_cast(order_ts AS TIMESTAMP) IS NULL THEN 'Invalid timestamp'
            WHEN try_cast(order_ts AS TIMESTAMP)>current_timestamp THEN 'Future timestamp'
            WHEN try_cast(quantity AS INTEGER) IS NULL OR try_cast(quantity AS INTEGER)<=0 THEN 'Quantity must be positive'
            WHEN try_cast(unit_price AS DOUBLE) IS NULL OR try_cast(unit_price AS DOUBLE)<=0 THEN 'Invalid unit price'
            WHEN try_cast(discount_pct AS DOUBLE) IS NULL OR try_cast(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100 THEN 'Invalid discount'
            WHEN try_cast(order_amount AS DOUBLE) IS NULL THEN 'Missing/invalid order amount'
            WHEN abs(try_cast(order_amount AS DOUBLE)-try_cast(quantity AS DOUBLE)*try_cast(unit_price AS DOUBLE)*(1-try_cast(discount_pct AS DOUBLE)/100))>0.02 THEN 'Amount mismatch'
            WHEN upper(trim(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'Invalid status'
            WHEN NOT EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id=raw_orders.customer_id AND c.dq_status <> 'FAIL') THEN 'Unknown customer'
            WHEN NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id=raw_orders.product_id AND p.dq_status <> 'FAIL') THEN 'Unknown product'
            WHEN NOT EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id=raw_orders.store_id AND s.dq_status <> 'FAIL') THEN 'Unknown store'
            WHEN upper(trim(order_status))='DELIVERED' AND try_cast(delivery_minutes AS DOUBLE) IS NULL THEN 'Missing delivery time'
            WHEN upper(trim(order_status))='DELIVERED' AND try_cast(delivery_minutes AS DOUBLE)>=120 THEN 'Delivery time too long'
            ELSE NULL END failure_reason,
          CASE WHEN coalesce(trim(order_id),'')='' OR coalesce(trim(customer_id),'')='' OR coalesce(trim(product_id),'')='' OR coalesce(trim(store_id),'')=''
            OR try_cast(order_ts AS TIMESTAMP) IS NULL OR try_cast(order_ts AS TIMESTAMP)>current_timestamp
            OR try_cast(quantity AS INTEGER) IS NULL OR try_cast(quantity AS INTEGER)<=0
            OR try_cast(unit_price AS DOUBLE) IS NULL OR try_cast(unit_price AS DOUBLE)<=0
            OR try_cast(discount_pct AS DOUBLE) IS NULL OR try_cast(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100
            OR try_cast(order_amount AS DOUBLE) IS NULL
            OR abs(try_cast(order_amount AS DOUBLE)-try_cast(quantity AS DOUBLE)*try_cast(unit_price AS DOUBLE)*(1-try_cast(discount_pct AS DOUBLE)/100))>0.02
            OR upper(trim(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED')
            OR NOT EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id=raw_orders.customer_id AND c.dq_status <> 'FAIL')
            OR NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id=raw_orders.product_id AND p.dq_status <> 'FAIL')
            OR NOT EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id=raw_orders.store_id AND s.dq_status <> 'FAIL')
            OR (upper(trim(order_status))='DELIVERED' AND (try_cast(delivery_minutes AS DOUBLE) IS NULL OR try_cast(delivery_minutes AS DOUBLE)>=120))
            THEN 'FAIL' ELSE 'PASS' END dq_status
          FROM (SELECT * FROM raw_orders QUALIFY row_number() OVER(PARTITION BY order_id ORDER BY _loaded_at DESC)=1) raw_orders
