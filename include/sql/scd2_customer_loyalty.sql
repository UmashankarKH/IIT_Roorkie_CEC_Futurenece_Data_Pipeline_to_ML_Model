CREATE TABLE IF NOT EXISTS dim_customer_loyalty_history (
    customer_id VARCHAR,
    loyalty_tier VARCHAR,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    source_run_id VARCHAR
);

UPDATE dim_customer_loyalty_history AS history
SET valid_to = CURRENT_TIMESTAMP,
    is_current = FALSE
WHERE history.is_current
  AND EXISTS (
      SELECT 1
      FROM stg_customers AS current_customer
      WHERE current_customer.customer_id = history.customer_id
        AND current_customer.dq_status <> 'FAIL'
        AND current_customer.loyalty_tier IS DISTINCT FROM history.loyalty_tier
  );

INSERT INTO dim_customer_loyalty_history
    (customer_id, loyalty_tier, valid_from, valid_to, is_current, source_run_id)
SELECT customer.customer_id,
       customer.loyalty_tier,
       CURRENT_TIMESTAMP,
       NULL,
       TRUE,
       customer._run_id
FROM stg_customers AS customer
LEFT JOIN dim_customer_loyalty_history AS history
  ON history.customer_id = customer.customer_id
 AND history.is_current
WHERE customer.dq_status <> 'FAIL'
  AND (history.customer_id IS NULL
       OR customer.loyalty_tier IS DISTINCT FROM history.loyalty_tier);
