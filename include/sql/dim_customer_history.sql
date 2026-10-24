-- SCD Type 2 history of each customer's loyalty tier (bonus).
-- One row per version: valid_from / valid_to bound the period in which the
-- tier applied, and is_current marks the open version (valid_to 9999-12-31).
-- Run after stg_customers with two session variables set by
-- nammamart_elt.update_customer_history: scd_effective_at and scd_run_id.
-- Re-running with an unchanged snapshot changes nothing (idempotent).
CREATE TABLE IF NOT EXISTS dim_customer_history (
    customer_id VARCHAR,
    loyalty_tier VARCHAR,
    version INTEGER,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    run_id VARCHAR
);

-- New customers, and customers whose tier differs from their open version.
CREATE OR REPLACE TEMP TABLE scd_changes AS
SELECT s.customer_id, s.loyalty_tier, h.version AS current_version
FROM stg_customers s
LEFT JOIN dim_customer_history h ON h.customer_id = s.customer_id AND h.is_current
WHERE s.dq_status = 'PASS'
  AND (h.customer_id IS NULL OR h.loyalty_tier IS DISTINCT FROM s.loyalty_tier);

-- Close the open version of every changed customer...
UPDATE dim_customer_history
SET valid_to = getvariable('scd_effective_at'), is_current = false
WHERE is_current
  AND customer_id IN (SELECT customer_id FROM scd_changes WHERE current_version IS NOT NULL);

-- ...and open a new version (version 1 for a new customer).
INSERT INTO dim_customer_history
SELECT customer_id, loyalty_tier, coalesce(current_version, 0) + 1,
       getvariable('scd_effective_at'), TIMESTAMP '9999-12-31 00:00:00', true, getvariable('scd_run_id')
FROM scd_changes;
