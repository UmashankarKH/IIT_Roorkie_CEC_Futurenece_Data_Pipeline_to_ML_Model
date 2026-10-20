
CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY
                CASE WHEN COALESCE(TRIM(full_name), '') <> '' THEN 1 ELSE 0 END DESC,
                COALESCE(TRIM(phone), '') DESC,
                COALESCE(TRIM(area), '') DESC
        ) AS rn
    FROM raw_customers
),
deduplicated AS (
    SELECT * EXCLUDE (rn)
    FROM ranked
    WHERE rn = 1
),
checked AS (
    SELECT *,
        CASE
            WHEN COALESCE(TRIM(customer_id), '') = ''
                THEN 'missing_customer_id'
            WHEN COALESCE(TRIM(full_name), '') = ''
                THEN 'missing_full_name'
            ELSE NULL
        END AS reject_reason
    FROM deduplicated
)
SELECT *,
    UPPER(TRIM(loyalty_tier)) AS loyalty_tier_clean,
    UPPER(TRIM(is_active)) AS is_active_clean,
    (reject_reason IS NULL) AS is_valid
FROM checked;
