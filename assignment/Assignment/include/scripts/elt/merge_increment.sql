
CREATE OR REPLACE TABLE raw_orders_merged AS
WITH existing_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _loaded_at DESC NULLS LAST
        ) AS rn
    FROM raw_orders
    WHERE COALESCE(TRIM(CAST(order_id AS VARCHAR)), '') <> ''
),
existing_deduplicated AS (
    SELECT * EXCLUDE (rn)
    FROM existing_ranked
    WHERE rn = 1
),
incoming_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _loaded_at DESC NULLS LAST
        ) AS rn
    FROM increment_ranked
    WHERE COALESCE(TRIM(CAST(order_id AS VARCHAR)), '') <> ''
),
incoming_deduplicated AS (
    SELECT * EXCLUDE (rn)
    FROM incoming_ranked
    WHERE rn = 1
)
SELECT e.*
FROM existing_deduplicated e
WHERE NOT EXISTS (
    SELECT 1
    FROM incoming_deduplicated i
    WHERE CAST(i.order_id AS VARCHAR) = CAST(e.order_id AS VARCHAR)
)

UNION ALL BY NAME

SELECT *
FROM incoming_deduplicated;

CREATE OR REPLACE TABLE raw_orders AS
SELECT *
FROM raw_orders_merged;

DROP TABLE raw_orders_merged;
