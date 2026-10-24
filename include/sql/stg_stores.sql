-- Cast store fields and validate the reporting attributes used by the marts.
CREATE OR REPLACE TABLE stg_stores AS
SELECT
    store_id,
    store_name,
    zone,
    area,
    pincode,
    try_cast(open_date AS DATE) AS open_date,
    manager_name,
    try_cast(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    CASE WHEN store_id <> '' AND zone <> '' AND try_cast(capacity_orders_per_day AS INTEGER) > 0
         THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    concat_ws('; ',
        CASE WHEN store_id = '' THEN 'Missing store_id' END,
        CASE WHEN zone = '' THEN 'Missing zone' END,
        CASE WHEN try_cast(capacity_orders_per_day AS INTEGER) <= 0 THEN 'Store capacity is not positive' END
    ) AS failure_reason
FROM raw_stores
-- rowid preserves CSV order, so the first source row wins exactly as in the ETL.
QUALIFY row_number() OVER (PARTITION BY store_id ORDER BY _loaded_at, rowid) = 1;
