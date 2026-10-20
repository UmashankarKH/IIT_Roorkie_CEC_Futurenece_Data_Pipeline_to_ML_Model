CREATE OR REPLACE TABLE increment_ranked AS
SELECT * EXCLUDE (rn) FROM (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id
 ORDER BY order_ts_typed DESC NULLS LAST, _loaded_at DESC) rn
 FROM increment_typed WHERE COALESCE(TRIM(order_id),'')<>''
) WHERE rn=1;
