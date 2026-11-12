CREATE OR REPLACE TABLE mart_rejected_rows AS
SELECT 'orders' source_file,reject_reason,COUNT(*) rejected_rows FROM stg_orders WHERE NOT is_valid GROUP BY reject_reason
UNION ALL SELECT 'payments',reject_reason,COUNT(*) FROM stg_payments WHERE NOT is_valid GROUP BY reject_reason
UNION ALL SELECT 'customers',reject_reason,COUNT(*) FROM stg_customers WHERE NOT is_valid GROUP BY reject_reason
UNION ALL SELECT 'products',reject_reason,COUNT(*) FROM stg_products WHERE NOT is_valid GROUP BY reject_reason
UNION ALL SELECT 'stores',reject_reason,COUNT(*) FROM stg_stores WHERE NOT is_valid GROUP BY reject_reason;
