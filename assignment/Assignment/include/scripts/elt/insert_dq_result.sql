INSERT INTO dq_results
(run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP);
