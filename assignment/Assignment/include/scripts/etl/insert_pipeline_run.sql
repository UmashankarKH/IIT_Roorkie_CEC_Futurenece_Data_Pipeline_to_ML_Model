INSERT INTO pipeline_runs
(run_id, dag_id, rows_read, rows_written, rows_rejected, status, recorded_at)
VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP);
