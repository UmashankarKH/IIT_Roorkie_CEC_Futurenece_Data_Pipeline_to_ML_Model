CREATE TABLE IF NOT EXISTS dq_results (
 run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
 severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
 checked_at TIMESTAMP
);
