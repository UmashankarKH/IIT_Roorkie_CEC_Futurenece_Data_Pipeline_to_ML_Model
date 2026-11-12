CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id VARCHAR,
  dag_id VARCHAR,
  rows_read BIGINT,
  rows_written BIGINT,
  rows_rejected BIGINT,
  status VARCHAR,
  recorded_at TIMESTAMP
);
