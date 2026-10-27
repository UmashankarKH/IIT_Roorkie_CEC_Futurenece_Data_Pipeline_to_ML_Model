-- RAW layer: an exact copy of one CSV file, every column as text, NEVER cleaned.
-- Rows are only ever appended (one batch per run_id), so raw is the permanent audit trail.
-- Placeholders filled in by the DAG: {{table}} {{path}} {{source_file}} {{run_id}} {{loaded_at}}
CREATE TABLE IF NOT EXISTS raw_{{table}} AS
SELECT *, CAST(NULL AS TIMESTAMP) AS _loaded_at, CAST(NULL AS VARCHAR) AS _source_file,
       CAST(NULL AS VARCHAR) AS _run_id, CAST(NULL AS BIGINT) AS _source_row
FROM read_csv('{{path}}', header = true, all_varchar = true, nullstr = '<<NULL>>')
LIMIT 0;

-- re-running the same task for the same run replaces only that run's batch
DELETE FROM raw_{{table}} WHERE _run_id = '{{run_id}}' AND _source_file = '{{source_file}}';

INSERT INTO raw_{{table}}
SELECT *, CAST('{{loaded_at}}' AS TIMESTAMP), '{{source_file}}', '{{run_id}}', row_number() OVER () + 1
FROM read_csv('{{path}}', header = true, all_varchar = true, nullstr = '<<NULL>>');
