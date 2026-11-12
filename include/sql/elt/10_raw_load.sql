-- Raw layer: exact copy of the CSV, every column as text, nothing cleaned, nothing dropped.
-- Extra audit columns: _row_num (line in the source file), _loaded_at, _source_file, _run_id.
-- Raw tables only grow. The single DELETE below only removes rows written earlier by the SAME run_id,
-- so a task retry after a half-finished load cannot leave duplicate rows behind.
CREATE TABLE IF NOT EXISTS {{table}} AS
SELECT t.*, CAST(NULL AS BIGINT) AS _row_num, CAST(NULL AS TIMESTAMP) AS _loaded_at,
       CAST(NULL AS VARCHAR) AS _source_file, CAST(NULL AS VARCHAR) AS _run_id
FROM read_csv('{{path}}', all_varchar = true, header = true, nullstr = '<<no-null-marker>>', strict_mode = false) t
LIMIT 0;

DELETE FROM {{table}} WHERE _run_id = '{{run_id}}' AND _source_file = '{{source_file}}';

INSERT INTO {{table}} BY NAME
SELECT t.*, row_number() OVER () + 1 AS _row_num, now() AS _loaded_at,
       '{{source_file}}' AS _source_file, '{{run_id}}' AS _run_id
FROM read_csv('{{path}}', all_varchar = true, header = true, nullstr = '<<no-null-marker>>', strict_mode = false, parallel = false) t;
