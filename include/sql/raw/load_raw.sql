-- Raw layer: exact copy of a source CSV, every column as text, plus audit columns.
-- Append-only. The DELETE only removes rows written by an earlier attempt of the
-- SAME run and file (Airflow retry / Clear), so a retry never double-loads.
-- INSERT ... BY NAME fails loudly if the file's columns change (schema drift).
-- Placeholders: {{table}} {{path}} {{source_file}} {{run_id}}

CREATE TABLE IF NOT EXISTS {{table}} AS
SELECT *,
       CAST(NULL AS TIMESTAMP) AS _loaded_at,
       CAST(NULL AS VARCHAR)   AS _source_file,
       CAST(NULL AS VARCHAR)   AS _run_id,
       CAST(NULL AS BIGINT)    AS _row_num
FROM read_csv({{path}}, header = true, all_varchar = true, delim = ',', quote = '"')
LIMIT 0;

DELETE FROM {{table}} WHERE _run_id = {{run_id}} AND _source_file = {{source_file}};

INSERT INTO {{table}} BY NAME
SELECT *,
       current_timestamp::TIMESTAMP AS _loaded_at,
       {{source_file}}              AS _source_file,
       {{run_id}}                   AS _run_id,
       row_number() OVER ()         AS _row_num
FROM read_csv({{path}}, header = true, all_varchar = true, delim = ',', quote = '"');
