-- Raw layer: an exact copy of each export, every column as text, nothing cleaned.
-- Casting and cleaning are decisions, and decisions belong downstream where they can
-- be seen and changed. Keeping raw as text also means a bad value never silently
-- becomes NULL at load time.
--
-- Re-loading the same file replaces that file's rows and nothing else, so the full
-- load is idempotent while the 1 Oct increment (a different _source_file) is left
-- alone. _loaded_at / _source_file / _run_id are what Part E's audit trail is built on.
--
-- Placeholders: {table} {path} {run_id} {source_file}

CREATE TABLE IF NOT EXISTS {table} AS
SELECT *, CAST(NULL AS TIMESTAMP) AS _loaded_at,
          CAST(NULL AS VARCHAR)   AS _source_file,
          CAST(NULL AS VARCHAR)   AS _run_id
FROM read_csv('{path}', all_varchar = true, header = true)
LIMIT 0;

DELETE FROM {table} WHERE _source_file = '{source_file}';

INSERT INTO {table}
SELECT *, now() AS _loaded_at, '{source_file}' AS _source_file, '{run_id}' AS _run_id
FROM read_csv('{path}', all_varchar = true, header = true);
