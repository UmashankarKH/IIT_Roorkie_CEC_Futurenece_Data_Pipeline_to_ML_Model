CREATE OR REPLACE TABLE {table} AS SELECT * FROM {table}_merged;
DROP TABLE {table}_merged;
