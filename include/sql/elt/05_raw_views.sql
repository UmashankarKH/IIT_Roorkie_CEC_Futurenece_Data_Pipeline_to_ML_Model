-- Latest load of each source file. Raw tables only ever grow; staging reads the most recent load per file.
CREATE OR REPLACE VIEW v_latest_raw_orders AS
SELECT * FROM raw_orders r
WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_orders x WHERE x._source_file = r._source_file);

CREATE OR REPLACE VIEW v_latest_raw_payments AS
SELECT * FROM raw_payments r
WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_payments x WHERE x._source_file = r._source_file);

CREATE OR REPLACE VIEW v_latest_raw_customers AS
SELECT * FROM raw_customers r
WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_customers x WHERE x._source_file = r._source_file);

CREATE OR REPLACE VIEW v_latest_raw_products AS
SELECT * FROM raw_products r
WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_products x WHERE x._source_file = r._source_file);

CREATE OR REPLACE VIEW v_latest_raw_stores AS
SELECT * FROM raw_stores r
WHERE _run_id = (SELECT arg_max(_run_id, _loaded_at) FROM raw_stores x WHERE x._source_file = r._source_file);
