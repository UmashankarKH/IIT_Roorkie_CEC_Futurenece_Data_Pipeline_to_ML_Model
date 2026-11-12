-- Business question 6 (ETL): how many rows were rejected in the latest run,
-- by file and by reason. Source: the quarantine table written by the ETL DAG.
SELECT source_file,
       regexp_extract(failure_reason, 'RULE-[0-9]+') AS rule_id,
       count(*)                                      AS rows_rejected
FROM etl_quarantine
WHERE run_id = (SELECT run_id FROM pipeline_runs
                WHERE dag_id = 'nammamart_etl_waseem' ORDER BY start_time DESC LIMIT 1)
GROUP BY source_file, rule_id
ORDER BY rows_rejected DESC, source_file;
