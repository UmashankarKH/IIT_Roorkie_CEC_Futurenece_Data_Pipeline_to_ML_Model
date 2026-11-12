-- Business question 6 (ELT): how many rows were rejected in the latest run,
-- by file and by reason. Source: the per-row failure list built in SQL.
SELECT f.dataset,
       f.rule_id,
       c.check_name,
       c.severity,
       count(*) AS rows_rejected
FROM elt_rule_failures f
LEFT JOIN dq_rule_catalogue c ON c.rule_id = f.rule_id AND c.dataset = f.dataset
WHERE f.run_id = (SELECT run_id FROM pipeline_runs
                  WHERE dag_id = 'nammamart_elt_waseem' ORDER BY start_time DESC LIMIT 1)
  AND c.severity = 'Critical'
GROUP BY f.dataset, f.rule_id, c.check_name, c.severity
ORDER BY rows_rejected DESC, f.dataset;
