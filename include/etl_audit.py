"""Writes the quality scorecard (dq_results) and the run audit (pipeline_runs) to DuckDB."""
import duckdb
import pandas as pd

from include.nammamart_config import WAREHOUSE


def _upsert(df, table, dag_id, run_id):
    """Re-running the same run_id replaces its rows, so a retry never duplicates history."""
    con = duckdb.connect(str(WAREHOUSE))
    con.register("src", df)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM src WHERE 1=0")
    con.execute(f"DELETE FROM {table} WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM src")
    con.close()


def write_dq_results(results, dag_id, run_id):
    _upsert(pd.DataFrame(results), "dq_results", dag_id, run_id)


def write_pipeline_run(dag_id, run_id, start_time, end_time, rows_extracted,
                       rows_loaded, rows_quarantined, status):
    row = pd.DataFrame([dict(run_id=run_id, dag_id=dag_id, start_time=start_time,
                             end_time=end_time, rows_extracted=int(rows_extracted),
                             rows_loaded=int(rows_loaded), rows_quarantined=int(rows_quarantined),
                             status=status)])
    _upsert(row, "pipeline_runs", dag_id, run_id)