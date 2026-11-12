"""
include/nammamart_observability.py
---------------------------------
Observability and run-audit module for NammaMart pipelines.
Maintains pipeline_runs table for tracking SLAs, row counts, and health.
Includes capacity monitoring and alert notifications.
"""

import os
import duckdb
import pendulum
from datetime import datetime
from nammamart_etl import get_paths


def init_observability_tables(con: duckdb.DuckDBPyConnection):
    """Initializes pipeline_runs table."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR,
            dag_id VARCHAR,
            start_time VARCHAR,
            end_time VARCHAR,
            rows_extracted INTEGER,
            rows_loaded INTEGER,
            rows_quarantined INTEGER,
            status VARCHAR
        )
    """)


def record_pipeline_run(con: duckdb.DuckDBPyConnection, run_id: str, dag_id: str,
                        start_time: str, end_time: str,
                        rows_extracted: int, rows_loaded: int,
                        rows_quarantined: int, status: str):
    """Writes a run record to pipeline_runs."""
    init_observability_tables(con)
    con.execute("""
        INSERT INTO pipeline_runs (
            run_id, dag_id, start_time, end_time,
            rows_extracted, rows_loaded, rows_quarantined, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        run_id, dag_id, start_time, end_time,
        int(rows_extracted), int(rows_loaded), int(rows_quarantined), status
    ])
    print(f"[Observability] Recorded run {run_id} ({dag_id}) -> {status} (Extracted: {rows_extracted}, Loaded: {rows_loaded}, Quarantined: {rows_quarantined})")


def check_store_capacity(con: duckdb.DuckDBPyConnection, fact_table: str = "etl_fact_orders",
                         dim_store_table: str = "etl_dim_store"):
    """
    Bonus: Store capacity check comparing daily order count to store capacity_orders_per_day.
    Returns DataFrame of days/stores that exceeded capacity.
    """
    query = f"""
        SELECT
            f.order_date,
            s.store_id,
            s.store_name,
            s.zone,
            COUNT(f.order_id) AS orders_placed,
            s.capacity_orders_per_day,
            ROUND(COUNT(f.order_id) * 100.0 / s.capacity_orders_per_day, 1) AS utilization_pct,
            CASE
                WHEN COUNT(f.order_id) > s.capacity_orders_per_day THEN 'EXCEEDED'
                WHEN COUNT(f.order_id) > 0.8 * s.capacity_orders_per_day THEN 'NEAR_CAPACITY'
                ELSE 'NORMAL'
            END AS capacity_status
        FROM {fact_table} f
        JOIN {dim_store_table} s ON f.store_id = s.store_id
        GROUP BY f.order_date, s.store_id, s.store_name, s.zone, s.capacity_orders_per_day
        ORDER BY utilization_pct DESC;
    """
    return con.execute(query).fetchdf()
