"""Execute the six Step 11 business questions against either warehouse."""

from __future__ import annotations

from pathlib import Path
import re

import duckdb
import pandas as pd


SQL_DIR = Path(__file__).resolve().parent / "sql"
QUERY_FILES = {
    "ETL": SQL_DIR / "business_questions_etl.sql",
    "ELT": SQL_DIR / "business_questions_elt.sql",
}


def load_queries(pipeline: str) -> dict[str, str]:
    """Split the checked-in SQL file into named, independently runnable queries."""

    text = QUERY_FILES[pipeline].read_text()
    sections = re.split(r"^-- QUESTION: ([^\n]+)\n", text, flags=re.MULTILINE)
    return {sections[index]: sections[index + 1].strip() for index in range(1, len(sections), 2)}


def run_business_questions(database_path: str | Path, pipeline: str) -> dict[str, pd.DataFrame]:
    """Return one DataFrame per business question using a read-only connection."""

    results: dict[str, pd.DataFrame] = {}
    with duckdb.connect(str(database_path), read_only=True) as connection:
        for question_name, query in load_queries(pipeline).items():
            results[question_name] = connection.execute(query).fetchdf()
    return results


def september_revenue(database_path: str | Path, pipeline: str) -> float:
    """Calculate the comparable September delivered revenue total."""

    if pipeline == "ETL":
        query = """
            SELECT COALESCE(SUM(net_revenue), 0)
            FROM etl_fact_orders
            WHERE order_date >= '2026-09-01' AND order_date < '2026-10-01'
              AND order_status = 'DELIVERED'
        """
    else:
        query = """
            SELECT COALESCE(SUM(net_revenue), 0)
            FROM stg_orders
            WHERE dq_status = 'PASS'
              AND order_status = 'DELIVERED'
              AND order_date >= '2026-09-01' AND order_date < '2026-10-01'
        """
    with duckdb.connect(str(database_path), read_only=True) as connection:
        return float(connection.execute(query).fetchone()[0])


def explain_september_difference(etl_database: str | Path, elt_database: str | Path) -> pd.DataFrame:
    """Identify order IDs included by only one September revenue definition."""

    query = """
        WITH etl AS (
            SELECT order_id, net_revenue
            FROM etl_fact_orders
            WHERE order_date >= '2026-09-01' AND order_date < '2026-10-01'
              AND order_status = 'DELIVERED'
        ), elt AS (
            SELECT order_id, net_revenue
            FROM elt_orders
        )
        SELECT COALESCE(etl.order_id, elt.order_id) AS order_id,
               CASE WHEN etl.order_id IS NULL THEN 'ELT only'
                    WHEN elt.order_id IS NULL THEN 'ETL only'
                    ELSE 'Both' END AS inclusion,
               COALESCE(etl.net_revenue, 0) AS etl_revenue,
               COALESCE(elt.net_revenue, 0) AS elt_revenue
        FROM etl
        FULL OUTER JOIN elt USING (order_id)
        WHERE etl.order_id IS NULL OR elt.order_id IS NULL
        ORDER BY inclusion, order_id
    """
    with duckdb.connect(str(etl_database), read_only=True) as etl_connection, duckdb.connect(str(elt_database), read_only=True) as elt_connection:
        etl_connection.register("elt_orders", elt_connection.execute("SELECT order_id, net_revenue FROM stg_orders WHERE dq_status = 'PASS' AND order_status = 'DELIVERED' AND order_date >= '2026-09-01' AND order_date < '2026-10-01'").fetchdf())
        return etl_connection.execute(query).fetchdf()
