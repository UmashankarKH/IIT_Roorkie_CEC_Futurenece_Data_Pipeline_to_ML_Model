"""SCD Type 2 history for customer loyalty tiers in DuckDB."""

from datetime import date

import duckdb


SCD2_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
    customer_id VARCHAR NOT NULL,
    loyalty_tier VARCHAR,
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BOOLEAN NOT NULL
)
"""


def apply_customer_loyalty_scd2(
    con: duckdb.DuckDBPyConnection,
    source_table: str,
    effective_date: date | None = None,
) -> dict:
    """Maintain loyalty-tier history while keeping one current row per customer."""
    if source_table not in {"etl_dim_customer", "customers_curated"}:
        raise ValueError(f"Unsupported source table: {source_table}")

    effective_date = effective_date or date.today()
    con.execute(SCD2_TABLE_SQL)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _scd2_customer_source AS
        SELECT customer_id, loyalty_tier
        FROM (
            SELECT
                TRIM(CAST(customer_id AS VARCHAR)) AS customer_id,
                NULLIF(UPPER(TRIM(CAST(loyalty_tier AS VARCHAR))), '') AS loyalty_tier,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(customer_id AS VARCHAR))
                    ORDER BY TRIM(CAST(customer_id AS VARCHAR))
                ) AS rn
            FROM "{source_table}"
            WHERE customer_id IS NOT NULL
              AND TRIM(CAST(customer_id AS VARCHAR)) <> ''
        )
        WHERE rn = 1
    """)

    # Close changed versions only when their start date is earlier.
    con.execute("""
        UPDATE dim_customer_scd2 AS history
        SET effective_to = ? - INTERVAL 1 DAY,
            is_current = FALSE
        FROM _scd2_customer_source AS source
        WHERE history.customer_id = source.customer_id
          AND history.is_current = TRUE
          AND history.loyalty_tier IS DISTINCT FROM source.loyalty_tier
          AND history.effective_from < ?
    """, [effective_date, effective_date])

    # If a tier changes again on the same date, update that day's version
    # rather than creating a zero-length historical record.
    con.execute("""
        UPDATE dim_customer_scd2 AS history
        SET loyalty_tier = source.loyalty_tier
        FROM _scd2_customer_source AS source
        WHERE history.customer_id = source.customer_id
          AND history.is_current = TRUE
          AND history.loyalty_tier IS DISTINCT FROM source.loyalty_tier
          AND history.effective_from = ?
    """, [effective_date])

    # Insert customers without a current version, including changed tiers
    # whose prior version was closed above.
    con.execute("""
        INSERT INTO dim_customer_scd2 (
            customer_id, loyalty_tier, effective_from, effective_to, is_current
        )
        SELECT source.customer_id, source.loyalty_tier, ?, NULL, TRUE
        FROM _scd2_customer_source AS source
        LEFT JOIN dim_customer_scd2 AS current
          ON current.customer_id = source.customer_id
         AND current.is_current = TRUE
        WHERE current.customer_id IS NULL
    """, [effective_date])

    row = con.execute("""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE is_current),
            COUNT(*) FILTER (WHERE NOT is_current)
        FROM dim_customer_scd2
    """).fetchone()

    return {
        "history_rows": row[0],
        "current_customers": row[1],
        "historical_versions": row[2],
    }
