from pathlib import Path
import sys
import duckdb

# This file is expected at Assignment/tests/scripts/inspect_warehouse.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_DIR = PROJECT_ROOT / "include" / "warehouse"

DATABASES = {
    "ETL": WAREHOUSE_DIR / "warehouse_etl.duckdb",
    "ELT": WAREHOUSE_DIR / "warehouse_elt.duckdb",
}

# Common tables whose row counts should normally be non-zero after a successful run.
EXPECTED_TABLES = {
    "ETL": ["etl_fact_orders"],
    "ELT": [
        "raw_orders", "raw_payments", "raw_customers", "raw_products", "raw_stores",
        "stg_orders", "stg_payments", "stg_customers", "stg_products", "stg_stores",
        "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
        "mart_payment_reconciliation", "mart_top_customers",
        "mart_revenue_by_payment_mode", "mart_rejected_rows",
    ],
}

# Key columns are checked only when both the table and column exist.
KEY_CHECKS = {
    "ETL": {
        "etl_fact_orders": ["order_id"],
        "dim_customers": ["customer_id"],
        "dim_products": ["product_id"],
        "dim_stores": ["store_id"],
    },
    "ELT": {
        "raw_orders": ["order_id"],
        "raw_customers": ["customer_id"],
        "raw_products": ["product_id"],
        "raw_stores": ["store_id"],
        "stg_orders": ["order_id"],
        "stg_customers": ["customer_id"],
        "stg_products": ["product_id"],
        "stg_stores": ["store_id"],
    },
}


def quote_identifier(name: str) -> str:
    """Safely quote a DuckDB table/column identifier."""
    return '"' + name.replace('"', '""') + '"'


def get_tables(con):
    rows = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()
    return [row[0] for row in rows]


def get_columns(con, table):
    rows = con.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        ORDER BY ordinal_position
    """, [table]).fetchall()
    return [row[0] for row in rows]


def check_key(con, table, key):
    columns = get_columns(con, table)
    if key not in columns:
        print(f"    WARN  {table}.{key}: column not present; skipped")
        return

    qtable = quote_identifier(table)
    qkey = quote_identifier(key)

    null_count = con.execute(
        f"SELECT COUNT(*) FROM {qtable} WHERE {qkey} IS NULL"
    ).fetchone()[0]

    duplicate_count = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT {qkey}
            FROM {qtable}
            WHERE {qkey} IS NOT NULL
            GROUP BY {qkey}
            HAVING COUNT(*) > 1
        ) duplicates
    """).fetchone()[0]

    status = "PASS" if null_count == 0 and duplicate_count == 0 else "FAIL"
    print(
        f"    {status} key {table}.{key}: "
        f"nulls={null_count}, duplicated_key_values={duplicate_count}"
    )


def inspect_database(label, db_path):
    print(f"\n{'=' * 72}")
    print(f"{label} DATABASE")
    print(f"Path: {db_path}")
    print(f"{'=' * 72}")

    if not db_path.exists():
        print("FAIL Database file does not exist.")
        return False

    try:
        # Read-only mode avoids changing data during inspection.
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as exc:
        print(f"FAIL Could not open database: {exc}")
        return False

    failed_checks = 0
    try:
        tables = get_tables(con)
        print(f"Tables found: {len(tables)}")

        if not tables:
            print("FAIL No tables found.")
            con.close()
            return False

        print("\nTable row counts:")
        for table in tables:
            count = con.execute(
                f"SELECT COUNT(*) FROM {quote_identifier(table)}"
            ).fetchone()[0]
            status = "WARN EMPTY" if count == 0 else "OK"
            print(f"  {status:10} {table}: {count:,} rows")

        print("\nExpected table checks:")
        for table in EXPECTED_TABLES.get(label, []):
            if table not in tables:
                print(f"  FAIL Missing expected table: {table}")
                failed_checks += 1
            else:
                count = con.execute(
                    f"SELECT COUNT(*) FROM {quote_identifier(table)}"
                ).fetchone()[0]
                if count == 0:
                    print(f"  FAIL Expected table is empty: {table}")
                    failed_checks += 1
                else:
                    print(f"  PASS {table}: {count:,} rows")

        print("\nKey/null/duplicate checks:")
        for table, keys in KEY_CHECKS.get(label, {}).items():
            if table not in tables:
                continue
            for key in keys:
                before = failed_checks
                columns = get_columns(con, table)
                if key in columns:
                    qtable, qkey = quote_identifier(table), quote_identifier(key)
                    null_count = con.execute(
                        f"SELECT COUNT(*) FROM {qtable} WHERE {qkey} IS NULL"
                    ).fetchone()[0]
                    duplicate_count = con.execute(f"""
                        SELECT COUNT(*) FROM (
                            SELECT {qkey} FROM {qtable}
                            WHERE {qkey} IS NOT NULL
                            GROUP BY {qkey} HAVING COUNT(*) > 1
                        ) d
                    """).fetchone()[0]
                    status = "PASS" if null_count == 0 and duplicate_count == 0 else "FAIL"
                    print(
                        f"  {status} {table}.{key}: nulls={null_count}, "
                        f"duplicate key values={duplicate_count}"
                    )
                    if status == "FAIL":
                        failed_checks += 1
                else:
                    print(f"  WARN {table}.{key}: column missing; skipped")

        # Basic audit-table visibility checks; these are informational because
        # a specific run ID may not be present in older database contents.
        print("\nAudit table checks:")
        for table in ("dq_results", "pipeline_runs"):
            if table in tables:
                n = con.execute(
                    f"SELECT COUNT(*) FROM {quote_identifier(table)}"
                ).fetchone()[0]
                print(f"  PASS {table} exists ({n:,} rows)")
            else:
                print(f"  WARN {table} not found")

        print(f"\nResult: {'PASS' if failed_checks == 0 else f'FAIL ({failed_checks} check(s))'}")
        return failed_checks == 0
    finally:
        con.close()


def main():
    overall_ok = True
    for label, db_path in DATABASES.items():
        ok = inspect_database(label, db_path)
        overall_ok = overall_ok and ok

    print(f"\n{'=' * 72}")
    print("OVERALL:", "PASS" if overall_ok else "CHECK FAILURES ABOVE")
    print("Note: these are structural/basic data checks, not a substitute for the DAG's full DQ rules.")
    print(f"{'=' * 72}")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
