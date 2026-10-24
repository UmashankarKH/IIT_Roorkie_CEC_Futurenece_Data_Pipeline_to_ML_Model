from pathlib import Path

import duckdb


def test_order_merge_updates_corrections_and_inserts_new_orders():
    sql_path = (
        Path(__file__).parents[2]
        / "include"
        / "sql"
        / "elt_merge_orders.sql"
    )
    merge_sql = sql_path.read_text(encoding="utf-8")

    columns = [
        "order_id VARCHAR",
        "order_ts TIMESTAMP",
        "customer_id VARCHAR",
        "store_id VARCHAR",
        "product_id VARCHAR",
        "quantity INTEGER",
        "unit_price DECIMAL(12, 2)",
        "discount_pct DECIMAL(8, 2)",
        "order_amount DECIMAL(12, 2)",
        "calculated_amount DECIMAL(12, 2)",
        "payment_mode VARCHAR",
        "order_status VARCHAR",
        "channel VARCHAR",
        "delivery_minutes INTEGER",
        "net_revenue DECIMAL(12, 2)",
        "gross_margin DECIMAL(12, 2)",
        "dq_status VARCHAR",
        "failure_reason VARCHAR",
        "_source_file VARCHAR",
        "_run_id VARCHAR",
    ]
    column_names = [column.split()[0] for column in columns]
    connection = duckdb.connect(":memory:")

    try:
        connection.execute(
            "CREATE TABLE stg_orders (" + ", ".join(columns) + ")"
        )
        connection.execute(
            "CREATE TEMP TABLE stg_orders_source ("
            + ", ".join(columns)
            + ")"
        )

        def row(order_number, status, source_file, run_id):
            return (
                f"ORD{order_number:06d}",
                "2026-09-03 12:00:00",
                "CUS0001",
                "STR0001",
                "PRD0001",
                1,
                100.00,
                0.00,
                100.00,
                100.00,
                "UPI",
                status,
                "APP",
                12,
                100.00 if status == "DELIVERED" else 0.00,
                20.00 if status == "DELIVERED" else 0.00,
                "PASS",
                None,
                source_file,
                run_id,
            )

        connection.executemany(
            "INSERT INTO stg_orders VALUES ("
            + ", ".join("?" for _ in column_names)
            + ")",
            [
                row(i, "DELIVERED", "orders.csv", "base-run")
                for i in range(10)
            ],
        )
        connection.executemany(
            "INSERT INTO stg_orders_source VALUES ("
            + ", ".join("?" for _ in column_names)
            + ")",
            [
                row(
                    i,
                    "RETURNED",
                    "orders_2026-10-01_increment.csv",
                    "increment-run",
                )
                for i in range(10)
            ]
            + [
                row(
                    i,
                    "DELIVERED",
                    "orders_2026-10-01_increment.csv",
                    "increment-run",
                )
                for i in range(10, 160)
            ],
        )

        actions = connection.execute(merge_sql).fetchall()
        assert sum(action == "UPDATE" for action, _ in actions) == 10
        assert sum(action == "INSERT" for action, _ in actions) == 150
        assert connection.execute(
            "SELECT COUNT(*) FROM stg_orders"
        ).fetchone()[0] == 160
        assert connection.execute(
            "SELECT COUNT(*) FROM stg_orders "
            "WHERE order_status = 'RETURNED'"
        ).fetchone()[0] == 10

        connection.execute(
            "UPDATE stg_orders_source SET _run_id = 'repeat-run'"
        )
        assert connection.execute(merge_sql).fetchall() == []
        assert connection.execute(
            "SELECT COUNT(*) FROM stg_orders"
        ).fetchone()[0] == 160
        assert connection.execute(
            "SELECT COUNT(*) FROM stg_orders WHERE _run_id = 'repeat-run'"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_mart_sql_asset_builds_all_business_marts():
    sql_path = (
        Path(__file__).parents[2]
        / "include"
        / "sql"
        / "elt_marts.sql"
    )
    statements = sql_path.read_text(encoding="utf-8").split(";")
    connection = duckdb.connect(":memory:")

    try:
        connection.execute("""
            CREATE TABLE stg_orders (
                order_ts TIMESTAMP,
                order_id VARCHAR,
                store_id VARCHAR,
                product_id VARCHAR,
                customer_id VARCHAR,
                order_status VARCHAR,
                dq_status VARCHAR,
                net_revenue DECIMAL(12, 2),
                gross_margin DECIMAL(12, 2),
                delivery_minutes INTEGER,
                calculated_amount DECIMAL(12, 2),
                payment_mode VARCHAR
            )
        """)
        connection.execute(
            "CREATE TABLE stg_stores "
            "(store_id VARCHAR, zone VARCHAR, store_name VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE stg_products "
            "(product_id VARCHAR, category VARCHAR)"
        )
        connection.execute("""
            CREATE TABLE stg_payments (
                payment_id VARCHAR,
                order_id VARCHAR,
                payment_mode VARCHAR,
                amount_paid DECIMAL(12, 2),
                payment_status VARCHAR,
                dq_status VARCHAR
            )
        """)
        connection.execute("""
            CREATE TABLE stg_customers (
                customer_id VARCHAR,
                full_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                loyalty_tier VARCHAR
            )
        """)

        for statement in statements:
            if statement.strip():
                connection.execute(statement)

        mart_tables = {
            "mart_daily_store_kpis",
            "mart_category_margin",
            "mart_delivery_sla",
            "mart_payment_reconciliation",
            "mart_top_customers",
        }
        actual_tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert mart_tables <= actual_tables
    finally:
        connection.close()
