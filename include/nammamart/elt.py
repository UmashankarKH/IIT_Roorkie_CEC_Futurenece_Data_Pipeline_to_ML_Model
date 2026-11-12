
"""DuckDB SQL-based ELT pipeline for NammaMart."""

import logging
from pathlib import Path

import duckdb
import pandas as pd
from include.nammamart.scd2 import apply_customer_loyalty_scd2

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"
OUTPUT_DIR = PROJECT_ROOT / "include" / "output" / "nammamart_elt"
DB_PATH = OUTPUT_DIR / "nammamart_elt.duckdb"


SOURCE_FILES = {
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "orders": "orders.csv",
    "orders_increment": "orders_2026-10-01_increment.csv",
    "payments": "payments.csv",
}


def run_elt(run_id: str | None = None) -> dict:
    """Load raw CSVs into DuckDB, transform using SQL, and export results."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    results = {}

    try:
        # EXTRACT + LOAD: append-only raw history with file-hash tracking.
        import csv
        import hashlib
        import uuid

        run_id = run_id or str(uuid.uuid4())

        source_to_raw = {
            "customers": "customers",
            "products": "products",
            "stores": "stores",
            "orders": "orders",
            "orders_increment": "orders",
            "payments": "payments",
        }

        con.execute("""
            CREATE TABLE IF NOT EXISTS elt_ingestion_manifest (
                source_file VARCHAR PRIMARY KEY,
                content_hash VARCHAR NOT NULL,
                last_loaded_at TIMESTAMP NOT NULL,
                last_run_id VARCHAR NOT NULL
            )
        """)

        # Create destination raw tables and add metadata to legacy tables.
        for source_name, filename in SOURCE_FILES.items():
            source_path = SOURCE_DIR / filename
            if not source_path.is_file():
                raise FileNotFoundError(source_path)

            raw_table = f"raw_{source_to_raw[source_name]}"
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {raw_table} AS
                SELECT *,
                       CAST(NULL AS TIMESTAMP) AS _loaded_at,
                       CAST(NULL AS VARCHAR) AS _source_file,
                       CAST(NULL AS VARCHAR) AS _run_id
                FROM read_csv_auto(?, all_varchar = true)
                WHERE FALSE
                """,
                [str(source_path)],
            )
            for column, sql_type in (
                ("_loaded_at", "TIMESTAMP"),
                ("_source_file", "VARCHAR"),
                ("_run_id", "VARCHAR"),
            ):
                con.execute(
                    f"ALTER TABLE {raw_table} "
                    f"ADD COLUMN IF NOT EXISTS {column} {sql_type}"
                )

            # Backfill legacy rows without overwriting existing metadata.
            legacy_file = (
                "orders.csv" if source_name == "orders" else filename
            )
            con.execute(
                f"""
                UPDATE {raw_table}
                SET _loaded_at = COALESCE(_loaded_at, CURRENT_TIMESTAMP),
                    _source_file = COALESCE(_source_file, ?),
                    _run_id = COALESCE(_run_id, 'legacy_migration')
                WHERE _loaded_at IS NULL
                   OR _source_file IS NULL
                   OR _run_id IS NULL
                """,
                [legacy_file],
            )

        # Migrate the old separate increment table once, if it exists.
        increment_file = SOURCE_FILES["orders_increment"]
        legacy_increment_exists = con.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'raw_orders_increment'
        """).fetchone()[0] > 0

        migration_key = "__legacy_raw_orders_increment__"
        migration_done = con.execute(
            "SELECT COUNT(*) FROM elt_ingestion_manifest "
            "WHERE source_file = ?",
            [migration_key],
        ).fetchone()[0] > 0

        if legacy_increment_exists and not migration_done:
            already_migrated = con.execute("""
                SELECT COUNT(*)
                FROM raw_orders
                WHERE _source_file = ?
            """, [increment_file]).fetchone()[0]

            if already_migrated == 0:
                columns = [
                    row[0] for row in con.execute(
                        "SELECT * FROM raw_orders_increment LIMIT 0"
                    ).description
                ]
                # Exclude metadata if an older experimental table has them.
                columns = [
                    name for name in columns
                    if name not in {"_loaded_at", "_source_file", "_run_id"}
                ]
                quoted = ", ".join('"' + name.replace('"', '""') + '"' for name in columns)
                con.execute(
                    f"""
                    INSERT INTO raw_orders
                        ({quoted}, _loaded_at, _source_file, _run_id)
                    SELECT {quoted}, CURRENT_TIMESTAMP, ?, 'legacy_migration'
                    FROM raw_orders_increment
                    """,
                    [increment_file],
                )

            con.execute(
                "DELETE FROM elt_ingestion_manifest WHERE source_file = ?",
                [migration_key],
            )
            con.execute(
                """
                INSERT INTO elt_ingestion_manifest
                    (source_file, content_hash, last_loaded_at, last_run_id)
                VALUES (?, 'migrated-v1', CURRENT_TIMESTAMP, ?)
                """,
                [migration_key, run_id],
            )

        # Hash each source. Bootstrap the manifest from matching legacy rows
        # where possible; otherwise append the snapshot once.
        for source_name, filename in SOURCE_FILES.items():
            source_path = SOURCE_DIR / filename
            raw_table = f"raw_{source_to_raw[source_name]}"
            digest = hashlib.sha256(source_path.read_bytes()).hexdigest()

            manifest_hash = con.execute(
                "SELECT content_hash FROM elt_ingestion_manifest "
                "WHERE source_file = ?",
                [filename],
            ).fetchone()

            if manifest_hash and manifest_hash[0] == digest:
                logging.info("Raw ingestion skipped; unchanged file: %s", filename)
                continue

            if manifest_hash is None:
                csv_rows = sum(
                    1 for _ in csv.DictReader(
                        source_path.open(
                            newline="", encoding="utf-8-sig"
                        )
                    )
                )
                legacy_rows = con.execute(
                    f"""
                    SELECT COUNT(*) FROM {raw_table}
                    WHERE _source_file = ? AND _run_id = 'legacy_migration'
                    """,
                    [filename],
                ).fetchone()[0]

                if csv_rows == legacy_rows:
                    con.execute(
                        """
                        INSERT INTO elt_ingestion_manifest
                            (source_file, content_hash, last_loaded_at, last_run_id)
                        VALUES (?, ?, CURRENT_TIMESTAMP, ?)
                        """,
                        [filename, digest, run_id],
                    )
                    logging.info(
                        "Manifest bootstrapped from legacy rows: %s (%s rows)",
                        filename, csv_rows,
                    )
                    continue

            columns = [
                row[0] for row in con.execute(
                    "DESCRIBE SELECT * FROM read_csv_auto(?, all_varchar = true)",
                    [str(source_path)],
                ).fetchall()
            ]
            quoted = ", ".join('"' + name.replace('"', '""') + '"' for name in columns)
            con.execute(
                f"""
                INSERT INTO {raw_table}
                    ({quoted}, _loaded_at, _source_file, _run_id)
                SELECT {quoted}, CURRENT_TIMESTAMP, ?, ?
                FROM read_csv_auto(?, all_varchar = true)
                """,
                [filename, run_id, str(source_path)],
            )
            con.execute(
                "DELETE FROM elt_ingestion_manifest WHERE source_file = ?",
                [filename],
            )
            con.execute(
                """
                INSERT INTO elt_ingestion_manifest
                    (source_file, content_hash, last_loaded_at, last_run_id)
                VALUES (?, ?, CURRENT_TIMESTAMP, ?)
                """,
                [filename, digest, run_id],
            )
            logging.info("Appended raw snapshot: %s", filename)

        # Always transform the current source snapshot, even if unchanged
        # and therefore not appended to the persistent raw tables.
        for source_name, filename in SOURCE_FILES.items():
            source_path = SOURCE_DIR / filename
            if source_name == "orders_increment":
                temp_table = "raw_orders_increment_current"
            else:
                temp_table = f"raw_{source_to_raw[source_name]}_current"

            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE {temp_table} AS
                SELECT *
                FROM read_csv_auto(?, all_varchar = true)
                """,
                [str(source_path)],
            )

        # TRANSFORM: dimensions with conflicting business keys are quarantined.
        for table, key in [
            ("customers", "customer_id"),
            ("products", "product_id"),
            ("stores", "store_id"),
        ]:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table}_conflict_keys AS
                SELECT {key}
                FROM (
                    SELECT DISTINCT *
                    FROM raw_{table}_current
                ) d
                GROUP BY {key}
                HAVING COUNT(*) > 1
                """
            )

            # Identical duplicate rows are removed before conflict handling.
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table}_distinct AS
                SELECT DISTINCT *
                FROM raw_{table}_current
                """
            )

            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table}_quarantine AS
                SELECT d.*,
                       'conflicting_{key}' AS quarantine_reason
                FROM {table}_distinct d
                JOIN {table}_conflict_keys k USING ({key})
                UNION ALL
                SELECT d.*,
                       'missing_{key}' AS quarantine_reason
                FROM {table}_distinct d
                WHERE {key} IS NULL OR TRIM({key}) = ''
                """
            )

            #con.execute(
            #    f"""
            #    CREATE OR REPLACE TABLE {table}_curated AS
            #    SELECT * EXCLUDE ({key})
            #    FROM {table}_distinct
            #    WHERE FALSE
            #    """
            #) if False else None

            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table}_curated AS
                SELECT * EXCLUDE (_rn, _conflict)
                FROM (
                    SELECT d.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY {key}
                               ORDER BY {key}
                           ) AS _rn,
                           CASE
                               WHEN k.{key} IS NOT NULL THEN 1
                               ELSE 0
                           END AS _conflict
                    FROM {table}_distinct d
                    LEFT JOIN {table}_conflict_keys k USING ({key})
                    WHERE {key} IS NOT NULL AND TRIM({key}) <> ''
                ) q
                WHERE _rn = 1 AND _conflict = 0
                """
            )

        # Fill known optional dimension fields.
        con.execute(
            "UPDATE customers_curated SET city = 'UNKNOWN' "
            "WHERE city IS NULL OR TRIM(city) = ''"
        )
        con.execute("""
                UPDATE customers_curated
                SET email = NULL
                WHERE email IS NOT NULL
                AND UPPER(TRIM(email)) = 'N/A'
        """)
        con.execute(
            "UPDATE products_curated SET category = 'UNKNOWN' "
            "WHERE category IS NULL OR TRIM(category) = ''"
        )
        con.execute(
            "UPDATE stores_curated SET zone = 'UNKNOWN' "
            "WHERE zone IS NULL OR TRIM(zone) = ''"
        )

        # Merge base and incremental orders: the increment updates status
        # for existing order IDs and inserts new order IDs.
        con.execute("""
            CREATE OR REPLACE TABLE orders_base_distinct AS
            SELECT DISTINCT * FROM raw_orders_current
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_increment_distinct AS
            SELECT DISTINCT * FROM raw_orders_increment_current
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_base_conflict_keys AS
            SELECT order_id
            FROM orders_base_distinct
            WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
            GROUP BY order_id
            HAVING COUNT(*) > 1
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_increment_conflict_keys AS
            SELECT order_id
            FROM orders_increment_distinct
            WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
            GROUP BY order_id
            HAVING COUNT(*) > 1
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_quarantine AS
            SELECT b.*, 'conflicting_order_id' AS quarantine_reason
            FROM orders_base_distinct b
            JOIN orders_base_conflict_keys k USING (order_id)
            UNION ALL
            SELECT i.*, 'conflicting_increment_order_id' AS quarantine_reason
            FROM orders_increment_distinct i
            JOIN orders_increment_conflict_keys k USING (order_id)
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_merged AS
            SELECT b.*, 'orders.csv' AS source_file
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY order_id
                ) AS _rn
                FROM orders_base_distinct
                WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
                  AND order_id NOT IN (SELECT order_id FROM orders_base_conflict_keys)
            ) b
            WHERE b._rn = 1
            UNION ALL
            SELECT i.*, 'orders_2026-10-01_increment.csv' AS source_file
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY order_id
                ) AS _rn
                FROM orders_increment_distinct
                WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
                  AND order_id NOT IN (SELECT order_id FROM orders_increment_conflict_keys)
                  AND order_id NOT IN (
                      SELECT order_id FROM orders_base_distinct
                  )
            ) i
            WHERE i._rn = 1
        """)
        con.execute("""

            CREATE OR REPLACE TABLE orders_upserted AS
            SELECT
                b.order_id,
                b.order_ts,
                b.customer_id,
                b.store_id,
                b.product_id,
                b.quantity,
                b.unit_price,
                b.discount_pct,
                b.order_amount,
                b.payment_mode,
                COALESCE(i.order_status, b.order_status) AS order_status,
                b.channel,
                b.delivery_minutes,
                'orders.csv' AS source_file
            FROM orders_base_distinct b
            LEFT JOIN orders_increment_distinct i
                ON b.order_id = i.order_id
            WHERE b.order_id IS NOT NULL
            AND TRIM(b.order_id) <> ''
            AND b.order_id NOT IN (
                SELECT order_id FROM orders_base_conflict_keys
            )
            AND b.order_id NOT IN (
                SELECT order_id FROM orders_increment_conflict_keys
            )

            UNION ALL

            SELECT
                i.order_id,
                i.order_ts,
                i.customer_id,
                i.store_id,
                i.product_id,
                i.quantity,
                i.unit_price,
                i.discount_pct,
                i.order_amount,
                i.payment_mode,
                i.order_status,
                i.channel,
                i.delivery_minutes,
                'orders_2026-10-01_increment.csv' AS source_file
            FROM orders_increment_distinct i
            WHERE i.order_id IS NOT NULL
            AND TRIM(i.order_id) <> ''
            AND i.order_id NOT IN (
                SELECT order_id FROM orders_increment_conflict_keys
            )
            AND i.order_id NOT IN (
                SELECT order_id FROM orders_base_distinct
            );



         """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_validated AS
            SELECT o.*,
                CONCAT_WS(',',
                    CASE
                        WHEN o.order_ts IS NULL
                            OR TRIM(o.order_ts) = ''
                            OR UPPER(TRIM(o.order_ts)) = 'N/A'
                            THEN 'missing_order_ts'
                        WHEN TRY_CAST(o.order_ts AS TIMESTAMP) IS NULL
                            THEN 'invalid_order_ts'
                        END,
                    CASE WHEN o.customer_id IS NULL OR TRIM(o.customer_id) = ''
                         THEN 'missing_customer_id'
                         WHEN c.customer_id IS NULL THEN 'orphan_customer_id' END,
                    CASE WHEN o.product_id IS NULL OR TRIM(o.product_id) = ''
                         THEN 'missing_product_id'
                         WHEN p.product_id IS NULL THEN 'orphan_product_id' END,
                    CASE WHEN o.store_id IS NULL OR TRIM(o.store_id) = ''
                         THEN 'missing_store_id'
                         WHEN s.store_id IS NULL THEN 'orphan_store_id' END
                ) AS quarantine_reason
            FROM orders_upserted o
            LEFT JOIN customers_curated c USING (customer_id)
            LEFT JOIN products_curated p USING (product_id)
            LEFT JOIN stores_curated s USING (store_id)
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_quarantine_final AS
            SELECT * FROM orders_validated
            WHERE quarantine_reason <> ''
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_curated AS
            SELECT * EXCLUDE (quarantine_reason)
            FROM orders_validated
            WHERE quarantine_reason = ''
        """)
        con.execute("""
            CREATE OR REPLACE TABLE orders_quarantine AS
            SELECT *
            FROM orders_quarantine_final

            UNION ALL

            SELECT
                b.order_id,
                b.order_ts,
                b.customer_id,
                b.store_id,
                b.product_id,
                b.quantity,
                b.unit_price,
                b.discount_pct,
                b.order_amount,
                b.payment_mode,
                b.order_status,
                b.channel,
                b.delivery_minutes,
                'orders.csv' AS source_file,
                'missing_order_id' AS quarantine_reason
            FROM orders_base_distinct b
            WHERE b.order_id IS NULL OR TRIM(b.order_id) = ''

            UNION ALL

            SELECT
                i.order_id,
                i.order_ts,
                i.customer_id,
                i.store_id,
                i.product_id,
                i.quantity,
                i.unit_price,
                i.discount_pct,
                i.order_amount,
                i.payment_mode,
                i.order_status,
                i.channel,
                i.delivery_minutes,
                'orders_2026-10-01_increment.csv' AS source_file,
                'missing_order_id' AS quarantine_reason
            FROM orders_increment_distinct i
            WHERE i.order_id IS NULL OR TRIM(i.order_id) = ''
        """)

        # Payments referencing orders excluded from curation are quarantined.
        con.execute("""
            CREATE OR REPLACE TABLE payments_distinct AS
            SELECT DISTINCT * FROM raw_payments_current
        """)
        con.execute("""
            CREATE OR REPLACE TABLE payments_quarantine AS
            SELECT p.*, 'orphan_order_id' AS quarantine_reason
            FROM payments_distinct p
            LEFT JOIN orders_curated o USING (order_id)
            WHERE o.order_id IS NULL
        """)
        con.execute("""
            CREATE OR REPLACE TABLE payments_curated AS
            SELECT p.*
            FROM payments_distinct p
            JOIN orders_curated o USING (order_id)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY p.payment_id ORDER BY p.payment_id
            ) = 1
        """)

        # Execute the assignment SQL layers on the same connection so
        # current-run temporary source tables remain available.
        from include.nammamart.sql_runner import execute_sql_files


        execute_sql_files(
            con,
            [
                "01_staging_dimensions.sql",
                "02_dimension_quality.sql",
                "03_staging_orders.sql",
                "04_staging_payments.sql",
                "05_mart_daily_store_kpis.sql",
                "06_mart_category_margin.sql",
                "07_mart_delivery_sla.sql",
                "08_mart_top_customers.sql",
                "09_mart_store_capacity.sql",
            ],
        )

        # Maintain customer loyalty-tier history after curated tables exist.
        scd2_summary = apply_customer_loyalty_scd2(
            con,
            source_table="customers_curated",
        )
        logging.info("ELT customer SCD2 summary: %s", scd2_summary)
        results["customer_scd2"] = scd2_summary

        # Export the final SQL staging and quarantine tables.
        datasets = [
            ("customers", "stg_customers", "customers_quarantine", "customer_id"),
            ("products", "stg_products", "products_quarantine", "product_id"),
            ("stores", "stg_stores", "stores_quarantine", "store_id"),
            ("orders", "stg_orders", "orders_quarantine", "order_id"),
            ("payments", "stg_payments", "payments_quarantine", "payment_id"),
        ]


        report_rows = []

        for dataset, curated_table, quarantine_table, key in datasets:
            dataset_counts = {}

            for suffix, table in [
                ("curated", curated_table),
                ("quarantine", quarantine_table),
            ]:
                df = con.execute(f"SELECT * FROM {table}").df()

                # Do not export raw customer contact details from quarantine.
                if dataset == "customers":
                    if "email" in df.columns:
                        df["email"] = df["email"].map(
                            lambda value: (
                                str(value)[:2] + "***@***"
                                if pd.notna(value) and "@" in str(value)
                                else "***"
                                if pd.notna(value) and str(value).strip()
                                else value
                            )
                        )
                    if "phone" in df.columns:
                        df["phone"] = df["phone"].map(
                            lambda value: (
                                "***" + str(value)[-2:]
                                if pd.notna(value) and str(value).strip()
                                else value
                            )
                        )

                csv_path = OUTPUT_DIR / (
                    f"{dataset}.csv"
                    if suffix == "curated"
                    else f"{dataset}_quarantine.csv"
                )
                parquet_path = OUTPUT_DIR / (
                    f"{dataset}.parquet"
                    if suffix == "curated"
                    else f"{dataset}_quarantine.parquet"
                )

                df.to_csv(csv_path, index=False)
                df.to_parquet(parquet_path, index=False)

                dataset_counts[suffix] = len(df)
                results[f"{dataset}_{suffix}"] = {
                    "rows": len(df),
                    "csv": str(csv_path),
                    "parquet": str(parquet_path),
                }

            curated_count = con.execute(
                f"SELECT COUNT(*) FROM {curated_table}"
            ).fetchone()[0]
            duplicate_keys, missing_keys = con.execute(
                f"""
                SELECT
                    COUNT(*) - COUNT(DISTINCT {key}),
                    COUNT(*) FILTER (
                        WHERE {key} IS NULL OR TRIM({key}) = ''
                    )
                FROM {curated_table}
                """
            ).fetchone()

            report_rows.append({
                "dataset": dataset,
                "curated_rows": curated_count,
                "quarantined_rows": dataset_counts["quarantine"],
                "total_processed_rows": (
                    curated_count + dataset_counts["quarantine"]
                ),
                "business_key": key,
                "business_key_unique": duplicate_keys == 0,
                "missing_business_keys": missing_keys,
            })

        report = pd.DataFrame(report_rows)
        report.to_csv(OUTPUT_DIR / "elt_quality_report.csv", index=False)
        report.to_parquet(OUTPUT_DIR / "elt_quality_report.parquet", index=False)
        results["quality_report"] = report.to_dict(orient="records")
        results["run_id"] = run_id
        return results
    finally:
        con.close()
