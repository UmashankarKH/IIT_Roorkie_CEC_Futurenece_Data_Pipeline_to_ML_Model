
import os
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "fde_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="nammamart_elt_surender_singh",
    default_args=default_args,
    description="NammaMart ELT pipeline: raw, staging, data quality, audit and marts",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["nammamart", "elt", "fde"],
)
def nammamart_elt_surender_singh():

    # Resolve paths from this DAG file, not a hardcoded container directory.
    project_dir = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )

    data_dir = os.path.join(
        project_dir, "include", "data", "nammamart"
    )

    db_path = os.path.join(
        project_dir, "include", "warehouse_surender_singh.duckdb"
    )

    quarantine_dir = os.path.join(
        project_dir, "include", "quarantine", "surender_singh"
    )

    # ---------------------------------------------------------
    # RAW LAYER
    # Raw source columns are loaded as text, with load metadata.
    # Raw records are appended, never replaced or cleaned here.
    # ---------------------------------------------------------

    def append_raw_file(table_name, filename):
        import duckdb
        import pandas as pd

        context = get_current_context()
        current_run_id = context["run_id"]
        current_loaded_at = pendulum.now(IST).to_iso8601_string()

        path = os.path.join(data_dir, filename)

        if not os.path.isfile(path):
            raise FileNotFoundError(
                f"Required source file not found: {path}"
            )

        df = pd.read_csv(path, dtype=str)

        df["_loaded_at"] = current_loaded_at
        df["_source_file"] = filename
        df["_run_id"] = current_run_id

        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        conn = duckdb.connect(db_path)

        try:
            conn.register("incoming_batch", df)

            # Create an empty raw table on the first run.
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS
                SELECT * FROM incoming_batch WHERE 1 = 0
                """
            )

            existing_columns = [
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info('{table_name}')"
                ).fetchall()
            ]

            incoming_columns = list(df.columns)

            if existing_columns != incoming_columns:
                raise ValueError(
                    f"Schema mismatch for {table_name}.\n"
                    f"Existing columns: {existing_columns}\n"
                    f"Incoming columns: {incoming_columns}"
                )

            conn.execute(
                f"""
                INSERT INTO {table_name}
                SELECT * FROM incoming_batch
                """
            )

            conn.unregister("incoming_batch")

        finally:
            conn.close()

    @task(task_id="load_orders")
    def load_orders():
        # Load the base file first, then the incremental file.
        append_raw_file("raw_orders", "orders.csv")
        append_raw_file(
            "raw_orders", "orders_2026-10-01_increment.csv"
        )
        return "Loaded raw orders."

    @task(task_id="load_payments")
    def load_payments():
        append_raw_file("raw_payments", "payments.csv")
        return "Loaded raw payments."

    @task(task_id="load_customers")
    def load_customers():
        append_raw_file("raw_customers", "customers.csv")
        return "Loaded raw customers."

    @task(task_id="load_products")
    def load_products():
        append_raw_file("raw_products", "products.csv")
        return "Loaded raw products."

    @task(task_id="load_stores")
    def load_stores():
        append_raw_file("raw_stores", "stores.csv")
        return "Loaded raw stores."

    # ---------------------------------------------------------
    # STAGING, QUALITY CHECKS, QUARANTINE, MARTS AND AUDIT
    # ---------------------------------------------------------

    @task(task_id="transform_staging")
    def transform_staging():
        import duckdb

        context = get_current_context()
        current_run_id = context["run_id"]
        current_dag_id = context["dag"].dag_id

        conn = duckdb.connect(db_path)

        try:
            # -------------------------------------------------
            # 1. Create persistent quality and audit tables
            # -------------------------------------------------

            conn.execute("""
                CREATE TABLE IF NOT EXISTS dq_results (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    check_name VARCHAR,
                    dimension VARCHAR,
                    severity VARCHAR,
                    rows_checked BIGINT,
                    rows_failed BIGINT,
                    status VARCHAR,
                    checked_at TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    rows_extracted BIGINT,
                    rows_loaded BIGINT,
                    rows_quarantined BIGINT,
                    status VARCHAR
                )
            """)

            # -------------------------------------------------
            # 2. Stage customers: deduplicate and mask PII
            # -------------------------------------------------

            conn.execute("""
                CREATE OR REPLACE TABLE stg_customers AS
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY TRIM(customer_id)
                            ORDER BY _loaded_at DESC, rowid ASC
                        ) AS _rn
                    FROM raw_customers
                )
                SELECT
                    NULLIF(TRIM(customer_id), '') AS customer_id,
                    NULLIF(TRIM(full_name), '') AS full_name,
                    CASE
                        WHEN email IS NULL OR TRIM(email) = ''
                            THEN NULL
                        WHEN STRPOS(TRIM(email), '@') = 0
                            THEN '***'
                        ELSE
                            LEFT(SPLIT_PART(TRIM(email), '@', 1), 2)
                            || '***@'
                            || SPLIT_PART(TRIM(email), '@', 2)
                    END AS email,
                    CASE
                        WHEN phone IS NULL OR TRIM(phone) = ''
                            THEN NULL
                        WHEN LENGTH(TRIM(phone)) < 4
                            THEN '******'
                        ELSE
                            LEFT(TRIM(phone), 2)
                            || '******'
                            || RIGHT(TRIM(phone), 2)
                    END AS phone,
                    NULLIF(TRIM(city), '') AS city,
                    NULLIF(TRIM(area), '') AS area,
                    NULLIF(TRIM(pincode), '') AS pincode,
                    TRY_CAST(signup_date AS DATE) AS signup_date,
                    UPPER(NULLIF(TRIM(loyalty_tier), ''))
                        AS loyalty_tier,
                    CASE UPPER(TRIM(is_active))
                        WHEN 'Y' THEN TRUE
                        WHEN 'N' THEN FALSE
                        ELSE NULL
                    END AS is_active,
                    _source_file,
                    _run_id,

                    CASE
                        WHEN customer_id IS NULL
                             OR TRIM(customer_id) = ''
                            THEN 'FAIL'
                        WHEN UPPER(TRIM(is_active)) NOT IN ('Y', 'N')
                            THEN 'FAIL'
                        WHEN email IS NULL OR TRIM(email) = ''
                            THEN 'WARN'
                        ELSE 'PASS'
                    END AS dq_status,

                    NULLIF(
                        CONCAT_WS(
                            ';',
                            CASE
                                WHEN customer_id IS NULL
                                     OR TRIM(customer_id) = ''
                                    THEN 'missing_customer_id'
                            END,
                            CASE
                                WHEN UPPER(TRIM(is_active)) NOT IN ('Y', 'N')
                                    THEN 'invalid_is_active'
                            END,
                            CASE
                                WHEN email IS NULL OR TRIM(email) = ''
                                    THEN 'missing_email'
                            END
                        ),
                        ''
                    ) AS failure_reason

                FROM ranked
                WHERE _rn = 1
            """)

            # -------------------------------------------------
            # 3. Stage products: deduplicate and type numeric data
            # -------------------------------------------------

            conn.execute("""
                CREATE OR REPLACE TABLE stg_products AS
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY TRIM(product_id)
                            ORDER BY _loaded_at DESC, rowid ASC
                        ) AS _rn
                    FROM raw_products
                )
                SELECT
                    * EXCLUDE (
                        _loaded_at, _source_file, _run_id, _rn,
                        category, mrp, cost_price
                    ),
                    NULLIF(TRIM(category), '') AS category,
                    TRY_CAST(mrp AS DECIMAL(12, 2)) AS mrp,
                    TRY_CAST(cost_price AS DECIMAL(12, 2))
                        AS cost_price,
                    _source_file,
                    _run_id,

                    CASE
                        WHEN product_id IS NULL
                             OR TRIM(product_id) = ''
                            THEN 'FAIL'
                        WHEN category IS NULL OR TRIM(category) = ''
                            THEN 'FAIL'
                        WHEN TRY_CAST(mrp AS DECIMAL(12, 2)) IS NULL
                            THEN 'FAIL'
                        WHEN TRY_CAST(mrp AS DECIMAL(12, 2)) < 0
                            THEN 'FAIL'
                        WHEN TRY_CAST(
                            cost_price AS DECIMAL(12, 2)
                        ) IS NULL
                            THEN 'FAIL'
                        WHEN TRY_CAST(cost_price AS DECIMAL(12, 2)) < 0
                            THEN 'FAIL'
                        WHEN TRY_CAST(cost_price AS DECIMAL(12, 2))
                             > TRY_CAST(mrp AS DECIMAL(12, 2))
                            THEN 'FAIL'
                        ELSE 'PASS'
                    END AS dq_status,

                    NULLIF(
                        CONCAT_WS(
                            ';',
                            CASE
                                WHEN product_id IS NULL
                                     OR TRIM(product_id) = ''
                                    THEN 'missing_product_id'
                            END,
                            CASE
                                WHEN category IS NULL OR TRIM(category) = ''
                                    THEN 'missing_category'
                            END,
                            CASE
                                WHEN TRY_CAST(mrp AS DECIMAL(12, 2)) IS NULL
                                    THEN 'invalid_mrp'
                            END,
                            CASE
                                WHEN TRY_CAST(mrp AS DECIMAL(12, 2)) < 0
                                    THEN 'negative_mrp'
                            END,
                            CASE
                                WHEN TRY_CAST(
                                    cost_price AS DECIMAL(12, 2)
                                ) IS NULL
                                    THEN 'invalid_cost_price'
                            END,
                            CASE
                                WHEN TRY_CAST(
                                    cost_price AS DECIMAL(12, 2)
                                ) < 0
                                    THEN 'negative_cost_price'
                            END,
                            CASE
                                WHEN TRY_CAST(
                                    cost_price AS DECIMAL(12, 2)
                                ) > TRY_CAST(mrp AS DECIMAL(12, 2))
                                    THEN 'cost_above_mrp'
                            END
                        ),
                        ''
                    ) AS failure_reason

                FROM ranked
                WHERE _rn = 1
            """)

            # -------------------------------------------------
            # 4. Stage stores: deduplicate and standardize zone
            # -------------------------------------------------

            conn.execute("""
                CREATE OR REPLACE TABLE stg_stores AS
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY TRIM(store_id)
                            ORDER BY _loaded_at DESC, rowid ASC
                        ) AS _rn
                    FROM raw_stores
                )
                SELECT
                    * EXCLUDE (
                        _loaded_at, _source_file, _run_id, _rn,
                        zone, capacity_orders_per_day
                    ),
                    UPPER(NULLIF(TRIM(zone), '')) AS zone,
                    TRY_CAST(
                        capacity_orders_per_day AS INTEGER
                    ) AS capacity_orders_per_day,
                    _source_file,
                    _run_id,

                    CASE
                        WHEN store_id IS NULL
                             OR TRIM(store_id) = ''
                            THEN 'FAIL'
                        WHEN zone IS NULL
                             OR UPPER(TRIM(zone)) NOT IN (
                                'NORTH', 'SOUTH', 'EAST', 'WEST'
                             )
                            THEN 'FAIL'
                        WHEN TRY_CAST(
                            capacity_orders_per_day AS INTEGER
                        ) IS NULL
                            THEN 'FAIL'
                        WHEN TRY_CAST(
                            capacity_orders_per_day AS INTEGER
                        ) < 0
                            THEN 'FAIL'
                        ELSE 'PASS'
                    END AS dq_status,

                    NULLIF(
                        CONCAT_WS(
                            ';',
                            CASE
                                WHEN store_id IS NULL
                                     OR TRIM(store_id) = ''
                                    THEN 'missing_store_id'
                            END,
                            CASE
                                WHEN zone IS NULL
                                     OR UPPER(TRIM(zone)) NOT IN (
                                        'NORTH', 'SOUTH', 'EAST', 'WEST'
                                     )
                                    THEN 'missing_or_invalid_zone'
                            END,
                            CASE
                                WHEN TRY_CAST(
                                    capacity_orders_per_day AS INTEGER
                                ) IS NULL
                                    THEN 'invalid_capacity'
                            END,
                            CASE
                                WHEN TRY_CAST(
                                    capacity_orders_per_day AS INTEGER
                                ) < 0
                                    THEN 'negative_capacity'
                            END
                        ),
                        ''
                    ) AS failure_reason

                FROM ranked
                WHERE _rn = 1
            """)

            # -------------------------------------------------
            # 5. Stage payments: deduplicate and standardize
            # -------------------------------------------------

            conn.execute("""
                CREATE OR REPLACE TABLE stg_payments AS
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY TRIM(payment_id)
                            ORDER BY _loaded_at DESC, rowid DESC
                        ) AS rn
                    FROM raw_payments
                ),
                cleaned AS (
                    SELECT
                        TRIM(payment_id) AS payment_id,
                        TRIM(order_id) AS order_id,
                        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,

                        CASE UPPER(TRIM(payment_mode))
                            WHEN 'UPI' THEN 'UPI'
                            WHEN 'CARD' THEN 'Card'
                            WHEN 'CASH ON DELIVERY'
                                THEN 'Cash on Delivery'
                            WHEN 'COD' THEN 'Cash on Delivery'
                            WHEN 'WALLET' THEN 'Wallet'
                            ELSE TRIM(payment_mode)
                        END AS payment_mode,

                        TRY_CAST(amount_paid AS DECIMAL(12, 2))
                            AS amount_paid,

                        UPPER(TRIM(payment_status)) AS payment_status,
                        gateway_ref,
                        _source_file,
                        _run_id
                    FROM ranked
                    WHERE rn = 1
                )
                SELECT *,
                    CASE
                        WHEN payment_id IS NULL OR payment_id = ''
                            THEN 'FAIL'
                        WHEN order_id IS NULL OR order_id = ''
                            THEN 'FAIL'
                        WHEN amount_paid IS NULL OR amount_paid < 0
                            THEN 'FAIL'
                        WHEN payment_mode NOT IN (
                            'UPI', 'Card', 'Cash on Delivery', 'Wallet'
                        )
                            THEN 'FAIL'
                        ELSE 'PASS'
                    END AS dq_status,

                    NULLIF(
                        CONCAT_WS(
                            ';',
                            CASE
                                WHEN payment_id IS NULL OR payment_id = ''
                                    THEN 'missing_payment_id'
                            END,
                            CASE
                                WHEN order_id IS NULL OR order_id = ''
                                    THEN 'missing_order_id'
                            END,
                            CASE
                                WHEN amount_paid IS NULL OR amount_paid < 0
                                    THEN 'invalid_amount_paid'
                            END,
                            CASE
                                WHEN payment_mode NOT IN (
                                    'UPI', 'Card',
                                    'Cash on Delivery', 'Wallet'
                                )
                                    THEN 'invalid_payment_mode'
                            END
                        ),
                        ''
                    ) AS failure_reason
                FROM cleaned
            """)

            # -------------------------------------------------
            # 6. Stage orders: latest correction wins
            # -------------------------------------------------

            conn.execute("""
                CREATE OR REPLACE TEMP TABLE stg_orders_source AS
                WITH ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY TRIM(order_id)
                            ORDER BY
                                _loaded_at DESC,
                                CASE
                                    WHEN _source_file =
                                      'orders_2026-10-01_increment.csv'
                                        THEN 1
                                    ELSE 0
                                END DESC,
                                rowid DESC
                        ) AS rn
                    FROM raw_orders
                ),
                typed AS (
                    SELECT
                        TRIM(order_id) AS order_id,
                        TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
                        NULLIF(TRIM(customer_id), '') AS customer_id,
                        NULLIF(TRIM(store_id), '') AS store_id,
                        NULLIF(TRIM(product_id), '') AS product_id,
                        TRY_CAST(quantity AS INTEGER) AS quantity,
                        TRY_CAST(unit_price AS DECIMAL(12, 2))
                            AS unit_price,
                        TRY_CAST(discount_pct AS DECIMAL(8, 2))
                            AS discount_pct,
                        TRY_CAST(order_amount AS DECIMAL(12, 2))
                            AS order_amount,

                        CASE UPPER(TRIM(payment_mode))
                            WHEN 'UPI' THEN 'UPI'
                            WHEN 'CARD' THEN 'Card'
                            WHEN 'CASH ON DELIVERY'
                                THEN 'Cash on Delivery'
                            WHEN 'COD' THEN 'Cash on Delivery'
                            WHEN 'WALLET' THEN 'Wallet'
                            ELSE TRIM(payment_mode)
                        END AS payment_mode,

                        UPPER(TRIM(order_status)) AS order_status,
                        TRIM(channel) AS channel,
                        TRY_CAST(delivery_minutes AS INTEGER)
                            AS delivery_minutes,
                        _source_file,
                        _run_id

                    FROM ranked
                    WHERE rn = 1
                ),
                calculated AS (
                    SELECT *,
                        ROUND(
                            quantity * unit_price
                            * (1 - discount_pct / 100.0),
                            2
                        ) AS calculated_amount
                    FROM typed
                ),
                evaluated AS (
                    SELECT
                        o.*,

                        NULLIF(
                            CONCAT_WS(
                                ';',

                                CASE
                                    WHEN o.order_id IS NULL OR o.order_id = ''
                                        THEN 'missing_order_id'
                                END,

                                CASE
                                    WHEN o.order_ts IS NULL
                                        THEN 'invalid_or_missing_order_ts'
                                END,

                                CASE
                                    WHEN o.customer_id IS NULL
                                        THEN 'missing_customer_id'
                                END,

                                CASE
                                    WHEN o.store_id IS NULL
                                        THEN 'missing_store_id'
                                END,

                                CASE
                                    WHEN o.product_id IS NULL
                                        THEN 'missing_product_id'
                                END,

                                CASE
                                    WHEN o.quantity IS NULL OR o.quantity <= 0
                                        THEN 'invalid_quantity'
                                END,

                                CASE
                                    WHEN o.unit_price IS NULL OR o.unit_price < 0
                                        THEN 'invalid_unit_price'
                                END,

                                CASE
                                    WHEN o.discount_pct IS NULL
                                         OR o.discount_pct < 0
                                         OR o.discount_pct > 100
                                        THEN 'invalid_discount_pct'
                                END,

                                CASE
                                    WHEN o.calculated_amount IS NULL
                                        THEN 'invalid_calculated_amount'
                                END,

                                CASE
                                    WHEN o.order_status NOT IN (
                                        'DELIVERED', 'CANCELLED', 'RETURNED'
                                    )
                                        THEN 'invalid_order_status'
                                END,

                                CASE
                                    WHEN o.payment_mode NOT IN (
                                        'UPI', 'Card',
                                        'Cash on Delivery', 'Wallet'
                                    )
                                        THEN 'invalid_payment_mode'
                                END,

                                CASE
                                    WHEN o.order_status = 'DELIVERED'
                                         AND o.delivery_minutes IS NULL
                                        THEN 'delivered_missing_delivery_minutes'
                                END,

                                CASE
                                    WHEN o.delivery_minutes IS NOT NULL
                                         AND (
                                            o.delivery_minutes < 8
                                            OR o.delivery_minutes > 35
                                         )
                                        THEN 'delivery_outside_8_to_35_minutes'
                                END,

                                CASE
                                    WHEN o.order_amount IS NOT NULL
                                         AND o.calculated_amount IS NOT NULL
                                         AND ABS(
                                            o.order_amount - o.calculated_amount
                                         ) > 0.02
                                        THEN 'order_amount_mismatch'
                                END,

                                CASE
                                    WHEN o.customer_id IS NOT NULL
                                         AND c.customer_id IS NULL
                                        THEN 'unknown_customer_id'
                                END,

                                CASE
                                    WHEN o.store_id IS NOT NULL
                                         AND s.store_id IS NULL
                                        THEN 'unknown_store_id'
                                END,

                                CASE
                                    WHEN o.product_id IS NOT NULL
                                         AND p.product_id IS NULL
                                        THEN 'unknown_product_id'
                                END
                            ),
                            ''
                        ) AS failure_reason,

                        p.cost_price

                    FROM calculated o

                    LEFT JOIN stg_customers c
                        ON o.customer_id = c.customer_id

                    LEFT JOIN stg_stores s
                        ON o.store_id = s.store_id

                    LEFT JOIN stg_products p
                        ON o.product_id = p.product_id
                )
                SELECT
                    order_id,
                    order_ts,
                    customer_id,
                    store_id,
                    product_id,
                    quantity,
                    unit_price,
                    discount_pct,
                    order_amount,
                    calculated_amount,
                    payment_mode,
                    order_status,
                    channel,
                    delivery_minutes,

                    CASE
                        WHEN order_status = 'DELIVERED'
                             AND failure_reason IS NULL
                            THEN calculated_amount
                        ELSE 0
                    END AS net_revenue,

                    CASE
                        WHEN order_status = 'DELIVERED'
                             AND failure_reason IS NULL
                             AND cost_price IS NOT NULL
                            THEN ROUND(
                                calculated_amount - quantity * cost_price,
                                2
                            )
                        ELSE 0
                    END AS gross_margin,

                    CASE
                        WHEN failure_reason IS NULL THEN 'PASS'
                        ELSE 'FAIL'
                    END AS dq_status,

                    failure_reason,
                    _source_file,
                    _run_id

                FROM evaluated
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS stg_orders AS
                SELECT * FROM stg_orders_source WHERE FALSE
            """)
            with open(
                os.path.join(
                    project_dir, "include", "sql", "elt_merge_orders.sql"
                ),
                encoding="utf-8",
            ) as sql_file:
                merge_sql = sql_file.read()
            merge_actions = conn.execute(merge_sql).fetchall()
            inserted = sum(action == "INSERT" for action, _ in merge_actions)
            updated = sum(action == "UPDATE" for action, _ in merge_actions)

            # -------------------------------------------------
            # 7. Quarantine failed staging rows
            # -------------------------------------------------

            os.makedirs(quarantine_dir, exist_ok=True)

            safe_run_id = (
                current_run_id
                .replace(":", "_")
                .replace("+", "_")
                .replace("/", "_")
                .replace("\\", "_")
            )

            staging_tables = [
                "stg_orders",
                "stg_payments",
                "stg_customers",
                "stg_products",
                "stg_stores",
            ]

            for table_name in staging_tables:
                failed_df = conn.execute(
                    f"""
                    SELECT *
                    FROM {table_name}
                    WHERE dq_status = 'FAIL'
                    """
                ).df()

                if not failed_df.empty:
                    output_path = os.path.join(
                        quarantine_dir,
                        f"{table_name}_failed_{safe_run_id}.csv",
                    )
                    failed_df.to_csv(output_path, index=False)

            # -------------------------------------------------
            # 8. Save six data-quality dimensions for this run
            # -------------------------------------------------

            def record_dq_check(
                check_name,
                dimension,
                severity,
                table_name,
                failed_condition,
            ):
                rows_checked = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]

                rows_failed = conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE {failed_condition}
                    """
                ).fetchone()[0]

                conn.execute(
                    """
                    INSERT INTO dq_results
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        current_run_id,
                        current_dag_id,
                        check_name,
                        dimension,
                        severity,
                        rows_checked,
                        rows_failed,
                        "PASS" if rows_failed == 0 else "FAIL",
                        pendulum.now(IST).naive(),
                    ],
                )

            record_dq_check(
                "order_required_fields",
                "Completeness",
                "Critical",
                "stg_orders",
                "order_id IS NULL OR order_ts IS NULL "
                "OR customer_id IS NULL OR store_id IS NULL "
                "OR product_id IS NULL",
            )

            record_dq_check(
                "unique_staged_order_id",
                "Uniqueness",
                "Critical",
                "stg_orders",
                "order_id IN ("
                "SELECT order_id FROM stg_orders "
                "GROUP BY order_id HAVING COUNT(*) > 1)",
            )

            record_dq_check(
                "order_business_rules",
                "Validity",
                "Critical",
                "stg_orders",
                "dq_status = 'FAIL'",
            )

            record_dq_check(
                "source_amount_matches_calculation",
                "Accuracy",
                "Warning",
                "stg_orders",
                "order_amount IS NOT NULL "
                "AND calculated_amount IS NOT NULL "
                "AND ABS(order_amount - calculated_amount) > 0.02",
            )

            record_dq_check(
                "order_reference_integrity",
                "Consistency",
                "Critical",
                "stg_orders",
                "failure_reason LIKE '%unknown_customer_id%' "
                "OR failure_reason LIKE '%unknown_store_id%' "
                "OR failure_reason LIKE '%unknown_product_id%'",
            )

            record_dq_check(
                "no_future_order_timestamps",
                "Timeliness",
                "Warning",
                "stg_orders",
                "order_ts > CURRENT_TIMESTAMP",
            )

            record_dq_check(
                "required_payment_fields",
                "Completeness",
                "Critical",
                "stg_payments",
                "payment_id IS NULL OR order_id IS NULL "
                "OR payment_ts IS NULL OR amount_paid IS NULL",
            )
            record_dq_check(
                "unique_payment_id",
                "Uniqueness",
                "Critical",
                "stg_payments",
                "payment_id IN ("
                "SELECT payment_id FROM stg_payments "
                "GROUP BY payment_id HAVING COUNT(*) > 1)",
            )
            record_dq_check(
                "valid_payment_values",
                "Validity",
                "Critical",
                "stg_payments",
                "dq_status = 'FAIL'",
            )
            record_dq_check(
                "payment_order_reference",
                "Consistency",
                "Critical",
                "stg_payments",
                "order_id NOT IN (SELECT order_id FROM stg_orders)",
            )
            record_dq_check(
                "missing_successful_gateway_reference",
                "Completeness",
                "Warning",
                "stg_payments",
                "payment_status = 'SUCCESS' "
                "AND (gateway_ref IS NULL OR TRIM(gateway_ref) = '')",
            )
            record_dq_check(
                "payment_timestamp_not_future",
                "Timeliness",
                "Warning",
                "stg_payments",
                "payment_ts > CURRENT_TIMESTAMP",
            )

            record_dq_check(
                "required_customer_key",
                "Completeness",
                "Critical",
                "stg_customers",
                "customer_id IS NULL OR TRIM(customer_id) = ''",
            )
            record_dq_check(
                "unique_customer_id",
                "Uniqueness",
                "Critical",
                "stg_customers",
                "customer_id IN ("
                "SELECT customer_id FROM stg_customers "
                "GROUP BY customer_id HAVING COUNT(*) > 1)",
            )
            record_dq_check(
                "customer_record_validity",
                "Validity",
                "Warning",
                "stg_customers",
                "dq_status = 'FAIL' OR failure_reason LIKE '%invalid_email%'",
            )

            record_dq_check(
                "required_product_category",
                "Completeness",
                "Critical",
                "stg_products",
                "category IS NULL OR TRIM(category) = ''",
            )
            record_dq_check(
                "unique_product_id",
                "Uniqueness",
                "Critical",
                "stg_products",
                "product_id IN ("
                "SELECT product_id FROM stg_products "
                "GROUP BY product_id HAVING COUNT(*) > 1)",
            )
            record_dq_check(
                "product_price_validity",
                "Accuracy",
                "Critical",
                "stg_products",
                "mrp IS NULL OR cost_price IS NULL "
                "OR cost_price < 0 OR mrp < 0 OR cost_price > mrp",
            )

            record_dq_check(
                "required_store_zone",
                "Completeness",
                "Critical",
                "stg_stores",
                "zone IS NULL OR zone NOT IN "
                "('NORTH', 'SOUTH', 'EAST', 'WEST')",
            )
            record_dq_check(
                "unique_store_id",
                "Uniqueness",
                "Critical",
                "stg_stores",
                "store_id IN ("
                "SELECT store_id FROM stg_stores "
                "GROUP BY store_id HAVING COUNT(*) > 1)",
            )
            record_dq_check(
                "store_capacity_validity",
                "Validity",
                "Warning",
                "stg_stores",
                "capacity_orders_per_day IS NULL "
                "OR capacity_orders_per_day < 0",
            )

            return (
                "ELT staging tables and DQ results written; "
                f"orders MERGE inserted={inserted}, updated={updated}."
            )

        finally:
            conn.close()

    @task(task_id="build_marts")
    def build_marts():
        import duckdb

        with duckdb.connect(db_path) as conn:
            with open(
                os.path.join(project_dir, "include", "sql", "elt_marts.sql"),
                encoding="utf-8",
            ) as sql_file:
                for statement in sql_file.read().split(";"):
                    if statement.strip():
                        conn.execute(statement)
            conn.execute("""
                CREATE OR REPLACE TABLE mart_rejected_rows_today AS
                SELECT
                    source_file,
                    failure_reason,
                    SUM(rejected_rows) AS rejected_rows
                FROM (
                    SELECT _source_file AS source_file, failure_reason,
                           COUNT(*) AS rejected_rows
                    FROM stg_orders
                    WHERE dq_status = 'FAIL'
                    GROUP BY _source_file, failure_reason
                    UNION ALL
                    SELECT _source_file AS source_file, failure_reason,
                           COUNT(*) AS rejected_rows
                    FROM stg_payments
                    WHERE dq_status = 'FAIL'
                    GROUP BY _source_file, failure_reason
                    UNION ALL
                    SELECT _source_file AS source_file, failure_reason,
                           COUNT(*) AS rejected_rows
                    FROM stg_customers
                    WHERE dq_status = 'FAIL'
                    GROUP BY _source_file, failure_reason
                    UNION ALL
                    SELECT _source_file AS source_file, failure_reason,
                           COUNT(*) AS rejected_rows
                    FROM stg_products
                    WHERE dq_status = 'FAIL'
                    GROUP BY _source_file, failure_reason
                    UNION ALL
                    SELECT _source_file AS source_file, failure_reason,
                           COUNT(*) AS rejected_rows
                    FROM stg_stores
                    WHERE dq_status = 'FAIL'
                    GROUP BY _source_file, failure_reason
                ) rejected
                GROUP BY source_file, failure_reason
            """)

        return "ELT mart tables built."

    @task(task_id="audit_run", trigger_rule=TriggerRule.ALL_DONE)
    def audit_run():
        import duckdb

        context = get_current_context()
        current_run_id = context["run_id"]
        current_dag_id = context["dag"].dag_id
        dag_run = context["dag_run"]
        upstream_task_ids = [
            "load_orders",
            "load_payments",
            "load_customers",
            "load_products",
            "load_stores",
            "transform_staging",
            "build_marts",
        ]
        upstream_results = context["ti"].xcom_pull(
            task_ids=upstream_task_ids
        )
        run_status = (
            "SUCCESS"
            if len(upstream_results) == len(upstream_task_ids)
            and all(result is not None for result in upstream_results)
            else "FAILED"
        )
        start_time = (
            dag_run.start_date or pendulum.now(IST)
        )
        end_time = pendulum.now(IST)

        with duckdb.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    rows_extracted BIGINT,
                    rows_loaded BIGINT,
                    rows_quarantined BIGINT,
                    status VARCHAR
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dq_results (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    check_name VARCHAR,
                    dimension VARCHAR,
                    severity VARCHAR,
                    rows_checked BIGINT,
                    rows_failed BIGINT,
                    status VARCHAR,
                    checked_at TIMESTAMP
                )
            """)

            table_names = {
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()
            }

            def count_rows(table_name, condition=None, params=None):
                if table_name not in table_names:
                    return 0
                query = f"SELECT COUNT(*) FROM {table_name}"
                if condition:
                    query += f" WHERE {condition}"
                return conn.execute(query, params or []).fetchone()[0]

            raw_tables = (
                "raw_orders",
                "raw_payments",
                "raw_customers",
                "raw_products",
                "raw_stores",
            )
            staging_tables = (
                "stg_orders",
                "stg_payments",
                "stg_customers",
                "stg_products",
                "stg_stores",
            )
            rows_extracted = sum(
                count_rows(table_name, "_run_id = ?", [current_run_id])
                for table_name in raw_tables
            )
            rows_loaded = sum(
                count_rows(table_name)
                for table_name in staging_tables
            )
            rows_quarantined = sum(
                count_rows(
                    table_name,
                    "dq_status = 'FAIL'",
                )
                for table_name in staging_tables
            )

            conn.execute(
                "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
                [current_run_id, current_dag_id],
            )
            conn.execute(
                "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    current_run_id,
                    current_dag_id,
                    start_time,
                    end_time,
                    rows_extracted,
                    rows_loaded,
                    rows_quarantined,
                    run_status,
                ],
            )

            dq_count = conn.execute(
                "SELECT COUNT(*) FROM dq_results WHERE run_id = ? AND dag_id = ?",
                [current_run_id, current_dag_id],
            ).fetchone()[0]
            if dq_count == 0:
                conn.execute(
                    "INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        current_run_id,
                        current_dag_id,
                        "pipeline_execution",
                        "Timeliness",
                        "Critical",
                        rows_extracted,
                        0 if run_status == "SUCCESS" else 1,
                        run_status,
                        end_time.naive(),
                    ],
                )

    # ---------------------------------------------------------
    # Run sequentially to reduce DuckDB concurrent-write issues.
    # ---------------------------------------------------------

    with TaskGroup(
        group_id="load_raw_sources",
        prefix_group_id=False,
    ) as raw_load_group:
        t1 = load_orders()
        t2 = load_payments()
        t3 = load_customers()
        t4 = load_products()
        t5 = load_stores()
        t1 >> t2 >> t3 >> t4 >> t5

    t6 = transform_staging()
    t7 = audit_run()
    t8 = build_marts()

    raw_load_group >> t6 >> t8 >> t7


dag_instance = nammamart_elt_surender_singh()
