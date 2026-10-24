"""NammaMart ELT pipeline.

Extract -> Load raw -> Transform in DuckDB SQL -> Build marts -> Audit
Paste this file into:
    dags/nammamart_elt_donald_fernandes.py
"""
from __future__ import annotations

import os
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

# These are Docker-container paths. Astro maps the Windows assignment1
# project folders to these paths.
DATA_DIR = "/usr/local/airflow/include/data/nammamart"
DB_PATH = "/usr/local/airflow/include/warehouse_donald_fernandes.duckdb"

DEFAULT_ARGS = {
    "owner": "donald-fernandes",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

FILES = {
    "raw_orders": "orders.csv",
    "raw_payments": "payments.csv",
    "raw_customers": "customers.csv",
    "raw_products": "products.csv",
    "raw_stores": "stores.csv",
}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove BOM/space problems from CSV column names."""
    out = df.copy()
    out.columns = (
        out.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    return out


def _load_raw(
    con,
    table: str,
    filename: str,
    run_id: str,
    append: bool = False,
) -> int:
    """Load one CSV into a raw DuckDB table.

    Normal source tables are rebuilt on every run so an old metadata-only
    table cannot cause missing-column errors. The increment feed is appended
    to raw_orders to preserve raw history.
    """
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = _normalise_columns(df)

    # Add lineage columns after normalising the source schema.
    df["_loaded_at"] = pendulum.now("UTC").to_iso8601_string()
    df["_source_file"] = filename
    df["_run_id"] = run_id

    view_name = f"{table}_df"
    con.register(view_name, df)
    try:
        if not append:
            # Important fix: replace stale raw tables from earlier runs.
            # CREATE TABLE IF NOT EXISTS would preserve a bad table that
            # contains only _loaded_at/_source_file/_run_id.
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(
                f"CREATE TABLE {table} AS "
                f"SELECT * FROM {view_name} WHERE 1=0"
            )

        con.execute(f"INSERT INTO {table} SELECT * FROM {view_name}")
    finally:
        con.unregister(view_name)

    return len(df)


@dag(
    dag_id="nammamart_elt_donald_fernandes",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "elt", "donald-fernandes"],
)
def nammamart_elt():
    @task
    def extract_sources() -> dict:
        required = list(FILES.values()) + [
            "orders_2026-10-01_increment.csv"
        ]
        for filename in required:
            path = os.path.join(DATA_DIR, filename)
            if not os.path.exists(path):
                raise FileNotFoundError(path)

        return {
            key: os.path.join(DATA_DIR, value)
            for key, value in FILES.items()
        }

    @task
    def load_raw(paths: dict) -> dict:
        context = get_current_context()
        run_id = context["run_id"].replace(":", "_")
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        con = duckdb.connect(DB_PATH)
        counts = {}
        try:
            # Rebuild each base raw table from the current CSV schema.
            for table, filename in FILES.items():
                counts[table] = _load_raw(
                    con, table, filename, run_id, append=False
                )

            # Preserve the increment feed in the same raw_orders history.
            counts["raw_orders_increment"] = _load_raw(
                con,
                "raw_orders",
                "orders_2026-10-01_increment.csv",
                run_id,
                append=True,
            )
        finally:
            con.close()

        return {
            "run_id": run_id,
            "rows_extracted": sum(counts.values()),
            "counts": counts,
        }

    @task
    def transform_sql(raw_result: dict) -> dict:
        con = duckdb.connect(DB_PATH)
        try:
            # Current-state orders: newest loaded record wins by order_id.
            con.execute(
                """
                CREATE OR REPLACE TABLE stg_orders AS
                WITH latest AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY order_id
                               ORDER BY _loaded_at DESC
                           ) AS rn
                    FROM raw_orders
                ), typed AS (
                    SELECT
                        order_id,
                        TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
                        customer_id,
                        store_id,
                        product_id,
                        TRY_CAST(quantity AS INTEGER) AS quantity,
                        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
                        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
                        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
                        LOWER(TRIM(payment_mode)) AS payment_mode_raw,
                        LOWER(TRIM(order_status)) AS order_status_raw,
                        channel,
                        TRY_CAST(delivery_minutes AS INTEGER)
                            AS delivery_minutes,
                        _loaded_at,
                        _source_file,
                        _run_id
                    FROM latest
                    WHERE rn = 1
                )
                SELECT *,
                    CASE
                        WHEN order_status_raw = 'delivered'
                            THEN 'DELIVERED'
                        WHEN order_status_raw = 'cancelled'
                            THEN 'CANCELLED'
                        WHEN order_status_raw = 'returned'
                            THEN 'RETURNED'
                        ELSE UPPER(order_status_raw)
                    END AS order_status,
                    CASE
                        WHEN payment_mode_raw = 'upi' THEN 'UPI'
                        WHEN payment_mode_raw = 'card' THEN 'Card'
                        WHEN payment_mode_raw IN
                            ('cod', 'cash on delivery')
                            THEN 'Cash on Delivery'
                        WHEN payment_mode_raw = 'wallet' THEN 'Wallet'
                        ELSE payment_mode_raw
                    END AS payment_mode,
                    CASE
                        WHEN order_id IS NULL OR order_id = ''
                            THEN 'missing order_id'
                        WHEN order_ts IS NULL
                            THEN 'invalid order_ts'
                        WHEN order_status_raw NOT IN
                            ('delivered', 'cancelled', 'returned')
                            THEN 'invalid order_status'
                        WHEN quantity IS NULL OR quantity <= 0
                            THEN 'invalid quantity'
                        WHEN discount_pct IS NULL
                             OR discount_pct < 0
                             OR discount_pct > 100
                            THEN 'invalid discount_pct'
                        WHEN ABS(
                            order_amount - ROUND(
                                quantity * unit_price
                                * (1 - discount_pct / 100), 2
                            )
                        ) > 0.01
                            THEN 'order_amount mismatch'
                        WHEN order_status_raw = 'delivered'
                             AND delivery_minutes IS NULL
                            THEN 'delivered order missing delivery_minutes'
                        WHEN order_status_raw = 'delivered'
                             AND delivery_minutes > 120
                            THEN 'delivery_minutes above 120'
                        ELSE NULL
                    END AS failure_reason,
                    CASE
                        WHEN order_id IS NULL OR order_id = ''
                             OR order_ts IS NULL
                             OR order_status_raw NOT IN
                                ('delivered', 'cancelled', 'returned')
                             OR quantity IS NULL OR quantity <= 0
                             OR discount_pct IS NULL
                             OR discount_pct < 0 OR discount_pct > 100
                             OR ABS(
                                order_amount - ROUND(
                                    quantity * unit_price
                                    * (1 - discount_pct / 100), 2
                                )
                             ) > 0.01
                             OR (
                                order_status_raw = 'delivered'
                                AND delivery_minutes IS NULL
                             )
                             OR (
                                order_status_raw = 'delivered'
                                AND delivery_minutes > 120
                             )
                            THEN 'FAIL'
                        ELSE 'PASS'
                    END AS dq_status
                FROM typed
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE stg_customers AS
                WITH d AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY customer_id
                               ORDER BY _loaded_at DESC
                           ) AS rn
                    FROM raw_customers
                )
                SELECT
                    customer_id,
                    full_name,
                    CASE
                        WHEN POSITION('@' IN email) > 0
                            THEN LEFT(email, 2) || '***@'
                                 || SPLIT_PART(email, '@', 2)
                        ELSE ''
                    END AS email_masked,
                    CASE
                        WHEN LENGTH(
                            REGEXP_REPLACE(phone, '[^0-9]', '', 'g')
                        ) >= 4
                            THEN LEFT(
                                REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 2
                            ) || '******' || RIGHT(
                                REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 2
                            )
                        ELSE ''
                    END AS phone_masked,
                    city,
                    area,
                    pincode,
                    signup_date,
                    CASE
                        WHEN LOWER(loyalty_tier) = 'bronze'
                            THEN 'Bronze'
                        WHEN LOWER(loyalty_tier) = 'silver'
                            THEN 'Silver'
                        WHEN LOWER(loyalty_tier) = 'gold'
                            THEN 'Gold'
                        WHEN LOWER(loyalty_tier) = 'platinum'
                            THEN 'Platinum'
                        ELSE loyalty_tier
                    END AS loyalty_tier,
                    is_active
                FROM d
                WHERE rn = 1
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE stg_products AS
                WITH d AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY product_id
                               ORDER BY _loaded_at DESC
                           ) AS rn
                    FROM raw_products
                )
                SELECT
                    product_id,
                    product_name,
                    category,
                    sub_category,
                    brand,
                    TRY_CAST(mrp AS DOUBLE) AS mrp,
                    TRY_CAST(cost_price AS DOUBLE) AS cost_price,
                    unit,
                    is_perishable,
                    launch_date
                FROM d
                WHERE rn = 1
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE stg_stores AS
                WITH d AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY store_id
                               ORDER BY _loaded_at DESC
                           ) AS rn
                    FROM raw_stores
                )
                SELECT *
                FROM d
                WHERE rn = 1
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE stg_payments AS
                SELECT
                    payment_id,
                    order_id,
                    TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
                    CASE
                        WHEN LOWER(TRIM(payment_mode)) = 'upi'
                            THEN 'UPI'
                        WHEN LOWER(TRIM(payment_mode)) = 'card'
                            THEN 'Card'
                        WHEN LOWER(TRIM(payment_mode)) IN
                            ('cod', 'cash on delivery')
                            THEN 'Cash on Delivery'
                        WHEN LOWER(TRIM(payment_mode)) = 'wallet'
                            THEN 'Wallet'
                        ELSE payment_mode
                    END AS payment_mode,
                    TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
                    payment_status,
                    gateway_ref
                FROM raw_payments
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE mart_daily_store_kpis AS
                SELECT
                    CAST(o.order_ts AS DATE) AS order_date,
                    s.zone,
                    s.store_name,
                    ROUND(
                        SUM(
                            CASE
                                WHEN o.order_status = 'DELIVERED'
                                    THEN o.order_amount
                                ELSE 0
                            END
                        ), 2
                    ) AS net_revenue,
                    COUNT(*) AS order_count
                FROM stg_orders o
                LEFT JOIN stg_stores s USING (store_id)
                WHERE o.dq_status = 'PASS'
                GROUP BY 1, 2, 3
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE mart_category_margin AS
                SELECT
                    p.category,
                    ROUND(
                        SUM(
                            CASE
                                WHEN o.order_status = 'DELIVERED'
                                    THEN o.order_amount
                                ELSE 0
                            END
                        ), 2
                    ) AS revenue,
                    ROUND(
                        SUM(
                            CASE
                                WHEN o.order_status = 'DELIVERED'
                                    THEN o.order_amount
                                         - o.quantity * p.cost_price
                                ELSE 0
                            END
                        ), 2
                    ) AS gross_margin
                FROM stg_orders o
                LEFT JOIN stg_products p USING (product_id)
                WHERE o.dq_status = 'PASS'
                GROUP BY 1
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE mart_delivery_sla AS
                SELECT
                    s.store_id,
                    s.store_name,
                    AVG(o.delivery_minutes)
                        AS average_delivery_minutes,
                    100.0 * AVG(
                        CASE
                            WHEN o.delivery_minutes <= 15 THEN 1
                            ELSE 0
                        END
                    ) AS pct_within_15_min
                FROM stg_orders o
                LEFT JOIN stg_stores s USING (store_id)
                WHERE o.dq_status = 'PASS'
                  AND o.order_status = 'DELIVERED'
                GROUP BY 1, 2
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE mart_payment_reconciliation AS
                SELECT
                    p.order_id,
                    p.payment_id,
                    p.amount_paid,
                    o.order_amount,
                    CASE
                        WHEN o.order_id IS NULL
                            THEN 'ORDER_NOT_FOUND'
                        WHEN p.payment_status = 'SUCCESS'
                             AND ABS(p.amount_paid - o.order_amount) > 0.01
                            THEN 'SUCCESS_AMOUNT_MISMATCH'
                        ELSE 'MATCH'
                    END AS reconciliation_reason
                FROM stg_payments p
                LEFT JOIN stg_orders o USING (order_id)
                WHERE o.order_id IS NULL
                   OR (
                        p.payment_status = 'SUCCESS'
                        AND ABS(p.amount_paid - o.order_amount) > 0.01
                   )
                """
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE mart_top_customers AS
                SELECT
                    o.customer_id,
                    c.full_name,
                    c.loyalty_tier,
                    ROUND(
                        SUM(
                            CASE
                                WHEN o.order_status = 'DELIVERED'
                                    THEN o.order_amount
                                ELSE 0
                            END
                        ), 2
                    ) AS spend
                FROM stg_orders o
                LEFT JOIN stg_customers c USING (customer_id)
                WHERE o.dq_status = 'PASS'
                GROUP BY 1, 2, 3
                ORDER BY spend DESC
                LIMIT 10
                """
            )

            con.execute(
                """
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
                """
            )
            con.execute(
                """
                INSERT INTO dq_results
                SELECT
                    ?,
                    'nammamart_elt_donald_fernandes',
                    'stg_orders dq_status',
                    'validity',
                    'Critical',
                    COUNT(*),
                    SUM(CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END),
                    CASE
                        WHEN SUM(
                            CASE WHEN dq_status = 'FAIL' THEN 1 ELSE 0 END
                        ) = 0 THEN 'PASS'
                        ELSE 'FAIL'
                    END,
                    CURRENT_TIMESTAMP
                FROM stg_orders
                """,
                [raw_result["run_id"]],
            )
        finally:
            con.close()

        return raw_result

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run(raw_result=None):
        con = duckdb.connect(DB_PATH)
        context = get_current_context()
        run_id = context["run_id"].replace(":", "_")
        try:
            con.execute(
                """
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
                """
            )

            has_staging = con.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = 'stg_orders'
                """
            ).fetchone()[0]

            if has_staging:
                rows_loaded = int(
                    con.execute(
                        """
                        SELECT COUNT(*)
                        FROM stg_orders
                        WHERE dq_status = 'PASS'
                        """
                    ).fetchone()[0]
                )
                rows_quarantined = int(
                    con.execute(
                        """
                        SELECT COUNT(*)
                        FROM stg_orders
                        WHERE dq_status = 'FAIL'
                        """
                    ).fetchone()[0]
                )
            else:
                rows_loaded = 0
                rows_quarantined = 0

            rows_extracted = int(
                (raw_result or {}).get("rows_extracted", 0)
            )
            dag_run = context["dag_run"]
            status = getattr(dag_run, "state", None) or "running"

            con.execute(
                """
                INSERT INTO pipeline_runs
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    context["dag"].dag_id,
                    dag_run.start_date,
                    pendulum.now("UTC"),
                    rows_extracted,
                    rows_loaded,
                    rows_quarantined,
                    status,
                ],
            )
        finally:
            con.close()

    nam_paths = extract_sources()
    nam_raw_result = load_raw(nam_paths)
    nam_transformed = transform_sql(nam_raw_result)
    audit_run(nam_transformed)


nammamart_elt()
