"""NammaMart ELT pipeline: append untouched CSV rows, then transform in DuckDB."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pendulum
from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.utils.trigger_rule import TriggerRule


INCLUDE_PATH = Path(__file__).resolve().parents[1] / "include"
sys.path.insert(0, str(INCLUDE_PATH))

from nammamart_common import (  # noqa: E402
    DATA_DIR,
    SQL_DIR,
    WAREHOUSE_DB,
    ensure_audit_tables,
    read_source,
    safe_run_id,
)


DAG_ID = "nammamart_elt_Tusshar_Tikoo"
DEFAULT_ARGS = {
    "owner": "nammamart-fde",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=2),
}

RAW_COLUMNS = {
    "raw_orders": [
        "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
        "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
        "channel", "delivery_minutes",
    ],
    "raw_payments": [
        "payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
        "payment_status", "gateway_ref",
    ],
    "raw_customers": [
        "customer_id", "full_name", "email", "phone", "city", "area", "pincode",
        "signup_date", "loyalty_tier", "is_active",
    ],
    "raw_products": [
        "product_id", "product_name", "category", "sub_category", "brand", "mrp",
        "cost_price", "unit", "is_perishable", "launch_date",
    ],
    "raw_stores": [
        "store_id", "store_name", "zone", "area", "pincode", "open_date",
        "manager_name", "capacity_orders_per_day",
    ],
}


def _source_result(filename: str) -> dict:
    path = DATA_DIR / filename
    frame = read_source(path)
    return {
        "path": str(path), "filename": filename, "rows": len(frame),
        "columns": list(frame.columns),
    }


def _run_sql_file(con, filename: str) -> None:
    path = SQL_DIR / filename
    sql_text = path.read_text(encoding="utf-8")
    for statement in _split_sql_statements(sql_text):
        con.execute(statement)


def _split_sql_statements(sql_text: str) -> list[str]:
    """Split SQL on statement terminators, leaving quoted semicolons intact."""
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    line_comment = False
    block_comment = False
    index = 0

    while index < len(sql_text):
        char = sql_text[index]
        next_char = sql_text[index + 1] if index + 1 < len(sql_text) else ""

        if line_comment:
            current.append(char)
            if char == "\n":
                line_comment = False
        elif block_comment:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                index += 1
                block_comment = False
        elif quote:
            current.append(char)
            if char == quote:
                if next_char == quote:
                    current.append(next_char)
                    index += 1
                else:
                    quote = None
        elif char == "-" and next_char == "-":
            current.extend((char, next_char))
            index += 1
            line_comment = True
        elif char == "/" and next_char == "*":
            current.extend((char, next_char))
            index += 1
            block_comment = True
        elif char in ("'", '"'):
            quote = char
            current.append(char)
        elif char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current.clear()
        else:
            current.append(char)
        index += 1

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


@dag(
    dag_id=DAG_ID,
    description="NammaMart raw, staging and mart ELT layers in DuckDB",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "assignment", "elt"],
)
def nammamart_elt_Tusshar_Tikoo():
    @task
    def extract_orders():
        result = _source_result("orders.csv")
        increment = DATA_DIR / "orders_2026-10-01_increment.csv"
        if increment.exists():
            result["increment"] = _source_result(increment.name)
        return result

    @task
    def extract_payments():
        return _source_result("payments.csv")

    @task
    def extract_customers():
        return _source_result("customers.csv")

    @task
    def extract_products():
        return _source_result("products.csv")

    @task
    def extract_stores():
        return _source_result("stores.csv")

    @task
    def load_raw(
        order_source: dict,
        payment_source: dict,
        customer_source: dict,
        product_source: dict,
        store_source: dict,
    ) -> dict:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        run_id = context["run_id"]
        loaded_at = pendulum.now("UTC").to_iso8601_string()
        sources = {
            "raw_orders": [order_source, order_source.get("increment")],
            "raw_payments": [payment_source],
            "raw_customers": [customer_source],
            "raw_products": [product_source],
            "raw_stores": [store_source],
        }
        con = duckdb.connect(str(WAREHOUSE_DB))
        counts: dict[str, int] = {}
        try:
            ensure_audit_tables(con)
            con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [context["run_id"], DAG_ID])
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS elt_quarantine (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    source_file VARCHAR,
                    row_key VARCHAR,
                    source_row_number BIGINT,
                    failure_reason VARCHAR,
                    record_json JSON,
                    quarantined_at TIMESTAMPTZ
                )
                """
            )
            for table, table_sources in sources.items():
                source_columns = RAW_COLUMNS[table]
                column_ddl = ", ".join(f'"{column}" VARCHAR' for column in source_columns)
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} ({column_ddl}, "
                    "_loaded_at TIMESTAMPTZ, _source_file VARCHAR, _run_id VARCHAR, _source_row_number BIGINT)"
                )
                inserted = 0
                for source in table_sources:
                    if not source:
                        continue
                    actual_columns = set(source.get("columns", []))
                    expected_columns = set(source_columns)
                    missing_columns = sorted(expected_columns - actual_columns)
                    unexpected_columns = sorted(actual_columns - expected_columns)
                    if missing_columns or unexpected_columns:
                        raise ValueError(
                            f"Schema mismatch for {source['filename']}: "
                            f"missing columns={missing_columns}; unexpected columns={unexpected_columns}"
                        )
                    source_columns_sql = ", ".join(f'"{column}"' for column in source_columns)
                    con.execute(
                        f"""
                        INSERT INTO {table} ({source_columns_sql}, _loaded_at, _source_file, _run_id, _source_row_number)
                        SELECT {source_columns_sql}, ?, ?, ?, (ROW_NUMBER() OVER () + 1)::BIGINT
                        FROM read_csv(?, header=true, all_varchar=true, nullstr='__NMM_EMPTY_FIELD_SENTINEL__')
                        """,
                        [loaded_at, source["filename"], run_id, source["path"]],
                    )
                    inserted += int(source["rows"])
                counts[table] = inserted
                print(f"[Raw] Appended {inserted} source rows to {table}.")
        finally:
            con.close()
        return {"rows_loaded": sum(counts.values()), "by_table": counts, "run_id": run_id}

    @task
    def transform_staging(raw_load: dict) -> dict:
        """Run all staging transformations in SQL, including the order MERGE."""
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            _run_sql_file(con, "elt_dimensions.sql")
            _run_sql_file(con, "elt_payments.sql")
            _run_sql_file(con, "elt_orders_source.sql")
            _run_sql_file(con, "elt_orders_merge.sql")
            order_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
            passed_count = con.execute(
                "SELECT COUNT(*) FROM stg_orders WHERE dq_status IN ('PASS', 'WARNING')"
            ).fetchone()[0]
            print(f"[Staging] {order_count} current order keys; {passed_count} are eligible for marts.")
            return {"order_keys": int(order_count), "passed_orders": int(passed_count)}
        finally:
            con.close()

    @task
    def record_dq_results(staged: dict) -> int:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            ensure_audit_tables(con)
            con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [context["run_id"], DAG_ID])
            con.execute(
                "CREATE OR REPLACE TEMP TABLE _pipeline_context AS "
                "SELECT ?::VARCHAR AS run_id, ?::VARCHAR AS dag_id",
                [context["run_id"], DAG_ID],
            )
            _run_sql_file(con, "elt_dq_results.sql")
            count = con.execute(
                "SELECT COUNT(*) FROM dq_results WHERE run_id = ? AND dag_id = ?",
                [context["run_id"], DAG_ID],
            ).fetchone()[0]
            print(f"[DQ] Wrote {count} SQL quality checks for this run.")
            return int(count)
        finally:
            con.close()

    @task
    def quarantine_failed_rows(dq_count: int) -> int:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        run_id = context["run_id"]
        con = duckdb.connect(str(WAREHOUSE_DB))
        inserted = 0
        try:
            con.execute("DELETE FROM elt_quarantine WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
            con.execute(
                "CREATE OR REPLACE TEMP TABLE _pipeline_context AS "
                "SELECT ?::VARCHAR AS run_id, ?::VARCHAR AS dag_id",
                [run_id, DAG_ID],
            )
            mapping = [
                ("stg_orders", "order_id", "raw_orders", True),
                ("stg_payments", "payment_id", "raw_payments", False),
                ("stg_customers", "customer_id", "raw_customers", False),
                ("stg_products", "product_id", "raw_products", False),
                ("stg_stores", "store_id", "raw_stores", False),
            ]
            for table, key, _, _ in mapping:
                generic_count = con.execute(
                    f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE dq_status = 'FAIL'
                      AND NOT (_source_file LIKE '%orders%' AND failure_reason LIKE '%duplicate order_id%')
                    """
                ).fetchone()[0]
                if generic_count:
                    con.execute(
                        f"""
                        INSERT INTO elt_quarantine
                        SELECT context.run_id, context.dag_id, t._source_file,
                               COALESCE(CAST(t.{key} AS VARCHAR), ''), t._source_row_number,
                               t.failure_reason, to_json(t), CURRENT_TIMESTAMP
                        FROM {table} t CROSS JOIN _pipeline_context context
                        WHERE t.dq_status = 'FAIL'
                          AND NOT (t._source_file LIKE '%orders%' AND t.failure_reason LIKE '%duplicate order_id%')
                        """
                    )
                    inserted += int(generic_count)
            duplicate_json = {
                "raw_orders": "to_json(r)",
                "raw_payments": "to_json(r)",
                "raw_customers": "to_json(json_object('customer_id', r.customer_id, 'masked_email', CASE WHEN POSITION('@' IN r.email) > 1 THEN SUBSTR(r.email, 1, 2) || '***@' || SPLIT_PART(r.email, '@', 2) ELSE '***' END, 'masked_phone', CASE WHEN REGEXP_MATCHES(REGEXP_REPLACE(r.phone, '[^0-9]', '', 'g'), '^[0-9]{10}$') THEN SUBSTR(REGEXP_REPLACE(r.phone, '[^0-9]', '', 'g'), 1, 2) || '******' || RIGHT(REGEXP_REPLACE(r.phone, '[^0-9]', '', 'g'), 2) ELSE '***' END, 'loyalty_tier', r.loyalty_tier))",
                "raw_products": "to_json(r)",
                "raw_stores": "to_json(json_object('store_id', r.store_id, 'store_name', r.store_name, 'zone', r.zone, 'area', r.area, 'pincode', r.pincode, 'open_date', r.open_date, 'capacity_orders_per_day', r.capacity_orders_per_day))",
            }
            for table, key, raw_table, include_first in mapping:
                duplicate_filter = "key_rank >= 1" if include_first else "key_rank > 1"
                duplicate_count = con.execute(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT key_rank, key_count FROM (
                            WITH latest_copy AS (
                                SELECT *, ROW_NUMBER() OVER (
                                    PARTITION BY _source_file, _source_row_number
                                    ORDER BY _loaded_at DESC, _run_id DESC
                                ) AS copy_rank
                                FROM {raw_table}
                            ), stable AS (
                                SELECT * EXCLUDE (copy_rank) FROM latest_copy WHERE copy_rank = 1
                            )
                            SELECT ROW_NUMBER() OVER (
                                       PARTITION BY _source_file, NULLIF(TRIM({key}), '')
                                       ORDER BY _source_row_number
                                   ) AS key_rank,
                                   COUNT(*) OVER (PARTITION BY _source_file, NULLIF(TRIM({key}), '')) AS key_count
                            FROM stable
                        ) ranked
                        WHERE key_count > 1 AND {duplicate_filter}
                    )
                    """
                ).fetchone()[0]
                if not duplicate_count:
                    continue
                con.execute(
                    f"""
                    INSERT INTO elt_quarantine
                    WITH latest_copy AS (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY _source_file, _source_row_number
                            ORDER BY _loaded_at DESC, _run_id DESC
                        ) AS copy_rank
                        FROM {raw_table}
                    ), stable AS (
                        SELECT * EXCLUDE (copy_rank) FROM latest_copy WHERE copy_rank = 1
                    ), ranked AS (
                        SELECT *, ROW_NUMBER() OVER (
                                   PARTITION BY _source_file, NULLIF(TRIM({key}), '')
                                   ORDER BY _source_row_number
                               ) AS key_rank,
                               COUNT(*) OVER (PARTITION BY _source_file, NULLIF(TRIM({key}), '')) AS key_count
                        FROM stable
                    )
                    SELECT context.run_id, context.dag_id, r._source_file,
                           COALESCE(CAST(r.{key} AS VARCHAR), ''), r._source_row_number,
                           'duplicate {key}', {duplicate_json[raw_table]}, CURRENT_TIMESTAMP
                    FROM ranked r CROSS JOIN _pipeline_context context
                    WHERE r.key_count > 1 AND r.{key} IS NOT NULL AND {duplicate_filter}
                    """
                )
                inserted += int(duplicate_count)
            print(f"[Quarantine] Logged {inserted} failed or duplicate source records.")
        finally:
            con.close()
        return inserted

    @task
    def build_marts(quarantine_count: int) -> dict:
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            _run_sql_file(con, "elt_marts.sql")
            table_names = [
                "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
                "mart_payment_mode_revenue", "mart_payment_reconciliation", "mart_top_customers",
            ]
            for table in table_names:
                frame = con.execute(f"SELECT * FROM {table} LIMIT 10").fetchdf()
                print(f"[Mart] {table}: {len(frame)} displayed rows")
                if not frame.empty:
                    print(frame.to_string(index=False))
            return {
                "mart_orders": int(con.execute("SELECT COUNT(*) FROM mart_daily_store_kpis").fetchone()[0]),
                "reconciliation_rows": int(con.execute("SELECT COUNT(*) FROM mart_payment_reconciliation").fetchone()[0]),
            }
        finally:
            con.close()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run() -> None:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        dag_run: DagRun = context["dag_run"]
        ti = context["ti"]
        run_id = dag_run.run_id
        raw_load = ti.xcom_pull(task_ids="load_raw") or {}
        quarantine_count = ti.xcom_pull(task_ids="quarantine_failed_rows") or 0
        source_results = [
            ti.xcom_pull(task_ids=task_id) or {}
            for task_id in ("extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores")
        ]
        extracted = sum(int(result.get("rows", 0)) for result in source_results)
        extracted += sum(int((result.get("increment") or {}).get("rows", 0)) for result in source_results)

        states = [
            task_instance.state
            for task_instance in dag_run.get_task_instances()
            if task_instance.task_id != "audit_run"
        ]
        status = "FAILED" if any(state in {"failed", "upstream_failed"} for state in states) else "SUCCESS"
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            ensure_audit_tables(con)
            con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
            con.execute(
                "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    DAG_ID,
                    dag_run.start_date or pendulum.now("UTC"),
                    pendulum.now("UTC"),
                    extracted,
                    int(raw_load.get("rows_loaded", 0)),
                    int(quarantine_count),
                    status,
                ],
            )
        finally:
            con.close()
        print(f"[Audit] run_id={run_id} status={status} extracted={extracted} raw_loaded={raw_load.get('rows_loaded', 0)} quarantined={quarantine_count}")

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()

    raw = load_raw(orders, payments, customers, products, stores)
    staged = transform_staging(raw)
    dq = record_dq_results(staged)
    quarantined = quarantine_failed_rows(dq)
    marts = build_marts(quarantined)
    audit = audit_run()
    marts >> audit


nammamart_elt_Tusshar_Tikoo()
