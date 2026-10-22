"""Raw-preserving ELT pipeline for NammaMart."""

from pathlib import Path
import re
import sys
import csv
from datetime import timedelta

import duckdb
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.sdk import get_current_context
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/usr/local/airflow/include")

from pipeline_audit import record_pipeline_run


INCLUDE_DIR = Path("/usr/local/airflow/include")
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
SQL_DIR = INCLUDE_DIR / "sql"
WAREHOUSE_PATH = INCLUDE_DIR / "warehouse_sol_elt.duckdb"
ETL_WAREHOUSE_PATH = INCLUDE_DIR / "warehouse_sol.duckdb"

SOURCES = {
    "orders": "raw_orders",
    "payments": "raw_payments",
    "customers": "raw_customers",
    "products": "raw_products",
    "stores": "raw_stores",
}

RAW_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"],
}

DEFAULT_ARGS = {
    "owner": "fde-sol",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
}


def _run_sql_file(connection, filename: str, replacements: dict[str, str] | None = None) -> None:
    sql_path = SQL_DIR / filename
    if not sql_path.is_file():
        raise FileNotFoundError(f"Required ELT SQL file not found: {sql_path}")

    # Semicolons also appear inside the failure_reason delimiter and prose
    # literals, so split only on semicolons outside quoted SQL strings.
    sql = sql_path.read_text(encoding="utf-8")
    for marker, value in (replacements or {}).items():
        sql = sql.replace(marker, value)
    statements = []
    current = []
    quote = None
    index = 0
    while index < len(sql):
        char = sql[index]
        if quote:
            current.append(char)
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    current.append(sql[index + 1])
                    index += 1
                else:
                    quote = None
        elif char in ("'", '"'):
            quote = char
            current.append(char)
        elif char == ";":
            if "".join(current).strip():
                statements.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    if "".join(current).strip():
        statements.append("".join(current))
    for statement in statements:
        if statement.strip():
            connection.execute(statement)


def _quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dag(
    dag_id="nammamart_elt_sol",
    description="Append raw NammaMart exports, transform in DuckDB SQL, and build reporting marts",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "elt", "assignment"],
)
def nammamart_elt_sol():
    @task
    def load_raw_layer():
        context = get_current_context()
        run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", context["run_id"]).strip("_.-") or "manual_run"
        connection = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            _run_sql_file(connection, "raw_tables.sql")
            connection.execute("BEGIN TRANSACTION")
            sources = [
                ("orders", "orders.csv"),
                ("orders", "orders_2026-10-01_increment.csv"),
                ("payments", "payments.csv"),
                ("customers", "customers.csv"),
                ("products", "products.csv"),
                ("stores", "stores.csv"),
            ]
            loaded = {}
            for source, filename in sources:
                path = DATA_DIR / filename
                if not path.is_file():
                    raise FileNotFoundError(f"Required raw source is missing: {path}")
                columns = RAW_COLUMNS[source]
                with path.open("r", newline="", encoding="utf-8-sig") as source_stream:
                    actual_columns = next(csv.reader(source_stream), [])
                if actual_columns != columns:
                    raise ValueError(
                        f"Schema mismatch for {filename}. Expected {columns}; got {actual_columns}"
                    )
                columns_sql = "{" + ", ".join(
                    f"{_quoted(column)}: {_quoted('VARCHAR')}" for column in columns
                ) + "}"
                selected_columns = ", ".join(f'CAST("{column}" AS VARCHAR)' for column in columns)
                table = SOURCES[source]
                connection.execute(
                    f"""
                    INSERT INTO {table} ({', '.join(columns)}, _loaded_at, _source_file, _run_id)
                    SELECT {selected_columns},
                           CURRENT_TIMESTAMP + ROW_NUMBER() OVER () * INTERVAL '1 microsecond',
                           {_quoted(filename)}, {_quoted(run_id)}
                    FROM read_csv({_quoted(str(path))}, header=true, all_varchar=true,
                                  nullstr='__NMM_NULL_SENTINEL__', columns={columns_sql})
                    """
                )
                count = connection.execute(
                    "SELECT COUNT(*) FROM read_csv(?, header=true, all_varchar=true, nullstr='__NMM_NULL_SENTINEL__')",
                    [str(path)],
                ).fetchone()[0]
                loaded[filename] = count
                print(f"[Raw] Appended {count} untouched text rows from {filename} to {table}")
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        return {"run_id": run_id, "rows_by_file": loaded, "warehouse": str(WAREHOUSE_PATH)}

    @task
    def build_staging_layer(raw_result):
        connection = duckdb.connect(str(WAREHOUSE_PATH))
        stats = {}
        try:
            # Dimensions precede orders so SQL can enforce key integrity. Payments
            # follow orders so SUCCESS amount and mode checks can use clean orders.
            for table, filename in [
                ("stg_customers", "stg_customers.sql"),
                ("stg_products", "stg_products.sql"),
                ("stg_stores", "stg_stores.sql"),
                ("stg_orders", "stg_orders.sql"),
                ("stg_payments", "stg_payments.sql"),
            ]:
                _run_sql_file(connection, filename)
                count, failed = connection.execute(
                    f"SELECT COUNT(*), COUNT(*) FILTER (WHERE dq_status = 'FAIL') FROM {table}"
                ).fetchone()
                stats[table] = {"rows": count, "failed": failed}
                print(f"[Staging] {table}: {count} rows, {failed} failed")
            _run_sql_file(
                connection,
                "dq_results.sql",
                {
                    "{{RUN_ID}}": _quoted(raw_result["run_id"]),
                    "{{DAG_ID}}": _quoted("nammamart_elt_sol"),
                },
            )
            dq_count = connection.execute(
                "SELECT COUNT(*) FROM dq_results WHERE run_id = ? AND dag_id = ?",
                [raw_result["run_id"], "nammamart_elt_sol"],
            ).fetchone()[0]
            print(f"[DQ] Recorded {dq_count} ELT checks for run {raw_result['run_id']}")
        finally:
            connection.close()
        return {"run_id": raw_result["run_id"], "tables": stats}

    @task
    def build_mart_layer(staging_result):
        connection = duckdb.connect(str(WAREHOUSE_PATH))
        stats = {}
        try:
            for table, filename in [
                ("mart_daily_store_kpis", "mart_daily_store_kpis.sql"),
                ("mart_category_margin", "mart_category_margin.sql"),
                ("mart_delivery_sla", "mart_delivery_sla.sql"),
                ("mart_payment_reconciliation", "mart_payment_reconciliation.sql"),
                ("mart_payment_mode_revenue", "mart_payment_mode_revenue.sql"),
                ("mart_top_customers", "mart_top_customers.sql"),
                ("mart_dq_rejections", "mart_dq_rejections.sql"),
            ]:
                _run_sql_file(connection, filename)
                count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                stats[table] = count
                print(f"[Mart] {table}: {count} rows")
            for table in [
                "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
                "mart_payment_mode_revenue", "mart_top_customers", "mart_dq_rejections",
            ]:
                print(f"[Mart result] {table}")
                print(connection.execute(f"SELECT * FROM {table}").fetchdf().to_string(index=False))
        finally:
            connection.close()
        return {"run_id": staging_result["run_id"], "tables": stats}

    @task
    def compare_september_totals(raw_result, mart_result):
        connection = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            if not ETL_WAREHOUSE_PATH.is_file():
                connection.execute("""
                    CREATE OR REPLACE TABLE mart_etl_comparison AS
                    SELECT 'ETL warehouse not available; run the ETL DAG first' AS comparison_note,
                           NULL::BIGINT AS etl_delivered_orders,
                           NULL::BIGINT AS elt_delivered_orders,
                           NULL::DOUBLE AS etl_september_revenue,
                           NULL::DOUBLE AS elt_september_revenue,
                           NULL::DOUBLE AS revenue_delta,
                           NULL::DOUBLE AS etl_september_gross_margin,
                           NULL::DOUBLE AS elt_september_gross_margin,
                           NULL::DOUBLE AS gross_margin_delta,
                           NULL::BIGINT AS late_return_updates,
                           NULL::DOUBLE AS revenue_restated_by_late_returns,
                           NULL::DOUBLE AS revenue_delta_not_explained_by_late_returns,
                           NULL::DOUBLE AS gross_margin_restated_by_late_returns,
                           NULL::DOUBLE AS gross_margin_delta_not_explained_by_late_returns
                """)
                print(f"[Compare] ETL warehouse not found at {ETL_WAREHOUSE_PATH}; comparison deferred")
                return {"run_id": raw_result["run_id"], "status": "etl_warehouse_missing"}
            connection.execute(f"ATTACH {_quoted(str(ETL_WAREHOUSE_PATH))} AS etl_db (READ_ONLY)")
            _run_sql_file(connection, "compare_september.sql")
            result = connection.execute("SELECT * FROM mart_etl_comparison").fetchdf()
        finally:
            connection.close()
        print("[Compare] ELT versus ETL September totals")
        print(result.to_string(index=False))
        return {"run_id": raw_result["run_id"], "status": "compared", "mart_tables": mart_result["tables"]}

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def audit_run():
        context = get_current_context()
        dag_run = context["dag_run"]
        ti = context["ti"]
        raw_result = ti.xcom_pull(task_ids="load_raw_layer")
        staging_result = ti.xcom_pull(task_ids="build_staging_layer")
        comparison_result = ti.xcom_pull(task_ids="compare_september_totals")
        rows_extracted = sum(
            int(value) for value in (raw_result or {}).get("rows_by_file", {}).values()
        )
        rows_loaded = rows_extracted
        rows_quarantined = 0
        if isinstance(staging_result, dict):
            connection = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
            try:
                rows_quarantined = sum(
                    int(connection.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE dq_status = 'FAIL' AND _run_id = ?",
                        [raw_result["run_id"]],
                    ).fetchone()[0])
                    for table in [
                        "stg_orders", "stg_payments", "stg_customers",
                        "stg_products", "stg_stores",
                    ]
                )
            finally:
                connection.close()
        # The comparison is the terminal business task and depends on all
        # preceding ELT layers, so its XCom indicates those layers completed.
        status = "success" if isinstance(comparison_result, dict) else "failed"
        audit = record_pipeline_run(
            warehouse_path=WAREHOUSE_PATH,
            dag_id=dag_run.dag_id,
            run_id=dag_run.run_id,
            start_time=dag_run.start_date,
            end_time=pendulum.now("UTC"),
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
            rows_quarantined=rows_quarantined,
            status=status,
        )
        if status != "success":
            raise AirflowException(
                "ELT run failed before compare_september_totals completed; see upstream task logs and pipeline_runs."
            )
        return audit

    raw = load_raw_layer()
    staging = build_staging_layer(raw)
    marts = build_mart_layer(staging)
    comparison = compare_september_totals(raw, marts)

    audit = audit_run()
    [raw, staging, marts, comparison] >> audit


nammamart_elt_sol()
