from datetime import datetime, timedelta
import os
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
IST = pendulum.timezone("Asia/Kolkata")
PAYMENT_MODES = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "wallet": "Wallet",
}


def dataframe_records_for_xcom(df):
    import pandas as pd

    result = df.copy()
    for column in result.select_dtypes(
        include=["datetime", "datetimetz"]
    ).columns:
        result[column] = result[column].astype(str)
    return (
        result.astype(object)
        .where(pd.notna(result), None)
        .to_dict(orient="records")
    )


# Default arguments for the DAG
default_args = {
    'owner': 'fde_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

@dag(
    dag_id='nammamart_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL pipeline for NammaMart retail analytics',
    schedule="0 9 * * 1-6",  # Will be adjusted for 9 AM IST in Part C
    start_date=pendulum.datetime(2026, 9, 1, tz=IST),
    catchup=False,
    tags=['nammamart', 'etl', 'fde'],
)
def nammamart_etl_pipeline():

    
    PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(PROJECT_DIR, "include", "data", "nammamart")
    QUARANTINE_DIR = os.path.join(PROJECT_DIR, "include", "quarantine", "surender_singh")
    DB_PATH = os.path.join(PROJECT_DIR, "include", "warehouse_surender_singh.duckdb")

    def read_source_records(filename):
        import pandas as pd

        path = os.path.join(DATA_DIR, filename)
        df = pd.read_csv(path)
        df["_source_file"] = filename
        return dataframe_records_for_xcom(df)

    @task
    def extract_customers():
        return read_source_records("customers.csv")

    @task
    def extract_products():
        return read_source_records("products.csv")

    @task
    def extract_stores():
        return read_source_records("stores.csv")

    @task
    def extract_orders():
        return (
            read_source_records("orders.csv")
            + read_source_records("orders_2026-10-01_increment.csv")
        )

    @task
    def extract_payments():
        return read_source_records("payments.csv")

    @task
    def validate(
        customers_raw,
        products_raw,
        stores_raw,
        orders_raw,
        payments_raw,
    ):
        import pandas as pd

        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        df_cust = pd.DataFrame(customers_raw)
        df_prod = pd.DataFrame(products_raw)
        df_store = pd.DataFrame(stores_raw)
        df_ord = pd.DataFrame(orders_raw)

        customer_ids = set(
            df_cust['customer_id'].dropna().astype(str).str.strip()
        )
        product_ids = set(
            df_prod['product_id'].dropna().astype(str).str.strip()
        )
        store_ids = set(
            df_store['store_id'].dropna().astype(str).str.strip()
        )
        allowed_statuses = {'DELIVERED', 'CANCELLED', 'RETURNED'}
        now_ist = pd.Timestamp.now(tz='Asia/Kolkata').tz_localize(None)
        valid_orders = []
        quarantine_orders = []

        for _, row in df_ord.iterrows():
            reasons = []
            if pd.isna(row.get('order_id')):
                reasons.append("Missing order_id")
            if pd.isna(row.get('customer_id')):
                reasons.append("Missing customer_id")
            if pd.isna(row.get('store_id')):
                reasons.append("Missing store_id")
            if pd.isna(row.get('product_id')):
                reasons.append("Missing product_id")

            order_ts = pd.to_datetime(
                row.get('order_ts'),
                errors='coerce',
            )
            if pd.isna(order_ts):
                reasons.append("Missing or invalid order_ts")
            elif order_ts.tzinfo is not None:
                order_ts = order_ts.tz_convert('Asia/Kolkata').tz_localize(None)
                if order_ts > now_ist:
                    reasons.append("Future order_ts")
            elif order_ts > now_ist:
                reasons.append("Future order_ts")

            quantity = pd.to_numeric(row.get('quantity'), errors='coerce')
            if pd.isna(quantity) or quantity <= 0:
                reasons.append("Non-positive quantity")
            unit_price = pd.to_numeric(
                row.get('unit_price'),
                errors='coerce',
            )
            discount_pct = pd.to_numeric(
                row.get('discount_pct'),
                errors='coerce',
            )
            if pd.isna(unit_price) or unit_price < 0:
                reasons.append("Invalid unit_price")
            if pd.isna(discount_pct) or not 0 <= discount_pct <= 100:
                reasons.append("Invalid discount_pct")

            order_status = str(row.get('order_status')).strip().upper()
            if order_status not in allowed_statuses:
                reasons.append("Invalid order_status")

            payment_mode = str(row.get('payment_mode')).strip().lower()
            if payment_mode not in PAYMENT_MODES:
                reasons.append("Invalid payment_mode")

            customer_id = str(row.get('customer_id')).strip()
            if (
                not pd.isna(row.get('customer_id'))
                and customer_id not in customer_ids
            ):
                reasons.append("Unknown customer_id")
            product_id = str(row.get('product_id')).strip()
            if (
                not pd.isna(row.get('product_id'))
                and product_id not in product_ids
            ):
                reasons.append("Unknown product_id")
            store_id = str(row.get('store_id')).strip()
            if (
                not pd.isna(row.get('store_id'))
                and store_id not in store_ids
            ):
                reasons.append("Unknown store_id")

            source_amount = pd.to_numeric(
                row.get('order_amount'),
                errors='coerce',
            )
            if (
                not pd.isna(quantity)
                and not pd.isna(unit_price)
                and not pd.isna(discount_pct)
                and not pd.isna(source_amount)
            ):
                expected_amount = round(
                    quantity * unit_price * (1 - discount_pct / 100),
                    2,
                )
                if abs(source_amount - expected_amount) > 0.02:
                    reasons.append("order_amount mismatch")
            elif pd.isna(source_amount):
                reasons.append("Invalid order_amount")

            if order_status == 'DELIVERED':
                delivery_minutes = pd.to_numeric(
                    row.get('delivery_minutes'),
                    errors='coerce',
                )
                if pd.isna(delivery_minutes):
                    reasons.append(
                        "Delivered order missing or invalid delivery_minutes"
                    )
                elif not 8 <= delivery_minutes <= 35:
                    reasons.append(
                        "Delivered order delivery_minutes outside 8-35"
                    )
            
            if reasons:
                row_dict = row.to_dict()
                row_dict['failure_reason'] = ", ".join(reasons)
                row_dict['source_file'] = row.get(
                    "_source_file",
                    "orders.csv",
                )
                quarantine_orders.append(row_dict)
            else:
                valid_orders.append(row)

        df_valid_ord = pd.DataFrame(valid_orders)
        q_df = pd.DataFrame(
            quarantine_orders,
            columns=[*df_ord.columns, "failure_reason", "source_file"],
        )
        if not q_df.empty:
            q_path = os.path.join(QUARANTINE_DIR, f"orders_failed_{run_id}.csv")
            q_df.to_csv(q_path, index=False)

        return {
            "customers": customers_raw,
            "products": products_raw,
            "stores": stores_raw,
            "payments": payments_raw,
            "valid_orders": dataframe_records_for_xcom(df_valid_ord),
            "quarantined_orders": dataframe_records_for_xcom(q_df),
        }

    @task
    def transform(validated_data):
        import pandas as pd

        df_cust = pd.DataFrame(validated_data["customers"])
        df_prod = pd.DataFrame(validated_data["products"])
        df_store = pd.DataFrame(validated_data["stores"])
        df_pay = pd.DataFrame(validated_data["payments"])
        df_valid_ord = pd.DataFrame(validated_data["valid_orders"])
        q_df = pd.DataFrame(validated_data["quarantined_orders"])

        df_cust = df_cust.drop_duplicates(subset=['customer_id'])
        df_prod = df_prod.drop_duplicates(subset=['product_id'])
        df_valid_ord = df_valid_ord.drop_duplicates(
            subset=['order_id'],
            keep='last',
        )
        df_pay = df_pay.drop_duplicates(subset=['payment_id'])

        def mask_email(email):
            if pd.isna(email) or '@' not in str(email):
                return email
            parts = email.split('@')
            name = parts[0]
            masked_name = name[:2] + '***' if len(name) > 2 else '***'
            return f"{masked_name}@{parts[1]}"

        def mask_phone(phone):
            p_str = str(phone)
            if len(p_str) >= 6:
                return p_str[:2] + '******' + p_str[-2:]
            return '******'

        if 'email' in df_cust.columns:
            df_cust['email'] = df_cust['email'].apply(mask_email)
        if 'phone' in df_cust.columns:
            df_cust['phone'] = df_cust['phone'].apply(mask_phone)

        # Standardise payment modes & casing
        if 'payment_mode' in df_valid_ord.columns:
            normalized_payment_modes = (
                df_valid_ord['payment_mode']
                .astype(str)
                .str.strip()
                .str.lower()
                .map(PAYMENT_MODES)
            )
            if normalized_payment_modes.isna().any():
                invalid_modes = (
                    df_valid_ord.loc[
                        normalized_payment_modes.isna(), 'payment_mode'
                    ]
                    .drop_duplicates()
                    .tolist()
                )
                raise ValueError(
                    f"Cannot standardize validated payment modes: "
                    f"{invalid_modes}"
                )
            df_valid_ord['payment_mode'] = normalized_payment_modes

        if 'order_status' in df_valid_ord.columns:
            df_valid_ord['order_status'] = df_valid_ord['order_status'].astype(str).str.strip().str.upper()

        df_valid_ord['order_ts'] = pd.to_datetime(df_valid_ord['order_ts'])
        df_valid_ord['order_date'] = (
            df_valid_ord['order_ts'].dt.strftime("%Y-%m-%d")
        )
        df_valid_ord['order_ts'] = (
            df_valid_ord['order_ts'].dt.strftime("%Y-%m-%d %H:%M:%S")
        )
        df_valid_ord['quantity'] = pd.to_numeric(
            df_valid_ord['quantity'],
            errors="coerce",
        )
        df_valid_ord['unit_price'] = pd.to_numeric(
            df_valid_ord['unit_price'],
            errors="coerce",
        )
        df_valid_ord['discount_pct'] = pd.to_numeric(
            df_valid_ord['discount_pct'],
            errors="coerce",
        )
        df_prod['cost_price'] = pd.to_numeric(
            df_prod['cost_price'],
            errors="coerce",
        )

        df_valid_ord['calculated_amount'] = (
            df_valid_ord['quantity'] * df_valid_ord['unit_price'] * (1 - df_valid_ord['discount_pct'] / 100.0)
        ).round(2)

        df_merged = df_valid_ord.merge(df_prod[['product_id', 'cost_price']], on='product_id', how='left')
        df_merged['gross_margin'] = (
            df_merged['calculated_amount'] - (df_merged['quantity'] * df_merged['cost_price'])
        ).round(2)

        df_merged['within_15_min'] = df_merged['delivery_minutes'].apply(lambda x: 1 if pd.notna(x) and x <= 15 else 0)

        fact_cols = [
            'order_id', 'order_ts', 'order_date', 'customer_id', 'store_id',
            'product_id', 'quantity', 'unit_price', 'discount_pct',
            'calculated_amount', 'payment_mode', 'order_status', 'channel',
            'delivery_minutes', 'gross_margin', 'within_15_min', '_source_file',
        ]
        df_fact = df_merged[
            [column for column in fact_cols if column in df_merged.columns]
        ]

        return {
            "customers": dataframe_records_for_xcom(df_cust),
            "products": dataframe_records_for_xcom(df_prod),
            "stores": dataframe_records_for_xcom(df_store),
            "payments": dataframe_records_for_xcom(df_pay),
            "fact_orders": dataframe_records_for_xcom(df_fact),
            "quarantined_orders": dataframe_records_for_xcom(q_df),
        }

    @task
    def load(transformed_data):
        import duckdb
        import pandas as pd

        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        tables = {
            "etl_dim_customer": transformed_data["customers"],
            "etl_dim_product": transformed_data["products"],
            "etl_dim_store": transformed_data["stores"],
            "etl_payments": transformed_data["payments"],
            "etl_fact_orders": transformed_data["fact_orders"],
            "etl_quarantine_orders": transformed_data["quarantined_orders"],
        }
        with duckdb.connect(DB_PATH) as conn:
            for table_name, records in tables.items():
                dataframe = pd.DataFrame(records)
                conn.register("incoming_table", dataframe)
                conn.execute(
                    f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM incoming_table
                    """
                )
                conn.unregister("incoming_table")

        return "Loaded clean ETL tables into DuckDB."

    @task
    def reconcile_and_report():
        import duckdb

        with duckdb.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            WITH delivered_orders AS (
                SELECT order_id, calculated_amount 
                FROM etl_fact_orders 
                WHERE order_status = 'DELIVERED'
            ),
            agg_payments AS (
                SELECT
                    order_id,
                    SUM(TRY_CAST(amount_paid AS DOUBLE)) AS total_paid
                FROM etl_payments
                WHERE payment_status = 'SUCCESS'
                GROUP BY order_id
            )
            SELECT
                o.order_id,
                o.calculated_amount AS expected_amount,
                COALESCE(p.total_paid, 0) AS actual_paid,
                ABS(o.calculated_amount - COALESCE(p.total_paid, 0)) AS variance,
                CASE 
                    WHEN p.order_id IS NULL THEN 'Missing Payment Record'
                    WHEN ABS(o.calculated_amount - p.total_paid) > 0.01 THEN 'Amount Mismatch'
                    ELSE 'Reconciled'
                END AS reconciliation_status
            FROM delivered_orders o
            LEFT JOIN agg_payments p ON o.order_id = p.order_id
            WHERE p.order_id IS NULL OR ABS(o.calculated_amount - p.total_paid) > 0.01
            """)
            conn.execute("""
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT
                    f.order_date,
                    s.zone,
                    s.store_name,
                    SUM(f.calculated_amount) AS net_revenue
                FROM etl_fact_orders f
                JOIN etl_dim_store s ON f.store_id = s.store_id
                WHERE f.order_status = 'DELIVERED'
                GROUP BY f.order_date, s.zone, s.store_name
            """)
            conn.execute("""
                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT
                    p.category,
                    SUM(f.calculated_amount) AS total_revenue,
                    SUM(f.gross_margin) AS gross_margin
                FROM etl_fact_orders f
                JOIN etl_dim_product p ON f.product_id = p.product_id
                WHERE f.order_status = 'DELIVERED'
                GROUP BY p.category
            """)
            print("--- DAILY ZONE REVENUE SUMMARY ---")
            print(
                conn.execute(
                    "SELECT * FROM etl_daily_zone_revenue "
                    "ORDER BY net_revenue DESC"
                ).fetchdf().head(10)
            )
            print("--- CATEGORY MARGIN SUMMARY ---")
            print(
                conn.execute(
                    "SELECT * FROM etl_category_margin "
                    "ORDER BY total_revenue DESC"
                ).fetchdf().head(10)
            )

        return "Reconciliation and ETL reports completed."

    @task(task_id="audit_run", trigger_rule=TriggerRule.ALL_DONE)
    def audit_run():
        import csv
        import duckdb

        context = get_current_context()
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        dag_run = context["dag_run"]
        upstream_task_ids = [
            "extract_customers",
            "extract_products",
            "extract_stores",
            "extract_orders",
            "extract_payments",
            "validate",
            "transform",
            "load",
            "reconcile_and_report",
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

        source_files = [
            "customers.csv",
            "products.csv",
            "stores.csv",
            "orders.csv",
            "orders_2026-10-01_increment.csv",
            "payments.csv",
        ]
        rows_extracted = 0
        for filename in source_files:
            path = os.path.join(DATA_DIR, filename)
            with open(path, newline="", encoding="utf-8-sig") as source_file:
                rows_extracted += sum(1 for _ in csv.DictReader(source_file))

        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with duckdb.connect(DB_PATH) as conn:
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

            def count_rows(table_name):
                if table_name not in table_names:
                    return 0
                return conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]

            rows_loaded = sum(
                count_rows(table_name)
                for table_name in (
                    "etl_dim_customer",
                    "etl_dim_product",
                    "etl_dim_store",
                    "etl_fact_orders",
                )
            )
            rows_quarantined = count_rows("etl_quarantine_orders")
            rows_checked = rows_extracted

            conn.execute(
                "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
                [run_id, dag_id],
            )
            conn.execute(
                "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?",
                [run_id, dag_id],
            )

            checks = [
                (
                    "required_order_fields",
                    "Completeness",
                    "Critical",
                    "etl_quarantine_orders",
                    "failure_reason ILIKE '%Missing order_id%' "
                    "OR failure_reason ILIKE '%Missing customer_id%' "
                    "OR failure_reason ILIKE '%Missing store_id%' "
                    "OR failure_reason ILIKE '%Missing product_id%'",
                ),
                (
                    "unique_order_id",
                    "Uniqueness",
                    "Critical",
                    "etl_fact_orders",
                    "order_id IN (SELECT order_id FROM etl_fact_orders "
                    "GROUP BY order_id HAVING COUNT(*) > 1)",
                ),
                (
                    "valid_order_values",
                    "Validity",
                    "Critical",
                    "etl_quarantine_orders",
                    "failure_reason ILIKE '%Invalid order_status%' "
                    "OR failure_reason ILIKE '%Invalid payment_mode%'",
                ),
                (
                    "order_amount_and_delivery_accuracy",
                    "Accuracy",
                    "Critical",
                    "etl_quarantine_orders",
                    "failure_reason ILIKE '%order_amount mismatch%' "
                    "OR failure_reason ILIKE '%delivery_minutes%' "
                    "OR failure_reason ILIKE '%Non-positive quantity%' "
                    "OR failure_reason ILIKE '%Invalid unit_price%' "
                    "OR failure_reason ILIKE '%Invalid discount_pct%'",
                ),
                (
                    "order_reference_integrity",
                    "Consistency",
                    "Critical",
                    "etl_quarantine_orders",
                    "failure_reason ILIKE '%Unknown customer_id%' "
                    "OR failure_reason ILIKE '%Unknown product_id%' "
                    "OR failure_reason ILIKE '%Unknown store_id%'",
                ),
                (
                    "valid_order_timestamp",
                    "Timeliness",
                    "Warning",
                    "etl_quarantine_orders",
                    "failure_reason ILIKE '%order_ts%'",
                ),
            ]
            checked_at = pendulum.now(IST).naive()
            for check_name, dimension, severity, table_name, condition in checks:
                if table_name not in table_names:
                    failed_rows = 0
                    checked_rows = 0
                    status = "SKIPPED"
                else:
                    checked_rows = (
                        rows_extracted
                        if table_name == "etl_quarantine_orders"
                        else count_rows(table_name)
                    )
                    failed_rows = conn.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE {condition}"
                    ).fetchone()[0]
                    status = "PASS" if failed_rows == 0 else "FAIL"
                conn.execute(
                    "INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        run_id,
                        dag_id,
                        check_name,
                        dimension,
                        severity,
                        checked_rows,
                        failed_rows,
                        status,
                        checked_at,
                    ],
                )

            conn.execute(
                "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    dag_id,
                    dag_run.start_date or pendulum.now(IST).naive(),
                    pendulum.now(IST).naive(),
                    rows_extracted,
                    rows_loaded,
                    rows_quarantined,
                    run_status,
                ],
            )

    # Task Dependencies
    c = extract_customers()
    p = extract_products()
    s = extract_stores()
    o = extract_orders()
    pay = extract_payments()

    validated = validate(c, p, s, o, pay)
    transformed = transform(validated)
    loaded = load(transformed)
    reported = reconcile_and_report()
    audit_run_task = audit_run()
    loaded >> reported >> audit_run_task

dag_instance = nammamart_etl_pipeline() 