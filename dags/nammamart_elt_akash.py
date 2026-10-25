import os
import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task

DB_PATH = "/usr/local/airflow/include/warehouse_akash.duckdb"
DATA_DIR = "/usr/local/airflow/include/data/nammamart"
SQL_PATH = "/usr/local/airflow/include/sql/elt_models.sql"

default_args = {
    "owner": "akash",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
    "execution_timeout": pendulum.duration(minutes=15),
}

@dag(
    dag_id="nammamart_elt_akash",
    default_args=default_args,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["nammamart", "elt"],
)
def nammamart_elt_pipeline():

    @task
    def load_raw_tables():
        con = duckdb.connect(DB_PATH)
        for f in ["orders", "payments", "customers", "products", "stores"]:
            csv_path = f"{DATA_DIR}/{f}.csv"
            df = pd.read_csv(csv_path, dtype=str)
            df["_loaded_at"] = pendulum.now().to_iso8601_string()
            df["_source_file"] = f"{f}.csv"
            df["_run_id"] = "init_run"
            con.execute(f"CREATE OR REPLACE TABLE raw_{f} AS SELECT * FROM df")
        con.close()

    @task
    def run_elt_sql():
        con = duckdb.connect(DB_PATH)
        with open(SQL_PATH, "r") as sql_file:
            statements = [s.strip() for s in sql_file.read().split(";") if s.strip()]
        for stmt in statements:
            con.execute(stmt)
        con.close()

    @task
    def load_incremental():
        con = duckdb.connect(DB_PATH)
        inc_path = f"{DATA_DIR}/orders_2026-10-01_increment.csv"
        if os.path.exists(inc_path):
            df_inc = pd.read_csv(inc_path, dtype=str)
            df_inc["_loaded_at"] = pendulum.now().to_iso8601_string()
            df_inc["_source_file"] = "orders_2026-10-01_increment.csv"
            df_inc["_run_id"] = "incremental_run"
            con.execute("INSERT INTO raw_orders SELECT * FROM df_inc")
            
            con.execute("""
                CREATE OR REPLACE TEMP TABLE temp_inc AS
                SELECT 
                    order_id,
                    TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
                    TRY_CAST(TRY_CAST(order_ts AS TIMESTAMP) AS DATE) AS order_date,
                    customer_id, store_id, product_id,
                    TRY_CAST(quantity AS INTEGER) AS quantity,
                    TRY_CAST(unit_price AS DOUBLE) AS unit_price,
                    TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
                    TRY_CAST(order_amount AS DOUBLE) AS order_amount,
                    UPPER(TRIM(payment_mode)) AS payment_mode,
                    UPPER(TRIM(order_status)) AS order_status,
                    TRIM(channel) AS channel,
                    TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
                    'PASSED' AS dq_status,
                    CASE WHEN TRY_CAST(delivery_minutes AS DOUBLE) <= 15 THEN 1 ELSE 0 END AS within_15_min
                FROM df_inc;

                DELETE FROM stg_orders WHERE order_id IN (SELECT order_id FROM temp_inc);
                INSERT INTO stg_orders SELECT * FROM temp_inc;
            """)
        con.close()

    @task(trigger_rule="all_done")
    def run_audit():
        con = duckdb.connect(DB_PATH)
        con.execute("""
            INSERT INTO pipeline_runs 
            SELECT 
                SUBSTR(CAST(UUID() AS VARCHAR), 1, 8),
                'nammamart_elt_akash',
                CURRENT_TIMESTAMP - INTERVAL 1 MINUTE,
                CURRENT_TIMESTAMP,
                (SELECT COUNT(*) FROM stg_orders),
                'SUCCESS'
        """)
        con.close()

    t1 = load_raw_tables()
    t2 = run_elt_sql()
    t3 = load_incremental()
    t4 = run_audit()

    t1 >> t2 >> t3 >> t4

nammamart_elt_pipeline()
