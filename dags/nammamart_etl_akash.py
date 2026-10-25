import os
import duckdb
import pandas as pd
import pendulum
from datetime import datetime
from airflow.decorators import dag, task

DB_PATH = "/usr/local/airflow/include/warehouse_akash.duckdb"
DATA_DIR = "/usr/local/airflow/include/data/nammamart"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/akash"

default_args = {
    "owner": "akash",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
    "execution_timeout": pendulum.duration(minutes=15),
}

@dag(
    dag_id="nammamart_etl_akash",
    default_args=default_args,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["nammamart", "etl"],
)
def nammamart_etl_pipeline():

    @task
    def extract_and_validate():
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        con = duckdb.connect(DB_PATH)
        
        # 1. Read files
        df_orders = pd.read_csv(f"{DATA_DIR}/orders.csv")
        df_payments = pd.read_csv(f"{DATA_DIR}/payments.csv")
        df_customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
        df_products = pd.read_csv(f"{DATA_DIR}/products.csv")
        df_stores = pd.read_csv(f"{DATA_DIR}/stores.csv")

        # 2. Validation & Quarantining on Orders
        reasons = []
        for _, r in df_orders.iterrows():
            fail = []
            if pd.isna(r["customer_id"]) or r["customer_id"] not in df_customers["customer_id"].values:
                fail.append("INVALID_OR_MISSING_CUSTOMER")
            if pd.isna(r["store_id"]) or r["store_id"] not in df_stores["store_id"].values:
                fail.append("INVALID_OR_MISSING_STORE")
            if pd.isna(r["product_id"]) or r["product_id"] not in df_products["product_id"].values:
                fail.append("INVALID_OR_MISSING_PRODUCT")
            if str(r["order_status"]).upper().strip() not in ["DELIVERED", "CANCELLED", "RETURNED"]:
                fail.append("INVALID_ORDER_STATUS")
            if r["quantity"] <= 0:
                fail.append("NEGATIVE_OR_ZERO_QUANTITY")
            reasons.append(";".join(fail) if fail else "PASSED")

        df_orders["failure_reason"] = reasons
        passed_orders = df_orders[df_orders["failure_reason"] == "PASSED"].copy()
        failed_orders = df_orders[df_orders["failure_reason"] != "PASSED"].copy()

        # Save Quarantine
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        failed_orders.to_csv(f"{QUARANTINE_DIR}/orders_failed_{run_ts}.csv", index=False)

        # 3. Clean Dimensions & Mask PII
        df_customers["email"] = df_customers["email"].astype(str).apply(
            lambda x: (x[:2] + "***@" + x.split("@")[1]) if "@" in x else "masked_email"
        )
        df_customers["phone"] = df_customers["phone"].astype(str).apply(
            lambda x: (x[:2] + "******" + x[-2:]) if len(x) >= 10 else "**********"
        )
        df_customers = df_customers.drop_duplicates(subset=["customer_id"])
        df_products = df_products.drop_duplicates(subset=["product_id"])

        # 4. Idempotent Load to DuckDB Star Schema
        con.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM df_customers")
        con.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM df_products")
        con.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM df_stores")
        con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM passed_orders")

        # 5. Reconciliation Table
        recon_df = pd.merge(passed_orders[passed_orders["order_status"] == "DELIVERED"], 
                            df_payments, on="order_id", how="left")
        recon_df["reconciliation_status"] = recon_df.apply(
            lambda r: "MISSING_PAYMENT_RECORD" if pd.isna(r["payment_id"])
            else ("AMOUNT_MISMATCH" if round(float(r["order_amount"]), 2) != round(float(r["amount_paid"]), 2)
            else "RECONCILED"), axis=1
        )
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon_df")

        # 6. Quality scorecard entry
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR,
                dimension VARCHAR, severity VARCHAR, rows_checked INT,
                rows_failed INT, status VARCHAR, checked_at TIMESTAMP
            )
        """)
        con.execute(f"""
            INSERT INTO dq_results VALUES 
            ('{run_ts}', 'nammamart_etl_akash', 'order_integrity_check', 'Completeness & Validity', 
             'Critical', {len(df_orders)}, {len(failed_orders)}, 'COMPLETED', CURRENT_TIMESTAMP)
        """)
        con.close()
        return len(passed_orders)

    @task(trigger_rule="all_done")
    def run_audit():
        con = duckdb.connect(DB_PATH)
        con.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP,
                end_time TIMESTAMP, rows_loaded INT, status VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO pipeline_runs 
            SELECT 
                SUBSTR(CAST(UUID() AS VARCHAR), 1, 8),
                'nammamart_etl_akash',
                CURRENT_TIMESTAMP - INTERVAL 1 MINUTE,
                CURRENT_TIMESTAMP,
                (SELECT COUNT(*) FROM etl_fact_orders),
                'SUCCESS'
        """)
        con.close()

    extract_and_validate() >> run_audit()

nammamart_etl_pipeline()
