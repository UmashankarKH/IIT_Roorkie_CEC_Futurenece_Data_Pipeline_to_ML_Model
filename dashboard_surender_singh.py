import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(page_title="NammaMart Retail Analytics", layout="wide")

st.title("NammaMart Retail Analytics")
st.caption("Revenue, store performance, category margins, and payment exceptions")

PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = Path(
    os.environ.get(
        "NAMMAMART_DB_PATH",
        PROJECT_DIR / "include" / "warehouse_surender_singh.duckdb",
    )
).expanduser()

pipeline = st.radio(
    "Pipeline data",
    ("ETL", "ELT"),
    horizontal=True,
)

if st.button("Refresh dashboard data"):
    st.cache_data.clear()


@st.cache_data(ttl=10)
def load_data(database_path, selected_pipeline):
    if not os.path.isfile(database_path):
        raise FileNotFoundError(
            f"DuckDB warehouse not found: {database_path}. "
            "Run the ETL or ELT DAG first to create it."
        )

    if selected_pipeline == "ETL":
        store_query = """
            SELECT
                CAST(order_date AS DATE) AS order_date,
                zone,
                store_name,
                SUM(net_revenue) AS net_revenue
            FROM etl_daily_zone_revenue
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        """
        category_query = """
            SELECT category, total_revenue, gross_margin
            FROM etl_category_margin
            ORDER BY total_revenue DESC
        """
        reconciliation_query = """
            SELECT
                order_id,
                expected_amount,
                actual_paid,
                variance,
                reconciliation_status
            FROM etl_payment_reconciliation
            WHERE reconciliation_status <> 'Reconciled'
            ORDER BY variance DESC
            LIMIT 100
        """
        dag_id = "nammamart_etl_pipeline"
    else:
        store_query = """
            SELECT
                order_date,
                zone,
                store_name,
                SUM(net_revenue) AS net_revenue
            FROM mart_daily_store_kpis
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        """
        category_query = """
            SELECT category, total_revenue, gross_margin
            FROM mart_category_margin
            ORDER BY total_revenue DESC
        """
        reconciliation_query = """
            SELECT
                order_id,
                calculated_amount AS expected_amount,
                amount_paid AS actual_paid,
                unreconciled_amount AS variance,
                reconciliation_status
            FROM mart_payment_reconciliation
            WHERE reconciliation_status <> 'RECONCILED'
            ORDER BY variance DESC
            LIMIT 100
        """
        dag_id = "nammamart_elt_surender_singh"

    with duckdb.connect(database_path, read_only=True) as conn:
        df_stores = conn.execute(store_query).fetchdf()
        df_categories = conn.execute(category_query).fetchdf()
        df_reconciliation = conn.execute(reconciliation_query).fetchdf()
        df_orders = conn.execute(
            f"""
            SELECT
                COUNT(DISTINCT order_id) AS total_orders,
                COUNT(DISTINCT CASE
                    WHEN order_status = 'DELIVERED' THEN order_id
                END) AS delivered_orders
            FROM {"etl_fact_orders" if selected_pipeline == "ETL" else "stg_orders"}
            """
        ).fetchdf()
        df_latest_run = conn.execute(
            """
            SELECT start_time, status, rows_extracted, rows_loaded,
                   rows_quarantined
            FROM pipeline_runs
            WHERE dag_id = ?
            ORDER BY end_time DESC
            LIMIT 1
            """,
            [dag_id],
        ).fetchdf()

    return df_stores, df_categories, df_reconciliation, df_orders, df_latest_run


try:
    (
        df_stores,
        df_categories,
        df_reconciliation,
        df_orders,
        df_latest_run,
    ) = load_data(str(DB_PATH), pipeline)
except (duckdb.Error, OSError, ValueError) as exc:
    st.error(f"Could not load {pipeline} dashboard data: {exc}")
    st.info(
        "Start the Astro Airflow project and run the corresponding NammaMart "
        "DAG. The dashboard reads the DuckDB warehouse; it does not trigger "
        "the pipeline."
    )
    st.stop()

if df_latest_run.empty:
    st.warning(f"No run-audit record was found for the {pipeline} pipeline.")
else:
    latest_run = df_latest_run.iloc[0]
    st.caption(
        f"Latest {pipeline} run: {latest_run['start_time']} — "
        f"{latest_run['status']} | extracted {latest_run['rows_extracted']:,}, "
        f"loaded {latest_run['rows_loaded']:,}, "
        f"quarantined {latest_run['rows_quarantined']:,}"
    )

total_revenue = (
    df_stores["net_revenue"].sum() if not df_stores.empty else 0
)
total_orders = (
    int(df_orders.iloc[0]["total_orders"]) if not df_orders.empty else 0
)
delivered_orders = (
    int(df_orders.iloc[0]["delivered_orders"]) if not df_orders.empty else 0
)

col1, col2, col3 = st.columns(3)
col1.metric("Delivered Net Revenue", f"₹{total_revenue:,.2f}")
col2.metric("Total Orders", f"{total_orders:,}")
col3.metric("Delivered Orders", f"{delivered_orders:,}")

left, right = st.columns(2)

with left:
    st.subheader("Revenue by Zone and Store")
    if df_stores.empty:
        st.info("No store revenue rows are available.")
    else:
        store_summary = (
            df_stores.groupby(["zone", "store_name"], as_index=False)
            .agg(net_revenue=("net_revenue", "sum"))
        )
        fig_store = px.bar(
            store_summary,
            x="store_name",
            y="net_revenue",
            color="zone",
            title="Delivered net revenue by store (₹)",
            text_auto=".2s",
        )
        st.plotly_chart(fig_store, width="stretch")

with right:
    st.subheader("Category Gross Margin")
    if df_categories.empty:
        st.info("No category margin rows are available.")
    else:
        fig_margin = px.bar(
            df_categories,
            x="category",
            y="gross_margin",
            color="category",
            title="Gross margin by product category (₹)",
            text_auto=".2s",
        )
        st.plotly_chart(fig_margin, width="stretch")

st.subheader("Payment Reconciliation Exceptions")
if df_reconciliation.empty:
    st.success("No payment reconciliation exceptions were found.")
else:
    st.dataframe(df_reconciliation, width="stretch")

st.caption(f"Warehouse: `{DB_PATH}` · Dashboard data refreshes every 10 seconds.")
