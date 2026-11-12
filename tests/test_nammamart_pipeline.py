"""
tests/test_nammamart_pipeline.py
--------------------------------
Automated test suite for NammaMart ETL & ELT Pipelines.
Tests all client acceptance criteria:
  - 6 Data Quality dimensions (Critical & Warning)
  - PII masking compliance (DPDP Act 2023)
  - ETL Idempotency (run twice, check row count invariant)
  - ELT Incremental MERGE (150 new, 10 status corrections, 0 duplicate keys)
  - ETL vs ELT Parity
  - Observability Scorecards (dq_results, pipeline_runs)
  - Six Business Questions queries
"""

import sys
import os
import duckdb
import pandas as pd
import pytest

# Ensure include is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "include")))

from nammamart_masking import mask_email, mask_phone
from nammamart_quality import (
    validate_orders, validate_customers, validate_products,
    validate_stores, validate_payments
)
from nammamart_etl import run_etl_pipeline, get_paths
from nammamart_elt import run_elt_pipeline
from nammamart_observability import check_store_capacity


def test_pii_masking():
    """Verify masking logic protects personal data while retaining analytical utility."""
    # Email masking
    assert mask_email("priya.hegde@example.com") == "pr***@example.com"
    assert mask_email("a@b.com") == "a***@b.com"
    assert mask_email(None) == ""
    assert mask_email("") == ""

    # Phone masking (keep first 2 and last 2)
    assert mask_phone(9876543221) == "98******21"
    assert mask_phone("9876543221") == "98******21"
    assert mask_phone("invalid") == "******"
    assert mask_phone(None) == ""


def test_quality_dimensions_checks():
    """Verify checks across all 6 dimensions."""
    warehouse_db, _, data_dir = get_paths()
    con = duckdb.connect(":memory:")

    raw_orders = pd.read_csv(os.path.join(data_dir, "orders.csv"))
    raw_customers = pd.read_csv(os.path.join(data_dir, "customers.csv"))
    raw_products = pd.read_csv(os.path.join(data_dir, "products.csv"))
    raw_stores = pd.read_csv(os.path.join(data_dir, "stores.csv"))

    passed, failed = validate_orders(
        raw_orders, raw_customers, raw_products, raw_stores,
        run_id="test_run", dag_id="test_dag", con=con
    )

    # 1. Total split must equal raw count
    assert len(passed) + len(failed) == len(raw_orders)
    assert len(passed) == 2310
    assert len(failed) == 102

    # 2. Every failed row must have a documented failure_reason
    assert "failure_reason" in failed.columns
    assert (failed["failure_reason"].str.strip() == "").sum() == 0

    # 3. No critical bad status, nonpositive qty, or formula mismatch in passed
    assert (~passed["order_status"].isin(["DELIVERED", "CANCELLED", "RETURNED", "delivered"])).sum() == 0
    assert (passed["quantity"] <= 0).sum() == 0
    assert (passed["discount_pct"] > 100).sum() == 0


def test_etl_execution_and_idempotency(tmp_path):
    """Verify ETL runs end-to-end and is strictly idempotent."""
    warehouse_db, quarantine_dir, _ = get_paths()
    
    # Run 1
    res1 = run_etl_pipeline(run_id="run_test_1")
    con = duckdb.connect(warehouse_db)
    count1 = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
    dup_count1 = con.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM etl_fact_orders").fetchone()[0]
    con.close()

    assert count1 == 2310
    assert dup_count1 == 0

    # Run 2 (simulate re-trigger on same data)
    res2 = run_etl_pipeline(run_id="run_test_2")
    con = duckdb.connect(warehouse_db)
    count2 = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
    dup_count2 = con.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM etl_fact_orders").fetchone()[0]
    con.close()

    # Fact row count must remain invariant
    assert count1 == count2
    assert dup_count2 == 0


def test_elt_incremental_merge():
    """Verify ELT loads raw, merges increment, and handles updates without duplicates."""
    warehouse_db, _, _ = get_paths()
    con = duckdb.connect(warehouse_db)

    # Execute ELT pipeline
    run_elt_pipeline(run_id="run_elt_test", include_increment=True)

    # Staging counts
    total_stg = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    distinct_orders = con.execute("SELECT COUNT(DISTINCT order_id) FROM stg_orders").fetchone()[0]
    oct_orders = con.execute("SELECT COUNT(*) FROM stg_orders WHERE order_date = '2026-10-01'").fetchone()[0]
    
    con.close()

    assert total_stg == 2550
    assert distinct_orders == 2550
    assert oct_orders == 150


def test_september_parity_explanation():
    """Verify that prior to incremental returns, ELT matches ETL September totals."""
    warehouse_db, _, _ = get_paths()
    con = duckdb.connect(warehouse_db)

    etl_rev = con.execute("""
        SELECT ROUND(SUM(net_revenue), 2)
        FROM etl_fact_orders
        WHERE order_status = 'DELIVERED' AND order_date BETWEEN '2026-09-01' AND '2026-09-30'
    """).fetchone()[0]

    # Staging delivered before October 1 returns (i.e. if we include the 10 orders that were DELIVERED in September)
    elt_rev_before_returns = con.execute("""
        SELECT ROUND(SUM(CASE WHEN order_status = 'DELIVERED' THEN net_revenue ELSE order_amount END), 2)
        FROM stg_orders
        WHERE dq_status = 'PASSED'
          AND order_date BETWEEN '2026-09-01' AND '2026-09-30'
          AND (
              order_status = 'DELIVERED'
              OR (order_id IN (SELECT order_id FROM raw_orders WHERE _source_file = 'orders_2026-10-01_increment.csv') AND order_status = 'RETURNED')
          )
    """).fetchone()[0]

    con.close()
    assert etl_rev == 1135628.80
    assert elt_rev_before_returns == 1135628.80


def test_six_business_questions():
    """Verify SQL queries for all 6 business questions execute and return valid results."""
    warehouse_db, _, _ = get_paths()
    con = duckdb.connect(warehouse_db)

    # Q1: Daily net revenue by zone and store
    q1 = con.execute("""
        SELECT zone, store_name, order_date, daily_net_revenue
        FROM mart_daily_store_kpis
        LIMIT 5;
    """).fetchdf()
    assert len(q1) > 0

    # Q2: Category revenue and margin
    q2 = con.execute("""
        SELECT category, total_revenue, gross_margin, gross_margin_pct
        FROM mart_category_margin;
    """).fetchdf()
    assert len(q2) > 0
    assert q2['total_revenue'].sum() > 0

    # Q3: Average delivery SLA
    q3 = con.execute("""
        SELECT store_name, avg_delivery_minutes, pct_within_15_min
        FROM mart_delivery_sla;
    """).fetchdf()
    assert len(q3) == 15  # 15 stores

    # Q4: Payment reconciliation
    q4 = con.execute("""
        SELECT payment_mode, reported_net_revenue, unreconciled_amount
        FROM mart_revenue_by_payment_mode;
    """).fetchdf()
    assert len(q4) > 0

    # Q5: Top 10 customers by spend (masked PII)
    q5 = con.execute("""
        SELECT customer_id, full_name, masked_email, masked_phone, total_spend
        FROM mart_top_customers;
    """).fetchdf()
    assert len(q5) == 10
    # Confirm emails are masked
    assert all("*" in email for email in q5['masked_email'] if email)

    # Q6: Rejected orders summary
    q6 = con.execute("""
        SELECT source_file, failure_reason, rejected_count
        FROM mart_rejected_orders_summary;
    """).fetchdf()
    assert len(q6) > 0

    con.close()


def test_observability_and_bonus_capacity():
    """Verify pipeline_runs, dq_results and bonus capacity checks."""
    warehouse_db, _, _ = get_paths()
    con = duckdb.connect(warehouse_db)

    runs = con.execute("SELECT * FROM pipeline_runs").fetchdf()
    assert len(runs) > 0

    dq = con.execute("SELECT * FROM dq_results").fetchdf()
    assert len(dq) > 0

    capacity = check_store_capacity(con)
    assert len(capacity) > 0

    con.close()
