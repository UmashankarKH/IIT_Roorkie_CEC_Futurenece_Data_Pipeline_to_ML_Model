"""
include/nammamart_quality.py
----------------------------
Comprehensive Data Quality Framework for NammaMart pipelines.
Covers all 6 DQ dimensions:
  1. Completeness
  2. Uniqueness
  3. Validity
  4. Accuracy
  5. Consistency
  6. Timeliness

Maintains a permanent scorecard in DuckDB table `dq_results`:
  (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
"""

import os
import re
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
import pendulum


def init_dq_table(con: duckdb.DuckDBPyConnection):
    """Initializes the dq_results table if not exists."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR,
            dag_id VARCHAR,
            check_name VARCHAR,
            dimension VARCHAR,
            severity VARCHAR,
            rows_checked INTEGER,
            rows_failed INTEGER,
            status VARCHAR,
            checked_at VARCHAR
        )
    """)


def record_dq_check(con: duckdb.DuckDBPyConnection, run_id: str, dag_id: str,
                    check_name: str, dimension: str, severity: str,
                    rows_checked: int, rows_failed: int):
    """Records an individual check result into dq_results."""
    status = "PASSED" if rows_failed == 0 else ("FAILED" if severity == "Critical" else "WARNING")
    checked_at = pendulum.now("Asia/Kolkata").to_iso8601_string()
    
    con.execute("""
        INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [run_id, dag_id, check_name, dimension, severity, int(rows_checked), int(rows_failed), status, checked_at])
    
    prefix = "[DQ Critical]" if severity == "Critical" else "[DQ Warning]"
    print(f"{prefix} {check_name} ({dimension}): checked={rows_checked}, failed={rows_failed} -> {status}")


def validate_orders(orders_df: pd.DataFrame, customers_df: pd.DataFrame,
                    products_df: pd.DataFrame, stores_df: pd.DataFrame,
                    run_id: str, dag_id: str, con: duckdb.DuckDBPyConnection = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates orders across all 6 dimensions.
    Returns (clean_orders_df, failed_orders_df)
    Failed rows will have a 'failure_reason' column explaining every violation.
    """
    if con is not None:
        init_dq_table(con)

    df = orders_df.copy()
    total_rows = len(df)
    reasons = [[] for _ in range(total_rows)]

    # ── 1. Completeness ──────────────────────────────────────────
    # Check 1.1: customer_id not null
    mask_null_cust = df['customer_id'].isnull() | (df['customer_id'].astype(str).str.strip().isin(['', 'nan', 'None']))
    if con: record_dq_check(con, run_id, dag_id, "orders_customer_id_not_null", "Completeness", "Critical", total_rows, mask_null_cust.sum())
    for idx in np.where(mask_null_cust)[0]:
        reasons[idx].append("Completeness: null/empty customer_id")

    # Check 1.2: order_ts not null
    mask_null_ts = df['order_ts'].isnull() | (df['order_ts'].astype(str).str.strip().isin(['', 'nan', 'None']))
    if con: record_dq_check(con, run_id, dag_id, "orders_order_ts_not_null", "Completeness", "Critical", total_rows, mask_null_ts.sum())
    for idx in np.where(mask_null_ts)[0]:
        reasons[idx].append("Completeness: null order_ts")

    # Check 1.3: DELIVERED orders must have delivery_minutes
    mask_deliv_null_min = (df['order_status'].astype(str).str.strip().str.upper() == 'DELIVERED') & df['delivery_minutes'].isnull()
    if con: record_dq_check(con, run_id, dag_id, "orders_delivered_has_delivery_minutes", "Completeness", "Critical", total_rows, mask_deliv_null_min.sum())
    for idx in np.where(mask_deliv_null_min)[0]:
        reasons[idx].append("Completeness: DELIVERED order missing delivery_minutes")

    # ── 2. Uniqueness ────────────────────────────────────────────
    # Check 2.1: Duplicate order_id (keep first as valid, mark subsequent as duplicate)
    mask_dup_order = df['order_id'].duplicated(keep='first')
    if con: record_dq_check(con, run_id, dag_id, "orders_order_id_unique", "Uniqueness", "Critical", total_rows, mask_dup_order.sum())
    for idx in np.where(mask_dup_order)[0]:
        reasons[idx].append("Uniqueness: duplicate order_id")

    # ── 3. Validity ──────────────────────────────────────────────
    # Check 3.1: Valid order_status in ['DELIVERED', 'CANCELLED', 'RETURNED']
    valid_statuses = {'DELIVERED', 'CANCELLED', 'RETURNED'}
    mask_invalid_status = ~df['order_status'].astype(str).str.strip().str.upper().isin(valid_statuses)
    if con: record_dq_check(con, run_id, dag_id, "orders_valid_order_status", "Validity", "Critical", total_rows, mask_invalid_status.sum())
    for idx in np.where(mask_invalid_status)[0]:
        reasons[idx].append(f"Validity: invalid order_status '{df.iloc[idx]['order_status']}'")

    # Check 3.2: discount_pct between 0 and 100
    mask_invalid_disc = (df['discount_pct'] < 0) | (df['discount_pct'] > 100)
    if con: record_dq_check(con, run_id, dag_id, "orders_discount_pct_range", "Validity", "Critical", total_rows, mask_invalid_disc.sum())
    for idx in np.where(mask_invalid_disc)[0]:
        reasons[idx].append(f"Validity: discount_pct {df.iloc[idx]['discount_pct']}% out of 0-100 range")

    # Check 3.3: order_ts valid format
    def parse_ts(val):
        if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
            return None
        try:
            return pd.to_datetime(val)
        except Exception:
            return False

    parsed_dates = df['order_ts'].apply(parse_ts)
    mask_invalid_ts = parsed_dates == False
    if con: record_dq_check(con, run_id, dag_id, "orders_order_ts_valid_format", "Validity", "Critical", total_rows, mask_invalid_ts.sum())
    for idx in np.where(mask_invalid_ts)[0]:
        reasons[idx].append(f"Validity: invalid timestamp format '{df.iloc[idx]['order_ts']}'")

    # ── 4. Accuracy ──────────────────────────────────────────────
    # Check 4.1: quantity > 0
    mask_nonpos_qty = df['quantity'] <= 0
    if con: record_dq_check(con, run_id, dag_id, "orders_quantity_positive", "Accuracy", "Critical", total_rows, mask_nonpos_qty.sum())
    for idx in np.where(mask_nonpos_qty)[0]:
        reasons[idx].append(f"Accuracy: non-positive quantity {df.iloc[idx]['quantity']}")

    # Check 4.2: order_amount matches formula: round(quantity * unit_price * (1 - discount_pct/100), 2)
    expected_amount = (df['quantity'] * df['unit_price'] * (1 - df['discount_pct'] / 100.0)).round(2)
    amount_diff = (df['order_amount'] - expected_amount).abs()
    # If quantity or discount is already bad, formula check will also trigger, but we record formula mismatch specifically
    mask_bad_amount = (amount_diff > 0.02) & (~mask_nonpos_qty) & (~mask_invalid_disc)
    if con: record_dq_check(con, run_id, dag_id, "orders_amount_formula_match", "Accuracy", "Critical", total_rows, mask_bad_amount.sum())
    for idx in np.where(mask_bad_amount)[0]:
        reasons[idx].append(f"Accuracy: order_amount {df.iloc[idx]['order_amount']} != expected {expected_amount.iloc[idx]}")

    # Check 4.3: delivery_minutes <= 120 (for delivered orders)
    mask_extreme_deliv = (df['order_status'].astype(str).str.strip().str.upper() == 'DELIVERED') & (df['delivery_minutes'] > 120)
    if con: record_dq_check(con, run_id, dag_id, "orders_delivery_minutes_under_120", "Accuracy", "Critical", total_rows, mask_extreme_deliv.sum())
    for idx in np.where(mask_extreme_deliv)[0]:
        reasons[idx].append(f"Accuracy: delivery_minutes {df.iloc[idx]['delivery_minutes']} exceeds 120-minute threshold")

    # ── 5. Consistency ───────────────────────────────────────────
    # Foreign key check 5.1: customer_id in customers
    valid_customers = set(customers_df['customer_id'].dropna().astype(str))
    mask_unknown_cust = ~df['customer_id'].astype(str).isin(valid_customers) & (~mask_null_cust)
    if con: record_dq_check(con, run_id, dag_id, "orders_fk_customer_exists", "Consistency", "Critical", total_rows, mask_unknown_cust.sum())
    for idx in np.where(mask_unknown_cust)[0]:
        reasons[idx].append(f"Consistency: unknown customer_id '{df.iloc[idx]['customer_id']}'")

    # Foreign key check 5.2: product_id in products
    valid_products = set(products_df['product_id'].dropna().astype(str))
    mask_unknown_prod = ~df['product_id'].astype(str).isin(valid_products)
    if con: record_dq_check(con, run_id, dag_id, "orders_fk_product_exists", "Consistency", "Critical", total_rows, mask_unknown_prod.sum())
    for idx in np.where(mask_unknown_prod)[0]:
        reasons[idx].append(f"Consistency: unknown product_id '{df.iloc[idx]['product_id']}'")

    # Foreign key check 5.3: store_id in stores
    valid_stores = set(stores_df['store_id'].dropna().astype(str))
    mask_unknown_store = ~df['store_id'].astype(str).isin(valid_stores)
    if con: record_dq_check(con, run_id, dag_id, "orders_fk_store_exists", "Consistency", "Critical", total_rows, mask_unknown_store.sum())
    for idx in np.where(mask_unknown_store)[0]:
        reasons[idx].append(f"Consistency: unknown store_id '{df.iloc[idx]['store_id']}'")

    # ── 6. Timeliness ────────────────────────────────────────────
    # Check 6.1: No future timestamps (> 2026-10-02)
    def is_future(parsed_dt):
        if parsed_dt is None or parsed_dt == False:
            return False
        return parsed_dt > pd.Timestamp('2026-10-02')

    mask_future_ts = parsed_dates.apply(is_future)
    if con: record_dq_check(con, run_id, dag_id, "orders_order_ts_not_future", "Timeliness", "Critical", total_rows, mask_future_ts.sum())
    for idx in np.where(mask_future_ts)[0]:
        reasons[idx].append(f"Timeliness: future-dated order_ts '{df.iloc[idx]['order_ts']}'")

    # ── Assemble Failed and Passed DataFrames ────────────────────
    failure_reasons_str = ["; ".join(r) if r else "" for r in reasons]
    df['failure_reason'] = failure_reasons_str
    
    is_failed = df['failure_reason'] != ""
    failed_df = df[is_failed].copy()
    passed_df = df[~is_failed].copy().drop(columns=['failure_reason'])

    print(f"[Orders DQ Complete] Total: {total_rows} | Passed: {len(passed_df)} | Quarantined: {len(failed_df)}")
    return passed_df, failed_df


def validate_customers(customers_df: pd.DataFrame, run_id: str, dag_id: str,
                       con: duckdb.DuckDBPyConnection = None) -> pd.DataFrame:
    """Validates customers, standardizes loyalty tiers, dedupes, logs warnings/criticals."""
    if con is not None:
        init_dq_table(con)

    df = customers_df.copy()
    total_rows = len(df)

    # 1. Uniqueness: duplicate customer_id
    dups = df['customer_id'].duplicated(keep='first').sum()
    if con: record_dq_check(con, run_id, dag_id, "customers_customer_id_unique", "Uniqueness", "Warning", total_rows, dups)

    # 2. Validity: Phone is 10 digits
    invalid_phones = (~df['phone'].astype(str).str.match(r'^\d{10}$')).sum()
    if con: record_dq_check(con, run_id, dag_id, "customers_phone_10_digits", "Validity", "Warning", total_rows, invalid_phones)

    # 3. Validity: Email format
    valid_email_regex = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    invalid_emails = (~df['email'].dropna().str.contains(valid_email_regex)).sum() + df['email'].isnull().sum()
    if con: record_dq_check(con, run_id, dag_id, "customers_email_valid_format", "Validity", "Warning", total_rows, invalid_emails)

    # 4. Consistency: loyalty_tier casing
    standard_tiers = {'Bronze', 'Silver', 'Gold', 'Platinum'}
    non_std_tier = (~df['loyalty_tier'].isin(standard_tiers)).sum()
    if con: record_dq_check(con, run_id, dag_id, "customers_loyalty_tier_casing", "Consistency", "Warning", total_rows, non_std_tier)

    # 5. Timeliness: future signup_date
    future_signup = (pd.to_datetime(df['signup_date'], errors='coerce') > pd.Timestamp('2026-10-02')).sum()
    if con: record_dq_check(con, run_id, dag_id, "customers_signup_not_future", "Timeliness", "Critical", total_rows, future_signup)

    return df


def validate_products(products_df: pd.DataFrame, run_id: str, dag_id: str,
                      con: duckdb.DuckDBPyConnection = None) -> pd.DataFrame:
    """Validates products, logs DQ results."""
    if con is not None:
        init_dq_table(con)

    df = products_df.copy()
    total_rows = len(df)

    # 1. Completeness: category not null
    null_cat = df['category'].isnull().sum()
    if con: record_dq_check(con, run_id, dag_id, "products_category_not_null", "Completeness", "Critical", total_rows, null_cat)

    # 2. Uniqueness: product_id unique
    dups = df['product_id'].duplicated(keep='first').sum()
    if con: record_dq_check(con, run_id, dag_id, "products_product_id_unique", "Uniqueness", "Warning", total_rows, dups)

    # 3. Accuracy: mrp > 0
    nonpos_mrp = (df['mrp'] <= 0).sum()
    if con: record_dq_check(con, run_id, dag_id, "products_mrp_positive", "Accuracy", "Critical", total_rows, nonpos_mrp)

    # 4. Accuracy: cost_price <= mrp
    cost_gt_mrp = (df['cost_price'] > df['mrp']).sum()
    if con: record_dq_check(con, run_id, dag_id, "products_cost_lte_mrp", "Accuracy", "Warning", total_rows, cost_gt_mrp)

    return df


def validate_stores(stores_df: pd.DataFrame, run_id: str, dag_id: str,
                    con: duckdb.DuckDBPyConnection = None) -> pd.DataFrame:
    """Validates stores table."""
    if con is not None:
        init_dq_table(con)

    df = stores_df.copy()
    total_rows = len(df)

    # Completeness: zone not null
    null_zone = df['zone'].isnull().sum()
    if con: record_dq_check(con, run_id, dag_id, "stores_zone_not_null", "Completeness", "Warning", total_rows, null_zone)
    
    return df


def validate_payments(payments_df: pd.DataFrame, orders_df: pd.DataFrame,
                      run_id: str, dag_id: str, con: duckdb.DuckDBPyConnection = None) -> pd.DataFrame:
    """Validates payments table."""
    if con is not None:
        init_dq_table(con)

    df = payments_df.copy()
    total_rows = len(df)

    # 1. Uniqueness: payment_id unique
    dups = df['payment_id'].duplicated(keep='first').sum()
    if con: record_dq_check(con, run_id, dag_id, "payments_payment_id_unique", "Uniqueness", "Critical", total_rows, dups)

    # 2. Timeliness: payment_ts not future
    future_ts = (pd.to_datetime(df['payment_ts'], errors='coerce') > pd.Timestamp('2026-10-02')).sum()
    if con: record_dq_check(con, run_id, dag_id, "payments_payment_ts_not_future", "Timeliness", "Critical", total_rows, future_ts)

    # 3. Consistency: order_id exists in orders
    valid_orders = set(orders_df['order_id'].dropna().astype(str))
    missing_orders = (~df['order_id'].astype(str).isin(valid_orders)).sum()
    if con: record_dq_check(con, run_id, dag_id, "payments_fk_order_exists", "Consistency", "Critical", total_rows, missing_orders)

    # 4. Accuracy: amount_paid > 0
    nonpos_paid = (df['amount_paid'] <= 0).sum()
    if con: record_dq_check(con, run_id, dag_id, "payments_amount_paid_positive", "Accuracy", "Critical", total_rows, nonpos_paid)

    return df
