"""
include/nammamart_dq.py
-----------------------
Data Quality validation engine covering all six dimensions for NammaMart pipelines:
1. Completeness
2. Uniqueness
3. Validity
4. Accuracy
5. Consistency
6. Timeliness
Also includes Volume Anomaly and Store Capacity checks.
Logs results for the `dq_results` audit table.
"""

import os
import re
from datetime import datetime
import pandas as pd
import pendulum

from nammamart_masking import (
    standardise_payment_mode,
    standardise_order_status,
    standardise_loyalty_tier,
)


def run_data_quality_checks(
    orders_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    stores_df: pd.DataFrame,
    run_id: str = "manual_run",
    dag_id: str = "nammamart_etl_nihir",
) -> dict:
    """
    Executes checks across all 6 dimensions.
    Returns:
      {
        "passed_orders": pd.DataFrame,
        "failed_orders": pd.DataFrame,
        "passed_payments": pd.DataFrame,
        "failed_payments": pd.DataFrame,
        "passed_customers": pd.DataFrame,
        "failed_customers": pd.DataFrame,
        "passed_products": pd.DataFrame,
        "failed_products": pd.DataFrame,
        "passed_stores": pd.DataFrame,
        "failed_stores": pd.DataFrame,
        "dq_results": list of dicts,
        "checked_at": str,
      }
    """
    checked_at = pendulum.now("Asia/Kolkata").to_iso8601_string()
    dq_records = []

    def record_dq(check_name, dimension, severity, rows_checked, rows_failed):
        status = "PASSED" if rows_failed == 0 else ("FAILED" if severity == "Critical" else "WARNING")
        dq_records.append({
            "run_id": run_id,
            "dag_id": dag_id,
            "check_name": check_name,
            "dimension": dimension,
            "severity": severity,
            "rows_checked": int(rows_checked),
            "rows_failed": int(rows_failed),
            "status": status,
            "checked_at": checked_at,
        })

    # =========================================================================
    # 1. STORES VALIDATION
    # =========================================================================
    st_df = stores_df.copy()
    st_failed_reasons = {}

    # Completeness: zone not null
    mask_null_zone = st_df["zone"].isna() | (st_df["zone"].astype(str).str.strip() == "")
    for idx in st_df[mask_null_zone].index:
        st_failed_reasons.setdefault(idx, []).append("completeness:null_zone")
    record_dq("stores_zone_not_null", "Completeness", "Warning", len(st_df), mask_null_zone.sum())

    # Accuracy / Validity: capacity_orders_per_day > 0
    cap_numeric = pd.to_numeric(st_df["capacity_orders_per_day"], errors="coerce").fillna(0)
    mask_zero_cap = cap_numeric <= 0
    for idx in st_df[mask_zero_cap].index:
        st_failed_reasons.setdefault(idx, []).append("accuracy:capacity_le_zero")
    record_dq("stores_capacity_positive", "Accuracy", "Warning", len(st_df), mask_zero_cap.sum())

    # Uniqueness: store_id unique
    mask_dup_store = st_df.duplicated(subset=["store_id"], keep="first")
    for idx in st_df[mask_dup_store].index:
        st_failed_reasons.setdefault(idx, []).append("uniqueness:duplicate_store_id")
    record_dq("stores_store_id_unique", "Uniqueness", "Critical", len(st_df), mask_dup_store.sum())

    st_failed_idx = [i for i, r in st_failed_reasons.items() if any(x.startswith("uniqueness") for x in r)]
    failed_stores = st_df.loc[st_failed_idx].copy()
    if not failed_stores.empty:
        failed_stores["failure_reason"] = ["; ".join(st_failed_reasons[i]) for i in st_failed_idx]
    else:
        failed_stores["failure_reason"] = pd.Series(dtype=str)
    passed_stores = st_df.drop(index=st_failed_idx).copy()

    # Standardise store zone (BTM Layout S07 is in South zone)
    passed_stores["zone"] = passed_stores.apply(
        lambda r: "South" if str(r["zone"]).strip() == "" and "BTM" in str(r["area"]) else str(r["zone"]).strip(),
        axis=1
    )

    # =========================================================================
    # 2. PRODUCTS VALIDATION
    # =========================================================================
    pr_df = products_df.copy()
    pr_failed_reasons = {}

    # Uniqueness: product_id unique (drop old versions)
    mask_dup_prod = pr_df.duplicated(subset=["product_id"], keep="first")
    for idx in pr_df[mask_dup_prod].index:
        pr_failed_reasons.setdefault(idx, []).append("uniqueness:duplicate_product_id")
    record_dq("products_product_id_unique", "Uniqueness", "Critical", len(pr_df), mask_dup_prod.sum())

    # Completeness: category not null
    mask_null_cat = pr_df["category"].isna() | (pr_df["category"].astype(str).str.strip() == "")
    for idx in pr_df[mask_null_cat].index:
        pr_failed_reasons.setdefault(idx, []).append("completeness:null_category")
    record_dq("products_category_not_null", "Completeness", "Warning", len(pr_df), mask_null_cat.sum())

    # Accuracy: mrp > 0 and cost_price <= mrp
    mrp_num = pd.to_numeric(pr_df["mrp"], errors="coerce").fillna(0)
    cp_num = pd.to_numeric(pr_df["cost_price"], errors="coerce").fillna(0)
    mask_neg_mrp = mrp_num <= 0
    for idx in pr_df[mask_neg_mrp].index:
        pr_failed_reasons.setdefault(idx, []).append("accuracy:mrp_le_zero")
    record_dq("products_mrp_positive", "Accuracy", "Critical", len(pr_df), mask_neg_mrp.sum())

    mask_cp_gt_mrp = (cp_num > mrp_num) & (~mask_neg_mrp)
    for idx in pr_df[mask_cp_gt_mrp].index:
        pr_failed_reasons.setdefault(idx, []).append("accuracy:cost_price_gt_mrp")
    record_dq("products_cost_price_lte_mrp", "Accuracy", "Warning", len(pr_df), mask_cp_gt_mrp.sum())

    # Products with duplicate product_id or invalid mrp <= 0 are critical failures
    pr_crit_idx = [i for i, r in pr_failed_reasons.items() if any(x.startswith("uniqueness") or x.startswith("accuracy:mrp_le_zero") for x in r)]
    failed_products = pr_df.loc[pr_crit_idx].copy()
    failed_products["failure_reason"] = ["; ".join(pr_failed_reasons[i]) for i in pr_crit_idx]
    passed_products = pr_df.drop(index=pr_crit_idx).copy()

    # Remediation for warning rows in dimension table:
    # GoldenGrain products (Dal, Rice, Oil) belong to 'Staples'
    passed_products["category"] = passed_products.apply(
        lambda r: "Staples" if str(r["category"]).strip() == "" and "GoldenGrain" in str(r["product_name"]) else str(r["category"]).strip(),
        axis=1
    )

    # =========================================================================
    # 3. CUSTOMERS VALIDATION
    # =========================================================================
    cu_df = customers_df.copy()
    cu_failed_reasons = {}

    # Uniqueness: customer_id unique
    mask_dup_cust = cu_df.duplicated(subset=["customer_id"], keep="first")
    for idx in cu_df[mask_dup_cust].index:
        cu_failed_reasons.setdefault(idx, []).append("uniqueness:duplicate_customer_id")
    record_dq("customers_customer_id_unique", "Uniqueness", "Critical", len(cu_df), mask_dup_cust.sum())

    # Validity: email format
    email_regex = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    mask_bad_email = ~cu_df["email"].fillna("").astype(str).str.strip().apply(lambda x: bool(email_regex.match(x)))
    for idx in cu_df[mask_bad_email].index:
        cu_failed_reasons.setdefault(idx, []).append("validity:invalid_email_format")
    record_dq("customers_email_format", "Validity", "Warning", len(cu_df), mask_bad_email.sum())

    # Validity: phone 10 digits
    mask_bad_phone = ~cu_df["phone"].fillna("").astype(str).str.strip().apply(
        lambda x: bool(re.match(r"^\d{10}$", re.sub(r"^\+91[-\s]?", "", x)))
    )
    for idx in cu_df[mask_bad_phone].index:
        cu_failed_reasons.setdefault(idx, []).append("validity:invalid_phone_digits")
    record_dq("customers_phone_10_digits", "Validity", "Warning", len(cu_df), mask_bad_phone.sum())

    # Timeliness: signup_date not in future (> 2026-10-01)
    mask_future_signup = cu_df["signup_date"].fillna("").astype(str).str.strip() > "2026-10-01"
    for idx in cu_df[mask_future_signup].index:
        cu_failed_reasons.setdefault(idx, []).append("timeliness:future_signup_date")
    record_dq("customers_signup_not_future", "Timeliness", "Warning", len(cu_df), mask_future_signup.sum())

    # Consistency: loyalty_tier casing
    mask_tier_casing = ~cu_df["loyalty_tier"].isin(["Gold", "Silver", "Bronze", "Platinum"])
    for idx in cu_df[mask_tier_casing].index:
        cu_failed_reasons.setdefault(idx, []).append("consistency:loyalty_tier_casing")
    record_dq("customers_loyalty_tier_casing", "Consistency", "Warning", len(cu_df), mask_tier_casing.sum())

    cu_crit_idx = [i for i, r in cu_failed_reasons.items() if any(x.startswith("uniqueness") for x in r)]
    failed_customers = cu_df.loc[cu_crit_idx].copy()
    failed_customers["failure_reason"] = ["; ".join(cu_failed_reasons[i]) for i in cu_crit_idx]
    passed_customers = cu_df.drop(index=cu_crit_idx).copy()
    passed_customers["loyalty_tier"] = passed_customers["loyalty_tier"].apply(standardise_loyalty_tier)

    # Master key sets for foreign key validation
    valid_store_ids = set(passed_stores["store_id"].astype(str))
    valid_prod_ids = set(passed_products["product_id"].astype(str))
    valid_cust_ids = set(passed_customers["customer_id"].astype(str))

    # =========================================================================
    # 4. ORDERS VALIDATION
    # =========================================================================
    ord_df = orders_df.copy()
    ord_failed_reasons = {}

    # Uniqueness: order_id unique
    mask_dup_ord = ord_df.duplicated(subset=["order_id"], keep="first")
    for idx in ord_df[mask_dup_ord].index:
        ord_failed_reasons.setdefault(idx, []).append("uniqueness:duplicate_order_id")
    record_dq("orders_order_id_unique", "Uniqueness", "Critical", len(ord_df), mask_dup_ord.sum())

    # Completeness: customer_id not null
    mask_null_cust = ord_df["customer_id"].isna() | (ord_df["customer_id"].astype(str).str.strip() == "")
    for idx in ord_df[mask_null_cust].index:
        ord_failed_reasons.setdefault(idx, []).append("completeness:null_customer_id")
    record_dq("orders_customer_id_not_null", "Completeness", "Critical", len(ord_df), mask_null_cust.sum())

    # Completeness: order_ts not null
    mask_null_ts = ord_df["order_ts"].isna() | (ord_df["order_ts"].astype(str).str.strip() == "")
    for idx in ord_df[mask_null_ts].index:
        ord_failed_reasons.setdefault(idx, []).append("completeness:null_order_ts")
    record_dq("orders_order_ts_not_null", "Completeness", "Critical", len(ord_df), mask_null_ts.sum())

    # Completeness: DELIVERED orders must have delivery_minutes
    st_upper = ord_df["order_status"].fillna("").astype(str).str.strip().str.upper()
    dm_str = ord_df["delivery_minutes"].fillna("").astype(str).str.strip()
    mask_missing_dm = (st_upper == "DELIVERED") & (dm_str == "")
    for idx in ord_df[mask_missing_dm].index:
        ord_failed_reasons.setdefault(idx, []).append("completeness:missing_delivery_minutes")
    record_dq("orders_delivered_has_delivery_minutes", "Completeness", "Critical", len(ord_df), mask_missing_dm.sum())

    # Validity: order_status in allowed list
    mask_invalid_status = ~st_upper.isin(["DELIVERED", "CANCELLED", "RETURNED"])
    for idx in ord_df[mask_invalid_status].index:
        ord_failed_reasons.setdefault(idx, []).append(f"validity:invalid_order_status({ord_df.loc[idx, 'order_status']})")
    record_dq("orders_status_valid", "Validity", "Critical", len(ord_df), mask_invalid_status.sum())

    # Validity: payment_mode in allowed list
    pm_clean = ord_df["payment_mode"].fillna("").astype(str).str.strip().str.lower()
    mask_invalid_pm = ~pm_clean.isin(["upi", "card", "cash on delivery", "wallet", "cod"])
    for idx in ord_df[mask_invalid_pm].index:
        ord_failed_reasons.setdefault(idx, []).append(f"validity:invalid_payment_mode({ord_df.loc[idx, 'payment_mode']})")
    record_dq("orders_payment_mode_valid", "Validity", "Critical", len(ord_df), mask_invalid_pm.sum())

    # Validity: order_ts parses as timestamp
    def parse_ts(val):
        try:
            return pd.to_datetime(val)
        except Exception:
            return pd.NaT

    parsed_ts = ord_df["order_ts"].apply(parse_ts)
    mask_invalid_ts = parsed_ts.isna() & (~mask_null_ts)
    for idx in ord_df[mask_invalid_ts].index:
        ord_failed_reasons.setdefault(idx, []).append(f"validity:invalid_order_ts({ord_df.loc[idx, 'order_ts']})")
    record_dq("orders_order_ts_parseable", "Validity", "Critical", len(ord_df), mask_invalid_ts.sum())

    # Timeliness: order_ts not in future (> 2026-10-01)
    mask_future_ord = (parsed_ts > pd.Timestamp("2026-10-01 23:59:59"))
    for idx in ord_df[mask_future_ord].index:
        ord_failed_reasons.setdefault(idx, []).append(f"timeliness:future_order_ts({ord_df.loc[idx, 'order_ts']})")
    record_dq("orders_order_ts_not_future", "Timeliness", "Critical", len(ord_df), mask_future_ord.sum())

    # Accuracy: quantity > 0
    qty_num = pd.to_numeric(ord_df["quantity"], errors="coerce").fillna(0)
    mask_qty_le_zero = qty_num <= 0
    for idx in ord_df[mask_qty_le_zero].index:
        ord_failed_reasons.setdefault(idx, []).append(f"accuracy:quantity_le_zero({ord_df.loc[idx, 'quantity']})")
    record_dq("orders_quantity_positive", "Accuracy", "Critical", len(ord_df), mask_qty_le_zero.sum())

    # Accuracy / Validity: discount_pct in [0, 100]
    disc_num = pd.to_numeric(ord_df["discount_pct"], errors="coerce").fillna(0)
    mask_disc_invalid = (disc_num < 0) | (disc_num > 100)
    for idx in ord_df[mask_disc_invalid].index:
        ord_failed_reasons.setdefault(idx, []).append(f"validity:discount_pct_out_of_range({ord_df.loc[idx, 'discount_pct']})")
    record_dq("orders_discount_pct_range", "Validity", "Critical", len(ord_df), mask_disc_invalid.sum())

    # Accuracy: order_amount matches formula round(quantity * unit_price * (1 - discount_pct/100), 2)
    up_num = pd.to_numeric(ord_df["unit_price"], errors="coerce").fillna(0)
    amt_num = pd.to_numeric(ord_df["order_amount"], errors="coerce").fillna(0)
    expected_amt = (qty_num * up_num * (1.0 - disc_num / 100.0)).round(2)
    mask_amt_mismatch = (amt_num - expected_amt).abs() > 0.02
    for idx in ord_df[mask_amt_mismatch].index:
        ord_failed_reasons.setdefault(idx, []).append(
            f"accuracy:order_amount_mismatch({ord_df.loc[idx, 'order_amount']}!=exp_{expected_amt.loc[idx]})"
        )
    record_dq("orders_amount_formula_match", "Accuracy", "Critical", len(ord_df), mask_amt_mismatch.sum())

    # Accuracy: delivery_minutes < 120 for DELIVERED orders
    dm_num = pd.to_numeric(ord_df["delivery_minutes"], errors="coerce")
    mask_dm_bounds = (st_upper == "DELIVERED") & (dm_num.notna()) & ((dm_num <= 0) | (dm_num >= 120))
    for idx in ord_df[mask_dm_bounds].index:
        ord_failed_reasons.setdefault(idx, []).append(f"accuracy:delivery_minutes_out_of_range({ord_df.loc[idx, 'delivery_minutes']})")
    record_dq("orders_delivery_minutes_range", "Accuracy", "Critical", len(ord_df), mask_dm_bounds.sum())

    # Consistency: Foreign Keys against masters
    mask_bad_cust = ord_df["customer_id"].notna() & (~ord_df["customer_id"].astype(str).str.strip().isin(valid_cust_ids)) & (~mask_null_cust)
    for idx in ord_df[mask_bad_cust].index:
        ord_failed_reasons.setdefault(idx, []).append(f"consistency:unknown_customer_id({ord_df.loc[idx, 'customer_id']})")
    record_dq("orders_customer_id_fk", "Consistency", "Critical", len(ord_df), mask_bad_cust.sum())

    mask_bad_prod = ~ord_df["product_id"].astype(str).str.strip().isin(valid_prod_ids)
    for idx in ord_df[mask_bad_prod].index:
        ord_failed_reasons.setdefault(idx, []).append(f"consistency:unknown_product_id({ord_df.loc[idx, 'product_id']})")
    record_dq("orders_product_id_fk", "Consistency", "Critical", len(ord_df), mask_bad_prod.sum())

    mask_bad_store = ~ord_df["store_id"].astype(str).str.strip().isin(valid_store_ids)
    for idx in ord_df[mask_bad_store].index:
        ord_failed_reasons.setdefault(idx, []).append(f"consistency:unknown_store_id({ord_df.loc[idx, 'store_id']})")
    record_dq("orders_store_id_fk", "Consistency", "Critical", len(ord_df), mask_bad_store.sum())

    # Warning: non-delivered orders with delivery_minutes
    mask_non_deliv_dm = (st_upper != "DELIVERED") & (dm_str != "")
    record_dq("orders_non_delivered_no_delivery_minutes", "Consistency", "Warning", len(ord_df), mask_non_deliv_dm.sum())

    ord_crit_idx = list(ord_failed_reasons.keys())
    failed_orders = ord_df.loc[ord_crit_idx].copy()
    failed_orders["failure_reason"] = ["; ".join(ord_failed_reasons[i]) for i in ord_crit_idx]
    passed_orders = ord_df.drop(index=ord_crit_idx).copy()

    # Standardise passed orders
    passed_orders["order_status"] = passed_orders["order_status"].apply(standardise_order_status)
    passed_orders["payment_mode"] = passed_orders["payment_mode"].apply(standardise_payment_mode)

    # Volume Anomaly Check (Warn if order count dropped by > 50% vs expected ~2400)
    vol_status = "PASSED" if len(passed_orders) > 1000 else "WARNING"
    dq_records.append({
        "run_id": run_id,
        "dag_id": dag_id,
        "check_name": "orders_volume_anomaly_check",
        "dimension": "Completeness",
        "severity": "Warning",
        "rows_checked": len(ord_df),
        "rows_failed": 0 if vol_status == "PASSED" else len(ord_df),
        "status": vol_status,
        "checked_at": checked_at,
    })

    # =========================================================================
    # 5. PAYMENTS VALIDATION
    # =========================================================================
    pay_df = payments_df.copy()
    pay_failed_reasons = {}

    # Uniqueness: payment_id unique
    mask_dup_pay = pay_df.duplicated(subset=["payment_id"], keep="first")
    for idx in pay_df[mask_dup_pay].index:
        pay_failed_reasons.setdefault(idx, []).append("uniqueness:duplicate_payment_id")
    record_dq("payments_payment_id_unique", "Uniqueness", "Critical", len(pay_df), mask_dup_pay.sum())

    # Accuracy: amount_paid > 0
    pamt_num = pd.to_numeric(pay_df["amount_paid"], errors="coerce").fillna(0)
    mask_neg_pamt = pamt_num <= 0
    for idx in pay_df[mask_neg_pamt].index:
        pay_failed_reasons.setdefault(idx, []).append(f"accuracy:amount_paid_le_zero({pay_df.loc[idx, 'amount_paid']})")
    record_dq("payments_amount_paid_positive", "Accuracy", "Critical", len(pay_df), mask_neg_pamt.sum())

    # Validity: payment_mode
    ppm_clean = pay_df["payment_mode"].fillna("").astype(str).str.strip().str.lower()
    mask_invalid_ppm = ~ppm_clean.isin(["upi", "card", "cash on delivery", "wallet", "cod"])
    for idx in pay_df[mask_invalid_ppm].index:
        pay_failed_reasons.setdefault(idx, []).append(f"validity:invalid_payment_mode({pay_df.loc[idx, 'payment_mode']})")
    record_dq("payments_payment_mode_valid", "Validity", "Critical", len(pay_df), mask_invalid_ppm.sum())

    # Consistency: order_id exists in orders
    all_order_ids = set(ord_df["order_id"].astype(str))
    mask_orphan_pay = ~pay_df["order_id"].astype(str).isin(all_order_ids)
    for idx in pay_df[mask_orphan_pay].index:
        pay_failed_reasons.setdefault(idx, []).append(f"consistency:orphan_order_id({pay_df.loc[idx, 'order_id']})")
    record_dq("payments_order_id_fk", "Consistency", "Critical", len(pay_df), mask_orphan_pay.sum())

    # Timeliness: payment_ts not in future (> 2026-10-01)
    pts_str = pay_df["payment_ts"].fillna("").astype(str).str.strip()
    mask_future_pay = pts_str > "2026-10-01 23:59:59"
    for idx in pay_df[mask_future_pay].index:
        pay_failed_reasons.setdefault(idx, []).append(f"timeliness:future_payment_ts({pts_str.loc[idx]})")
    record_dq("payments_payment_ts_not_future", "Timeliness", "Critical", len(pay_df), mask_future_pay.sum())

    pay_crit_idx = list(pay_failed_reasons.keys())
    failed_payments = pay_df.loc[pay_crit_idx].copy()
    failed_payments["failure_reason"] = ["; ".join(pay_failed_reasons[i]) for i in pay_crit_idx]
    passed_payments = pay_df.drop(index=pay_crit_idx).copy()
    passed_payments["payment_mode"] = passed_payments["payment_mode"].apply(standardise_payment_mode)

    return {
        "passed_orders": passed_orders,
        "failed_orders": failed_orders,
        "passed_payments": passed_payments,
        "failed_payments": failed_payments,
        "passed_customers": passed_customers,
        "failed_customers": failed_customers,
        "passed_products": passed_products,
        "failed_products": failed_products,
        "passed_stores": passed_stores,
        "failed_stores": failed_stores,
        "dq_results": dq_records,
        "checked_at": checked_at,
    }
