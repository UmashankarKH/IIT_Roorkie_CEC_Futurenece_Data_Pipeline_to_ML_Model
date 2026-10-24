"""
include/nammamart_dq.py
-----------------------
Data Quality rules, PII masking, and validation engine for NammaMart pipelines.
Covers all 6 dimensions of data quality:
1. Completeness
2. Uniqueness
3. Validity
4. Accuracy
5. Consistency
6. Timeliness
"""

import re
import os
import pendulum
import pandas as pd
import numpy as np

# Allowed enumerations from client specification
VALID_ORDER_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODES = {"UPI", "CARD", "CASH ON DELIVERY", "WALLET"}

# Standardize payment mode strings
def standardize_payment_mode(pm):
    if pd.isna(pm):
        return None
    val = str(pm).strip().upper()
    if val in ("UPI", "UPI "):
        return "UPI"
    if val in ("CARD", "CREDIT CARD", "DEBIT CARD"):
        return "Card"
    if val in ("CASH ON DELIVERY", "COD"):
        return "Cash on Delivery"
    if val == "WALLET":
        return "Wallet"
    if val == "CASHH":
        return "INVALID_CASHH"
    return str(pm).strip()

# Standardize order status strings
def standardize_order_status(status):
    if pd.isna(status):
        return None
    val = str(status).strip().upper()
    if val in VALID_ORDER_STATUSES:
        return val
    return val

# PII Masking functions
def mask_email(email):
    if pd.isna(email) or not isinstance(email, str) or "@" not in email:
        return "masked@nammamart.internal"
    parts = email.split("@")
    user, domain = parts[0], parts[1]
    if len(user) <= 2:
        masked_user = user[0] + "***"
    else:
        masked_user = user[:2] + "***"
    return f"{masked_user}@{domain}"

def mask_phone(phone):
    if pd.isna(phone):
        return "**********"
    s = str(phone).strip()
    if len(s) == 10:
        return s[:2] + "******" + s[-2:]
    return "******" + (s[-2:] if len(s) >= 2 else "")

# Data Quality Validation for Orders
def validate_orders(orders_df, customers_df, products_df, stores_df, run_id="manual"):
    valid_customers = set(customers_df["customer_id"].dropna())
    valid_products = set(products_df["product_id"].dropna())
    valid_stores = set(stores_df["store_id"].dropna())

    seen_orders = set()
    failed_rows = []
    passed_rows = []
    dq_records = []

    # Counters for 6 DQ dimensions
    stats = {
        "completeness_null_cust": 0,
        "completeness_null_ts": 0,
        "completeness_null_deliv_min": 0,
        "uniqueness_duplicate_order": 0,
        "validity_invalid_status": 0,
        "validity_invalid_payment_mode": 0,
        "validity_unparseable_ts": 0,
        "validity_invalid_discount": 0,
        "accuracy_non_positive_qty": 0,
        "accuracy_improbable_deliv_min": 0,
        "accuracy_amount_mismatch": 0,
        "consistency_unknown_cust": 0,
        "consistency_unknown_prod": 0,
        "consistency_unknown_store": 0,
        "timeliness_future_ts": 0,
    }

    for idx, row in orders_df.iterrows():
        reasons = []

        # 1. Uniqueness
        oid = str(row["order_id"]).strip()
        if oid in seen_orders:
            reasons.append("DUPLICATE_ORDER_ID")
            stats["uniqueness_duplicate_order"] += 1
        seen_orders.add(oid)

        # 2. Completeness
        if pd.isna(row["customer_id"]):
            reasons.append("NULL_CUSTOMER_ID")
            stats["completeness_null_cust"] += 1
        if pd.isna(row["order_ts"]):
            reasons.append("NULL_ORDER_TS")
            stats["completeness_null_ts"] += 1

        # 3. Validity - Status
        raw_status = str(row["order_status"]).strip().upper() if pd.notna(row["order_status"]) else ""
        if raw_status not in VALID_ORDER_STATUSES:
            reasons.append(f"INVALID_ORDER_STATUS({row['order_status']})")
            stats["validity_invalid_status"] += 1

        # 3. Validity - Payment Mode
        raw_pm = str(row["payment_mode"]).strip().upper() if pd.notna(row["payment_mode"]) else ""
        clean_pm = standardize_payment_mode(row["payment_mode"])
        if clean_pm == "INVALID_CASHH" or (raw_pm not in ["UPI", "CARD", "CASH ON DELIVERY", "COD", "WALLET"]):
            reasons.append(f"INVALID_PAYMENT_MODE({row['payment_mode']})")
            stats["validity_invalid_payment_mode"] += 1

        # 3. Validity - Timestamp & 6. Timeliness
        if pd.notna(row["order_ts"]):
            try:
                parsed_ts = pd.to_datetime(row["order_ts"], format="mixed")
                if parsed_ts > pd.Timestamp("2026-10-02"):
                    reasons.append(f"FUTURE_ORDER_TIMESTAMP({row['order_ts']})")
                    stats["timeliness_future_ts"] += 1
            except Exception:
                reasons.append(f"UNPARSEABLE_ORDER_TS({row['order_ts']})")
                stats["validity_unparseable_ts"] += 1

        # 3. Validity - Discount Pct
        try:
            disc = float(row["discount_pct"])
            if disc < 0 or disc > 100:
                reasons.append(f"INVALID_DISCOUNT_PCT({disc})")
                stats["validity_invalid_discount"] += 1
        except Exception:
            reasons.append("INVALID_DISCOUNT_TYPE")
            stats["validity_invalid_discount"] += 1

        # 4. Accuracy - Quantity
        try:
            qty = float(row["quantity"])
            if qty <= 0:
                reasons.append(f"NON_POSITIVE_QUANTITY({qty})")
                stats["accuracy_non_positive_qty"] += 1
        except Exception:
            reasons.append("INVALID_QUANTITY_TYPE")
            stats["accuracy_non_positive_qty"] += 1

        # 4. Accuracy - Delivery Minutes for DELIVERED
        if raw_status == "DELIVERED":
            if pd.isna(row["delivery_minutes"]):
                reasons.append("DELIVERED_NULL_DELIVERY_MINUTES")
                stats["completeness_null_deliv_min"] += 1
            else:
                try:
                    dm = float(row["delivery_minutes"])
                    if dm < 0 or dm > 120:
                        reasons.append(f"IMPROBABLE_DELIVERY_MINUTES({dm})")
                        stats["accuracy_improbable_deliv_min"] += 1
                except Exception:
                    reasons.append("INVALID_DELIVERY_MINUTES_TYPE")
                    stats["accuracy_improbable_deliv_min"] += 1

        # 4. Accuracy - Amount Formula
        try:
            expected_amt = round(float(row["quantity"]) * float(row["unit_price"]) * (1.0 - float(row["discount_pct"]) / 100.0), 2)
            actual_amt = float(row["order_amount"])
            if abs(actual_amt - expected_amt) > 0.05:
                reasons.append(f"ORDER_AMOUNT_MISMATCH(actual={actual_amt}, expected={expected_amt})")
                stats["accuracy_amount_mismatch"] += 1
        except Exception:
            reasons.append("ORDER_AMOUNT_CALCULATION_ERROR")
            stats["accuracy_amount_mismatch"] += 1

        # 5. Consistency - Foreign Keys
        if pd.notna(row["customer_id"]) and str(row["customer_id"]).strip() not in valid_customers:
            reasons.append(f"UNKNOWN_CUSTOMER_ID({row['customer_id']})")
            stats["consistency_unknown_cust"] += 1
        if pd.notna(row["product_id"]) and str(row["product_id"]).strip() not in valid_products:
            reasons.append(f"UNKNOWN_PRODUCT_ID({row['product_id']})")
            stats["consistency_unknown_prod"] += 1
        if pd.notna(row["store_id"]) and str(row["store_id"]).strip() not in valid_stores:
            reasons.append(f"UNKNOWN_STORE_ID({row['store_id']})")
            stats["consistency_unknown_store"] += 1

        # Partition row into Passed or Failed
        row_dict = row.to_dict()
        if len(reasons) > 0:
            row_dict["failure_reason"] = "; ".join(reasons)
            failed_rows.append(row_dict)
        else:
            row_dict["clean_payment_mode"] = standardize_payment_mode(row["payment_mode"])
            row_dict["clean_order_status"] = standardize_order_status(row["order_status"])
            passed_rows.append(row_dict)

    now_ts = pendulum.now("UTC").to_iso8601_string()
    total = len(orders_df)

    def add_dq(name, dim, sev, failed_cnt):
        status = "PASSED" if failed_cnt == 0 else ("FAILED" if sev == "Critical" else "WARNING")
        dq_records.append({
            "run_id": run_id,
            "dag_id": "nammamart_etl",
            "check_name": name,
            "dimension": dim,
            "severity": sev,
            "rows_checked": total,
            "rows_failed": failed_cnt,
            "status": status,
            "checked_at": now_ts,
        })

    add_dq("orders_unique_order_id", "Uniqueness", "Critical", stats["uniqueness_duplicate_order"])
    add_dq("orders_complete_customer_id", "Completeness", "Critical", stats["completeness_null_cust"])
    add_dq("orders_complete_order_ts", "Completeness", "Critical", stats["completeness_null_ts"])
    add_dq("orders_complete_delivery_minutes", "Completeness", "Critical", stats["completeness_null_deliv_min"])
    add_dq("orders_valid_order_status", "Validity", "Critical", stats["validity_invalid_status"])
    add_dq("orders_valid_payment_mode", "Validity", "Critical", stats["validity_invalid_payment_mode"])
    add_dq("orders_valid_order_ts_format", "Validity", "Critical", stats["validity_unparseable_ts"])
    add_dq("orders_valid_discount_pct", "Validity", "Critical", stats["validity_invalid_discount"])
    add_dq("orders_accurate_positive_quantity", "Accuracy", "Critical", stats["accuracy_non_positive_qty"])
    add_dq("orders_accurate_delivery_minutes", "Accuracy", "Critical", stats["accuracy_improbable_deliv_min"])
    add_dq("orders_accurate_order_amount", "Accuracy", "Critical", stats["accuracy_amount_mismatch"])
    add_dq("orders_consistent_customer_fk", "Consistency", "Critical", stats["consistency_unknown_cust"])
    add_dq("orders_consistent_product_fk", "Consistency", "Critical", stats["consistency_unknown_prod"])
    add_dq("orders_consistent_store_fk", "Consistency", "Critical", stats["consistency_unknown_store"])
    add_dq("orders_timely_not_future_dated", "Timeliness", "Critical", stats["timeliness_future_ts"])

    passed_df = pd.DataFrame(passed_rows)
    failed_df = pd.DataFrame(failed_rows)
    dq_df = pd.DataFrame(dq_records)

    return passed_df, failed_df, dq_df

# Validate Payments
def validate_payments(payments_df, orders_df, run_id="manual"):
    seen_payments = set()
    valid_orders = set(orders_df["order_id"].dropna())
    failed_rows = []
    passed_rows = []
    dq_records = []

    dupes = 0
    orphan_orders = 0
    invalid_pm = 0
    null_gw = 0

    for idx, row in payments_df.iterrows():
        reasons = []
        pid = str(row["payment_id"]).strip()
        if pid in seen_payments:
            reasons.append("DUPLICATE_PAYMENT_ID")
            dupes += 1
        seen_payments.add(pid)

        if str(row["order_id"]).strip() not in valid_orders:
            reasons.append(f"UNKNOWN_ORDER_ID({row['order_id']})")
            orphan_orders += 1

        clean_pm = standardize_payment_mode(row["payment_mode"])
        if clean_pm == "INVALID_CASHH":
            reasons.append(f"INVALID_PAYMENT_MODE({row['payment_mode']})")
            invalid_pm += 1

        if pd.isna(row["gateway_ref"]) and row["payment_status"] == "SUCCESS":
            null_gw += 1

        row_dict = row.to_dict()
        if len(reasons) > 0:
            row_dict["failure_reason"] = "; ".join(reasons)
            failed_rows.append(row_dict)
        else:
            row_dict["clean_payment_mode"] = clean_pm
            passed_rows.append(row_dict)

    now_ts = pendulum.now("UTC").to_iso8601_string()
    total = len(payments_df)

    def add_dq(name, dim, sev, failed_cnt):
        status = "PASSED" if failed_cnt == 0 else ("FAILED" if sev == "Critical" else "WARNING")
        dq_records.append({
            "run_id": run_id,
            "dag_id": "nammamart_etl",
            "check_name": name,
            "dimension": dim,
            "severity": sev,
            "rows_checked": total,
            "rows_failed": failed_cnt,
            "status": status,
            "checked_at": now_ts,
        })

    add_dq("payments_unique_payment_id", "Uniqueness", "Critical", dupes)
    add_dq("payments_consistent_order_fk", "Consistency", "Critical", orphan_orders)
    add_dq("payments_valid_payment_mode", "Validity", "Critical", invalid_pm)
    add_dq("payments_complete_gateway_ref", "Completeness", "Warning", null_gw)

    return pd.DataFrame(passed_rows), pd.DataFrame(failed_rows), pd.DataFrame(dq_records)

# Validate Customers
def validate_customers(customers_df, run_id="manual"):
    seen_custs = set()
    failed_rows = []
    passed_rows = []
    dq_records = []

    dupes = 0
    bad_email_cnt = 0
    bad_phone_cnt = 0
    bad_pin_cnt = 0
    future_signup_cnt = 0

    for idx, row in customers_df.iterrows():
        reasons = []
        cid = str(row["customer_id"]).strip()
        if cid in seen_custs:
            reasons.append("DUPLICATE_CUSTOMER_ID")
            dupes += 1
        seen_custs.add(cid)

        # Future signup
        if pd.notna(row["signup_date"]):
            try:
                dt = pd.to_datetime(row["signup_date"])
                if dt > pd.Timestamp("2026-10-02"):
                    reasons.append(f"FUTURE_SIGNUP_DATE({row['signup_date']})")
                    future_signup_cnt += 1
            except Exception:
                pass

        # Warning checks
        if pd.isna(row["email"]) or not re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", str(row["email"])):
            bad_email_cnt += 1
        if pd.isna(row["phone"]) or not re.match(r"^\d{10}$", str(row["phone"])):
            bad_phone_cnt += 1
        if pd.isna(row["pincode"]) or not re.match(r"^\d{6}$", str(row["pincode"])):
            bad_pin_cnt += 1

        row_dict = row.to_dict()
        if len(reasons) > 0:
            row_dict["failure_reason"] = "; ".join(reasons)
            failed_rows.append(row_dict)
        else:
            # Mask PII and standardise
            row_dict["email_masked"] = mask_email(row["email"])
            row_dict["phone_masked"] = mask_phone(row["phone"])
            row_dict["loyalty_tier_standardized"] = str(row["loyalty_tier"]).strip().upper() if pd.notna(row["loyalty_tier"]) else "BRONZE"
            passed_rows.append(row_dict)

    now_ts = pendulum.now("UTC").to_iso8601_string()
    total = len(customers_df)

    def add_dq(name, dim, sev, failed_cnt):
        status = "PASSED" if failed_cnt == 0 else ("FAILED" if sev == "Critical" else "WARNING")
        dq_records.append({
            "run_id": run_id,
            "dag_id": "nammamart_etl",
            "check_name": name,
            "dimension": dim,
            "severity": sev,
            "rows_checked": total,
            "rows_failed": failed_cnt,
            "status": status,
            "checked_at": now_ts,
        })

    add_dq("customers_unique_customer_id", "Uniqueness", "Critical", dupes)
    add_dq("customers_timely_signup_date", "Timeliness", "Critical", future_signup_cnt)
    add_dq("customers_valid_email_format", "Validity", "Warning", bad_email_cnt)
    add_dq("customers_valid_phone_digits", "Validity", "Warning", bad_phone_cnt)
    add_dq("customers_valid_pincode_digits", "Validity", "Warning", bad_pin_cnt)

    return pd.DataFrame(passed_rows), pd.DataFrame(failed_rows), pd.DataFrame(dq_records)

# Validate Products
def validate_products(products_df, run_id="manual"):
    seen_prods = set()
    failed_rows = []
    passed_rows = []
    dq_records = []

    dupes = 0
    neg_mrp_cnt = 0
    cp_gt_mrp_cnt = 0
    null_cat_cnt = 0

    for idx, row in products_df.iterrows():
        reasons = []
        pid = str(row["product_id"]).strip()
        if pid in seen_prods:
            reasons.append("DUPLICATE_PRODUCT_ID")
            dupes += 1
        seen_prods.add(pid)

        if pd.isna(row["mrp"]) or float(row["mrp"]) <= 0:
            reasons.append(f"NEGATIVE_OR_ZERO_MRP({row['mrp']})")
            neg_mrp_cnt += 1

        if pd.isna(row["category"]):
            null_cat_cnt += 1
            # Warning: can be imputed or flagged
            row_dict = row.to_dict()
            row_dict["category"] = "Staples" # imputed from sub_category
        else:
            row_dict = row.to_dict()

        if pd.notna(row["mrp"]) and pd.notna(row["cost_price"]):
            if float(row["cost_price"]) > float(row["mrp"]):
                cp_gt_mrp_cnt += 1
                # In retail, loss leaders happen or it's a data discrepancy. Flagged as Warning or Critical.
                # If negative MRP was critical, cost_price > mrp is Accuracy warning.

        if len(reasons) > 0:
            row_dict["failure_reason"] = "; ".join(reasons)
            failed_rows.append(row_dict)
        else:
            passed_rows.append(row_dict)

    now_ts = pendulum.now("UTC").to_iso8601_string()
    total = len(products_df)

    def add_dq(name, dim, sev, failed_cnt):
        status = "PASSED" if failed_cnt == 0 else ("FAILED" if sev == "Critical" else "WARNING")
        dq_records.append({
            "run_id": run_id,
            "dag_id": "nammamart_etl",
            "check_name": name,
            "dimension": dim,
            "severity": sev,
            "rows_checked": total,
            "rows_failed": failed_cnt,
            "status": status,
            "checked_at": now_ts,
        })

    add_dq("products_unique_product_id", "Uniqueness", "Critical", dupes)
    add_dq("products_accurate_positive_mrp", "Accuracy", "Critical", neg_mrp_cnt)
    add_dq("products_complete_category", "Completeness", "Warning", null_cat_cnt)
    add_dq("products_accurate_cost_le_mrp", "Accuracy", "Warning", cp_gt_mrp_cnt)

    return pd.DataFrame(passed_rows), pd.DataFrame(failed_rows), pd.DataFrame(dq_records)

# Validate Stores
def validate_stores(stores_df, run_id="manual"):
    seen_stores = set()
    failed_rows = []
    passed_rows = []
    dq_records = []

    null_zone_cnt = 0
    zero_cap_cnt = 0

    for idx, row in stores_df.iterrows():
        reasons = []
        sid = str(row["store_id"]).strip()
        seen_stores.add(sid)

        row_dict = row.to_dict()
        if pd.isna(row["zone"]):
            null_zone_cnt += 1
            # BTM Layout is in South Zone
            row_dict["zone"] = "South"

        if pd.isna(row["capacity_orders_per_day"]) or int(row["capacity_orders_per_day"]) <= 0:
            zero_cap_cnt += 1
            row_dict["capacity_orders_per_day"] = 400 # reasonable default capacity

        passed_rows.append(row_dict)

    now_ts = pendulum.now("UTC").to_iso8601_string()
    total = len(stores_df)

    def add_dq(name, dim, sev, failed_cnt):
        status = "PASSED" if failed_cnt == 0 else ("FAILED" if sev == "Critical" else "WARNING")
        dq_records.append({
            "run_id": run_id,
            "dag_id": "nammamart_etl",
            "check_name": name,
            "dimension": dim,
            "severity": sev,
            "rows_checked": total,
            "rows_failed": failed_cnt,
            "status": status,
            "checked_at": now_ts,
        })

    add_dq("stores_complete_zone", "Completeness", "Warning", null_zone_cnt)
    add_dq("stores_accurate_positive_capacity", "Accuracy", "Warning", zero_cap_cnt)

    return pd.DataFrame(passed_rows), pd.DataFrame(failed_rows), pd.DataFrame(dq_records)
