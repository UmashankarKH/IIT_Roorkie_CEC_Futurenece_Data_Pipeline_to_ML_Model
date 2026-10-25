"""Profile the NammaMart assignment data and create the Part 0 outputs.

Run from the repository root with:
    python include/profile_nammamart.py
"""

import os
import shutil
from pathlib import Path

import pandas as pd


# Resolve all input and output locations from the repository root so that the
# profiler can be run from any working directory.
ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "assignment" / "nammamart_dataset"
DATA_DIR = ROOT / "include" / "data" / "nammamart"
OUTPUT_DIR = ROOT / "assignment" / "profile"

# These are the files required by Part 0, including the next-day incremental
# feed used to identify order corrections and new orders.
FILES = [
    "orders.csv",
    "payments.csv",
    "customers.csv",
    "products.csv",
    "stores.csv",
    "orders_2026-10-01_increment.csv",
]

# Metadata drives the generic profiling loop and avoids hard-coding the same
# profiling operation separately for every source file.
KEYS = {
    "orders.csv": "order_id",
    "payments.csv": "payment_id",
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "stores.csv": "store_id",
    "orders_2026-10-01_increment.csv": "order_id",
}

# Columns expected to contain numbers, dates, or controlled categorical values.
# The source files are read as strings first so malformed values are preserved
# and can be reported instead of being silently converted to nulls.
NUMERIC = {
    "orders.csv": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments.csv": ["amount_paid"],
    "customers.csv": ["pincode"],
    "products.csv": ["mrp", "cost_price"],
    "stores.csv": ["pincode", "capacity_orders_per_day"],
    "orders_2026-10-01_increment.csv": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
}
DATES = {
    "orders.csv": ["order_ts"],
    "payments.csv": ["payment_ts"],
    "customers.csv": ["signup_date"],
    "products.csv": ["launch_date"],
    "stores.csv": ["open_date"],
    "orders_2026-10-01_increment.csv": ["order_ts"],
}
CATEGORICAL = {
    "orders.csv": ["order_status", "payment_mode", "channel"],
    "payments.csv": ["payment_mode", "payment_status"],
    "customers.csv": ["city", "area", "loyalty_tier", "is_active"],
    "products.csv": ["category", "sub_category", "brand", "unit", "is_perishable"],
    "stores.csv": ["zone", "area"],
    "orders_2026-10-01_increment.csv": ["order_status", "payment_mode", "channel"],
}
ALLOWED_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}


def profiling_as_of_date() -> pd.Timestamp:
    """Return the local date used to identify future-dated source records.

    Set NAMMAMART_PROFILE_AS_OF_DATE to reproduce a historical profiling run;
    otherwise use today's date in the client's Asia/Kolkata timezone.
    """
    configured_date = os.getenv("NAMMAMART_PROFILE_AS_OF_DATE")
    if configured_date:
        return pd.Timestamp(configured_date).normalize()
    return pd.Timestamp.now(tz="Asia/Kolkata").normalize().tz_localize(None)


def blank(value: object) -> bool:
    """Return True when a source value is missing or contains only whitespace."""
    return value is None or str(value).strip() == ""


def add_issue(issues: list[dict[str, object]], file: str, key: object, column: str,
              problem: str, dimension: str, rule: str, severity: str) -> None:
    """Append one standardized row to the Data Issues Log."""
    issues.append({
        "file": file, "row_key": str(key), "column": column, "problem": problem,
        "quality_dimension": dimension, "proposed_rule": rule, "severity": severity,
    })


def main() -> None:
    """Copy source data, profile it, validate business rules, and write outputs."""
    as_of_date = profiling_as_of_date()
    # Create the required directories and copy the untouched assignment exports
    # into the location expected by the later Airflow pipelines.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for filename in FILES:
        shutil.copy2(SOURCE_DIR / filename, DATA_DIR / filename)

    # Preserve every source value as text, including malformed values and blank
    # fields, because profiling must observe the data exactly as received.
    frames = {filename: pd.read_csv(DATA_DIR / filename, dtype=str, keep_default_na=False) for filename in FILES}

    # Build a long-form profile containing counts, nulls, duplicates, ranges,
    # and distinct categorical values for each file and column.
    profile: list[dict[str, object]] = []
    for filename, frame in frames.items():
        profile.append({"File": filename, "Section": "summary", "Column": "__file__", "Metric": "row_count", "Value": len(frame)})
        for column in frame.columns:
            profile.append({"File": filename, "Section": "nulls", "Column": column, "Metric": "null_count", "Value": int(frame[column].map(blank).sum())})
        key = KEYS[filename]
        counts = frame[key].value_counts()
        duplicate_groups = counts[counts > 1]
        profile.extend([
            {"File": filename, "Section": "duplicates", "Column": key, "Metric": "duplicate_key_groups", "Value": len(duplicate_groups)},
            {"File": filename, "Section": "duplicates", "Column": key, "Metric": "duplicate_rows_beyond_first", "Value": int((duplicate_groups - 1).sum())},
        ])
        for column in NUMERIC.get(filename, []):
            values = pd.to_numeric(frame[column], errors="coerce").dropna()
            if not values.empty:
                profile.extend([
                    {"File": filename, "Section": "numeric_range", "Column": column, "Metric": "min", "Value": values.min()},
                    {"File": filename, "Section": "numeric_range", "Column": column, "Metric": "max", "Value": values.max()},
                ])
        for column in DATES.get(filename, []):
            values = pd.to_datetime(frame[column], errors="coerce").dropna()
            if not values.empty:
                profile.extend([
                    {"File": filename, "Section": "date_range", "Column": column, "Metric": "min", "Value": values.min().isoformat(sep=" ")},
                    {"File": filename, "Section": "date_range", "Column": column, "Metric": "max", "Value": values.max().isoformat(sep=" ")},
                ])
        for column in CATEGORICAL.get(filename, []):
            values = sorted(value for value in frame[column].unique() if not blank(value))
            profile.append({"File": filename, "Section": "distinct_values", "Column": column, "Metric": "values", "Value": " | ".join(values)})

    # Save the machine-readable profile for inspection and reuse in the
    # submission documentation.
    pd.DataFrame(profile).to_csv(OUTPUT_DIR / "profile_summary.csv", index=False)

    # Prepare reference-key sets for foreign-key validation across source files.
    issues: list[dict[str, object]] = []
    orders = frames["orders.csv"]
    payments = frames["payments.csv"]
    customers = frames["customers.csv"]
    products = frames["products.csv"]
    stores = frames["stores.csv"]
    order_ids = set(orders["order_id"])
    customer_ids = set(customers["customer_id"])
    product_ids = set(products["product_id"])
    store_ids = set(stores["store_id"])

    # Check primary-key completeness and uniqueness for every source file.
    for filename, frame in frames.items():
        key = KEYS[filename]
        for value, count in frame[key].value_counts().items():
            if count > 1:
                add_issue(issues, filename, value, key, f"Duplicate key appears {count} times", "Uniqueness", "Primary key must be unique", "Critical")
        for _, row in frame.iterrows():
            if blank(row[key]):
                add_issue(issues, filename, "<blank>", key, "Required key is blank", "Completeness", "Primary key must be present", "Critical")

    # Validate order fields, relationships, allowed values, amount calculations,
    # timestamps, and delivery-time rules from the assignment brief.
    for _, row in orders.iterrows():
        key = row.order_id
        for column, valid_keys, rule in [("customer_id", customer_ids, "Order customer_id must exist in customers"), ("product_id", product_ids, "Order product_id must exist in products"), ("store_id", store_ids, "Order store_id must exist in stores")]:
            if blank(row[column]):
                add_issue(issues, "orders.csv", key, column, f"{column} is blank", "Completeness", rule, "Critical")
            elif row[column] not in valid_keys:
                add_issue(issues, "orders.csv", key, column, f"{column} does not exist", "Consistency", rule, "Critical")
        status = str(row.order_status).strip().upper()
        mode = str(row.payment_mode).strip()
        if status not in ALLOWED_STATUS:
            add_issue(issues, "orders.csv", key, "order_status", f"Invalid status: {row.order_status}", "Validity", "Status must be DELIVERED, CANCELLED or RETURNED", "Critical")
        if mode not in ALLOWED_MODES:
            add_issue(issues, "orders.csv", key, "payment_mode", f"Invalid payment mode: {row.payment_mode}", "Validity", "Payment mode must be one of the allowed values", "Critical")
        quantity, unit, discount, amount = [pd.to_numeric(row[column], errors="coerce") for column in ["quantity", "unit_price", "discount_pct", "order_amount"]]
        if pd.isna(quantity) or quantity <= 0:
            add_issue(issues, "orders.csv", key, "quantity", "Quantity is missing or not greater than zero", "Accuracy", "quantity must be > 0", "Critical")
        if pd.notna(discount) and not 0 <= discount <= 100:
            add_issue(issues, "orders.csv", key, "discount_pct", "Discount is outside 0-100", "Validity", "discount_pct must be between 0 and 100", "Critical")
        if all(pd.notna(value) for value in [quantity, unit, discount, amount]):
            expected = round(quantity * unit * (1 - discount / 100), 2)
            if abs(amount - expected) > 0.01:
                add_issue(issues, "orders.csv", key, "order_amount", f"Amount {amount} does not match calculated amount {expected}", "Accuracy", "order_amount = quantity * unit_price * (1 - discount_pct/100), rounded to 2 decimals", "Critical")
        timestamp = pd.to_datetime(row.order_ts, errors="coerce")
        if pd.isna(timestamp):
            add_issue(issues, "orders.csv", key, "order_ts", "Timestamp cannot be parsed", "Validity", "order_ts must be a valid timestamp", "Critical")
        elif timestamp.normalize() > as_of_date:
            add_issue(issues, "orders.csv", key, "order_ts", "Future-dated order timestamp", "Timeliness", "Order timestamp must not be future dated", "Critical")
        delivery = pd.to_numeric(row.delivery_minutes, errors="coerce")
        if status == "DELIVERED" and pd.isna(delivery):
            add_issue(issues, "orders.csv", key, "delivery_minutes", "Delivered order has no delivery time", "Completeness", "DELIVERED orders require delivery_minutes", "Critical")
        if pd.notna(delivery) and delivery >= 120:
            add_issue(issues, "orders.csv", key, "delivery_minutes", f"Implausible delivery time: {delivery} minutes", "Accuracy", "Delivery time must be below 120 minutes", "Critical")
        elif status == "DELIVERED" and pd.notna(delivery) and delivery > 35:
            add_issue(issues, "orders.csv", key, "delivery_minutes", f"Delivery exceeds normal 8-35 minute range: {delivery}", "Accuracy", "Log delivery times outside normal range as a warning", "Warning")

    # Validate payment modes, order references, timestamps, and gateway amount
    # reconciliation for successful payments.
    for _, row in payments.iterrows():
        key = row.payment_id
        mode = str(row.payment_mode).strip()
        if mode not in ALLOWED_MODES:
            add_issue(issues, "payments.csv", key, "payment_mode", f"Invalid payment mode: {row.payment_mode}", "Validity", "Payment mode must be one of the allowed values", "Critical")
        if row.order_id not in order_ids:
            add_issue(issues, "payments.csv", key, "order_id", "Payment references an unknown order", "Consistency", "Payment order_id must exist in orders", "Critical")
        if pd.isna(pd.to_datetime(row.payment_ts, errors="coerce")):
            add_issue(issues, "payments.csv", key, "payment_ts", "Timestamp cannot be parsed", "Validity", "payment_ts must be a valid timestamp", "Critical")
        if str(row.payment_status).strip().upper() == "SUCCESS" and row.order_id in set(orders.order_id):
            order_amount = pd.to_numeric(orders.loc[orders.order_id == row.order_id, "order_amount"], errors="coerce")
            paid_amount = pd.to_numeric(row.amount_paid, errors="coerce")
            if not order_amount.empty and pd.notna(order_amount.iloc[0]) and pd.notna(paid_amount) and abs(order_amount.iloc[0] - paid_amount) > 0.01:
                add_issue(issues, "payments.csv", key, "amount_paid", "SUCCESS payment does not equal order_amount", "Consistency", "A SUCCESS payment must equal its order amount", "Critical")

    # Validate customer contact formats and signup-date timeliness. PII remains
    # untouched here; masking belongs in the ETL/ELT transformation layers.
    for _, row in customers.iterrows():
        key = row.customer_id
        if not pd.Series([row.email]).str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$").iloc[0]:
            add_issue(issues, "customers.csv", key, "email", "Email does not match expected format", "Validity", "Email should match a basic email pattern", "Warning")
        if not str(row.phone).isdigit() or len(str(row.phone)) != 10:
            add_issue(issues, "customers.csv", key, "phone", "Phone is not exactly 10 digits", "Validity", "Phone should contain 10 digits", "Warning")
        if not str(row.pincode).isdigit() or len(str(row.pincode)) != 6:
            add_issue(issues, "customers.csv", key, "pincode", "Pincode is not exactly 6 digits", "Validity", "Pincode should contain 6 digits", "Warning")
        signup = pd.to_datetime(row.signup_date, errors="coerce")
        if pd.isna(signup):
            add_issue(issues, "customers.csv", key, "signup_date", "Signup date cannot be parsed", "Validity", "signup_date must be a valid date", "Critical")
        elif signup.normalize() > as_of_date:
            add_issue(issues, "customers.csv", key, "signup_date", "Future-dated signup date", "Timeliness", "Signup date must not be future dated", "Critical")

    # Validate product completeness, price relationships, and launch dates.
    for _, row in products.iterrows():
        key = row.product_id
        mrp, cost = pd.to_numeric(row.mrp, errors="coerce"), pd.to_numeric(row.cost_price, errors="coerce")
        if blank(row.category):
            add_issue(issues, "products.csv", key, "category", "Category is blank", "Completeness", "Product category is required", "Critical")
        if pd.isna(mrp) or mrp <= 0:
            add_issue(issues, "products.csv", key, "mrp", "MRP is missing or not positive", "Accuracy", "mrp must be > 0", "Critical")
        if pd.notna(mrp) and pd.notna(cost) and cost > mrp:
            add_issue(issues, "products.csv", key, "cost_price", "Cost price exceeds MRP", "Accuracy", "cost_price must be <= mrp", "Critical")
        if pd.isna(pd.to_datetime(row.launch_date, errors="coerce")):
            add_issue(issues, "products.csv", key, "launch_date", "Launch date cannot be parsed", "Validity", "launch_date must be a valid date", "Critical")

    # Validate store attributes needed for zone-level reporting.
    for _, row in stores.iterrows():
        if blank(row.zone):
            add_issue(issues, "stores.csv", row.store_id, "zone", "Store zone is blank", "Completeness", "Store zone is required for zone reporting", "Critical")
        if pd.isna(pd.to_datetime(row.open_date, errors="coerce")):
            add_issue(issues, "stores.csv", row.store_id, "open_date", "Open date cannot be parsed", "Validity", "open_date must be a valid date", "Critical")

    # Existing IDs in the increment feed are expected corrections. Log them as
    # warnings so the ELT pipeline can prove it performs an upsert rather than
    # creating duplicate facts.
    increment = frames["orders_2026-10-01_increment.csv"]
    for _, row in increment.iterrows():
        if row.order_id in order_ids:
            add_issue(issues, "orders_2026-10-01_increment.csv", row.order_id, "order_id", "Existing order key is an intentional correction/upsert candidate", "Consistency", "Increment rows must merge existing order_id values rather than append duplicates", "Warning")

    # Write the row-level issue register and the questions to clarify with the
    # client before implementing production quality rules.
    pd.DataFrame(issues).to_csv(OUTPUT_DIR / "data_issues_log.csv", index=False)
    pd.DataFrame({"question": [
        "Should negative quantity or an order_amount mismatch be treated as a rejected data-entry error, or can either represent a return/adjustment?",
        "When an order changes from DELIVERED to RETURNED, should September revenue be restated historically, or preserved as originally reported with a separate returns view?",
        "What retention period and role-based access policy should apply to raw customer PII, quarantine files, and masked reporting tables?",
    ]}).to_csv(OUTPUT_DIR / "client_questions.csv", index=False)
    print(f"Copied {len(FILES)} files to {DATA_DIR}")
    print(f"Profile rows: {len(profile)} -> {OUTPUT_DIR / 'profile_summary.csv'}")
    print(f"Data issues: {len(issues)} -> {OUTPUT_DIR / 'data_issues_log.csv'}")
    print(pd.DataFrame(issues).groupby(["file", "severity"]).size().to_string())


if __name__ == "__main__":
    main()
