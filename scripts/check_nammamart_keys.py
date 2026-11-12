import csv
from pathlib import Path
from collections import Counter

DATA_DIR = Path("include/data/nammamart")


def load_csv(filename):
    with (DATA_DIR / filename).open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        return list(csv.DictReader(f))


def values(rows, column):
    return {
        row[column].strip()
        for row in rows
        if row.get(column) and row[column].strip()
    }


def check_key(label, rows, column):
    counts = Counter(
        row[column].strip()
        for row in rows
        if row.get(column) and row[column].strip()
    )
    missing = sum(
        not row.get(column) or not row[column].strip()
        for row in rows
    )
    duplicates = sum(count - 1 for count in counts.values() if count > 1)

    print(
        f"{label}: rows={len(rows)}, distinct_keys={len(counts)}, "
        f"missing_keys={missing}, duplicate_key_excess={duplicates}"
    )


customers = load_csv("customers.csv")
orders = load_csv("orders.csv")
increment = load_csv("orders_2026-10-01_increment.csv")
payments = load_csv("payments.csv")
products = load_csv("products.csv")
stores = load_csv("stores.csv")

print("=== Candidate primary keys ===")
check_key("customers.customer_id", customers, "customer_id")
check_key("orders.order_id", orders, "order_id")
check_key("increment.order_id", increment, "order_id")
check_key("payments.payment_id", payments, "payment_id")
check_key("products.product_id", products, "product_id")
check_key("stores.store_id", stores, "store_id")

print("\n=== Foreign-key integrity ===")
customer_ids = values(customers, "customer_id")
product_ids = values(products, "product_id")
store_ids = values(stores, "store_id")
order_ids = values(orders, "order_id") | values(increment, "order_id")

for column, known, label in [
    ("customer_id", customer_ids, "orders -> customers"),
    ("product_id", product_ids, "orders -> products"),
    ("store_id", store_ids, "orders -> stores"),
]:
    missing = sum(
        not row.get(column) or not row[column].strip()
        for row in orders + increment
    )
    orphaned = sum(
        bool(row.get(column) and row[column].strip())
        and row[column].strip() not in known
        for row in orders + increment
    )
    print(f"{label}: missing={missing}, orphaned={orphaned}")

orphan_payments = sum(
    bool(row.get("order_id") and row["order_id"].strip())
    and row["order_id"].strip() not in order_ids
    for row in payments
)
missing_payment_order_ids = sum(
    not row.get("order_id") or not row["order_id"].strip()
    for row in payments
)
print(
    "payments -> orders: "
    f"missing_order_id={missing_payment_order_ids}, "
    f"orphaned_order_ids={orphan_payments}"
)

print("\n=== Incremental overlap ===")
base_ids = values(orders, "order_id")
increment_ids = values(increment, "order_id")
print(f"Base distinct order IDs: {len(base_ids)}")
print(f"Increment distinct order IDs: {len(increment_ids)}")
print(f"IDs present in both: {len(base_ids & increment_ids)}")
print(f"New IDs in increment: {len(increment_ids - base_ids)}")
