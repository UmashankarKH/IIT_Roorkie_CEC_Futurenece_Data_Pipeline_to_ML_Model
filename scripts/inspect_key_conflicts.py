import csv
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path("include/data/nammamart")


def load(filename):
    with (DATA_DIR / filename).open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        return list(csv.DictReader(f))


def inspect(filename, key):
    rows = load(filename)
    grouped = defaultdict(list)

    for row in rows:
        value = (row.get(key) or "").strip()
        if value:
            grouped[value].append(row)

    duplicate_groups = [
        group for group in grouped.values() if len(group) > 1
    ]
    exact_groups = sum(
        len({tuple(sorted(row.items())) for row in group}) == 1
        for group in duplicate_groups
    )
    conflicting_groups = len(duplicate_groups) - exact_groups

    print(f"\n{filename} | key={key}")
    print(f"Duplicate-key groups: {len(duplicate_groups)}")
    print(f"Groups with identical rows: {exact_groups}")
    print(f"Groups with conflicting rows: {conflicting_groups}")

    if conflicting_groups:
        differing_columns = set()
        for group in duplicate_groups:
            if len({tuple(sorted(row.items())) for row in group}) > 1:
                for column in row_columns(group):
                    if len({row.get(column) for row in group}) > 1:
                        differing_columns.add(column)

        print("Columns that differ in conflicting groups:")
        print(sorted(differing_columns))


def row_columns(group):
    return group[0].keys()


inspect("customers.csv", "customer_id")
inspect("products.csv", "product_id")
inspect("orders.csv", "order_id")
inspect("payments.csv", "payment_id")
