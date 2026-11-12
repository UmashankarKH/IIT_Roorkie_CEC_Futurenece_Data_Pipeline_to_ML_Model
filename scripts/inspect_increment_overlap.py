import csv
from pathlib import Path

DATA_DIR = Path("include/data/nammamart")


def load(filename):
    with (DATA_DIR / filename).open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        return list(csv.DictReader(f))


base = load("orders.csv")
increment = load("orders_2026-10-01_increment.csv")

base_by_id = {}
for row in base:
    base_by_id.setdefault(row["order_id"].strip(), []).append(row)

overlap = 0
identical = 0
changed = 0
base_duplicate_keys = 0
different_columns = set()

for row in increment:
    order_id = row["order_id"].strip()
    matches = base_by_id.get(order_id, [])

    if not matches:
        continue

    overlap += 1

    # Compare against the unique base row only.
    if len(matches) != 1:
        base_duplicate_keys += 1
        continue

    previous = matches[0]
    diffs = {
        column
        for column in row
        if row.get(column, "") != previous.get(column, "")
    }

    if not diffs:
        identical += 1
    else:
        changed += 1
        different_columns.update(diffs)

print(f"Overlapping incremental order IDs: {overlap}")
print(f"Identical to the unique base row: {identical}")
print(f"Different from the unique base row: {changed}")
print(f"Overlaps with duplicate base keys: {base_duplicate_keys}")
print(f"Columns changed across differing rows: {sorted(different_columns)}")
