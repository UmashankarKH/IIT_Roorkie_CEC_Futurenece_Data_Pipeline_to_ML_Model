import csv
from pathlib import Path

DATA_DIR = Path("include/data/nammamart")

for path in sorted(DATA_DIR.glob("*.csv")):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        rows = list(reader)

    print(f"\n{'=' * 60}")
    print(f"File: {path.name}")
    print(f"Rows: {len(rows)}")
    print(f"Columns ({len(columns)}): {columns}")

    print("Missing values:")
    for column in columns:
        missing = sum(
            row.get(column) is None or not row.get(column, "").strip()
            for row in rows
        )
        if missing:
            print(f"  {column}: {missing}")

    unique_rows = {
        tuple(row.get(column, "") for column in columns)
        for row in rows
    }
    print(f"Exact duplicate rows: {len(rows) - len(unique_rows)}")
