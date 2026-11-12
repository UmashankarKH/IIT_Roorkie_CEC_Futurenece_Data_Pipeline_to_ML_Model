
"""Physical source-row and transformation reconciliation metrics."""

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"

SOURCE_FILES = [
    "customers.csv",
    "products.csv",
    "stores.csv",
    "orders.csv",
    "orders_2026-10-01_increment.csv",
    "payments.csv",
]


def get_source_reconciliation_metrics(
    source_dir: Path | None = None,
) -> dict[str, int]:
    """Count physical source rows, exact duplicates and superseded orders."""
    frames = {}
    rows_extracted = 0
    rows_deduplicated = 0
    input_dir = source_dir or SOURCE_DIR

    for filename in SOURCE_FILES:
        path = input_dir / filename
        if not path.is_file():
            raise FileNotFoundError(path)

        df = pd.read_csv(path, dtype="string", keep_default_na=False)
        frames[filename] = df

        rows_extracted += len(df)
        rows_deduplicated += len(df) - len(df.drop_duplicates())

    base = frames["orders.csv"]
    increment = frames["orders_2026-10-01_increment.csv"]

    base_ids = base.loc[
        base["order_id"].str.strip().ne(""),
        ["order_id", "order_status"],
    ].drop_duplicates()

    increment_ids = increment.loc[
        increment["order_id"].str.strip().ne(""),
        ["order_id", "order_status"],
    ].drop_duplicates()

    overlap = base_ids.merge(
        increment_ids,
        on="order_id",
        how="inner",
        suffixes=("_base", "_increment"),
    )

    rows_superseded = int(
        (
            overlap["order_status_base"]
            != overlap["order_status_increment"]
        ).sum()
    )

    return {
        "rows_extracted": rows_extracted,
        "rows_deduplicated": rows_deduplicated,
        "rows_superseded": rows_superseded,
    }
