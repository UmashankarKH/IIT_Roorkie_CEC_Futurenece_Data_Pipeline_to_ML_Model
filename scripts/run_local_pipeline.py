from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import duckdb
import pandas as pd
from matplotlib import pyplot as plt

from include.nammamart_etl_utils import build_fact_orders, validate_dataframe

DATA_DIR = ROOT / "include" / "data" / "nammamart"
OUTPUT_DIR = ROOT / "output"
WAREHOUSE_DB = ROOT / "include" / "warehouse_local.duckdb"


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    files = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv"]
    valid = {}
    failed = {}
    totals = {}

    for file_name in files:
        df = pd.read_csv(DATA_DIR / file_name)
        clean_df, failed_df = validate_dataframe(df, file_name)
        valid[file_name] = clean_df
        failed[file_name] = failed_df
        totals[file_name] = {"valid_rows": len(clean_df), "failed_rows": len(failed_df)}
        print(f"{file_name}: valid={len(clean_df)} failed={len(failed_df)}")

    fact = build_fact_orders(
        valid["orders.csv"],
        valid["customers.csv"],
        valid["products.csv"],
        valid["stores.csv"],
        "local_run",
    )
    print(f"fact_rows={len(fact)}")
    print(fact.head(3).to_string(index=False))

    con = duckdb.connect(str(WAREHOUSE_DB))
    con.register("fact", fact)
    con.execute("CREATE TABLE IF NOT EXISTS local_fact_orders AS SELECT * FROM fact LIMIT 0")
    con.execute("INSERT INTO local_fact_orders SELECT * FROM fact")
    zone_summary = con.execute(
        "SELECT zone, ROUND(SUM(net_revenue), 2) AS net_revenue FROM local_fact_orders GROUP BY zone ORDER BY zone"
    ).fetchdf()
    zone_summary["zone"] = zone_summary["zone"].fillna("Unknown")
    zone_summary["zone"] = zone_summary["zone"].astype(str)
    print("\nZone revenue summary:")
    print(zone_summary.to_string(index=False))
    con.close()

    summary_df = pd.DataFrame(
        [{"file_name": k, "valid_rows": v["valid_rows"], "failed_rows": v["failed_rows"]} for k, v in totals.items()]
    )
    summary_df.to_csv(OUTPUT_DIR / "pipeline_validation_summary.csv", index=False)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(zone_summary["zone"], zone_summary["net_revenue"], color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"])
    ax.set_title("NammaMart net revenue by zone")
    ax.set_xlabel("Zone")
    ax.set_ylabel("Net revenue")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pipeline_summary.png", dpi=150)
    print(f"\nPipeline summary saved to: {OUTPUT_DIR / 'pipeline_summary.png'}")


if __name__ == "__main__":
    main()
