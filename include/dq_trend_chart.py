"""
include/dq_trend_chart.py
-------------------------
Generates a data-quality trend chart from dq_results in both DuckDB warehouses.

Reads the latest run per check and plots rows_failed over time per DAG,
showing how data quality has trended across pipeline runs.

Usage:
    python include/dq_trend_chart.py
Output:
    include/dq_trend.png
"""

import os
import duckdb
import pandas as pd

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ETL_DB = PROJECT_ROOT / "include" / "warehouse_rudin.duckdb"
ELT_DB = PROJECT_ROOT / "include" / "warehouse_rudin_elt.duckdb"
OUTPUT_PNG = PROJECT_ROOT / "include" / "dq_trend.png"


def _fetch_trend(db_path: str, dag_id: str) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute("""
            SELECT checked_at::TIMESTAMP AS checked_at,
                   check_name,
                   severity,
                   rows_checked,
                   rows_failed,
                   status
            FROM dq_results
            WHERE dag_id = ?
            ORDER BY checked_at ASC
        """, [dag_id]).fetchdf()
    finally:
        con.close()
    return df


def main() -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[DQ Trend Chart] matplotlib not installed; skipping chart generation.")
        print("  Add `matplotlib` to requirements.txt and rerun `astro dev restart` to enable.")
        return

    e_trend = _fetch_trend(str(ETL_DB), "nammamart_etl_rudin")
    l_trend = _fetch_trend(str(ELT_DB), "nammamart_elt_rudin")
    trend = pd.concat([e_trend.assign(pipeline="ETL"), l_trend.assign(pipeline="ELT")])

    if trend.empty:
        print("[DQ Trend Chart] No dq_results found; nothing to plot.")
        return

    trend["checked_at"] = pd.to_datetime(trend["checked_at"])

    fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
    fig.suptitle("NammaMart Data-Quality Trend (rows_failed per check per run)", fontsize=12)

    for ax, pipeline in zip(axes, ["ETL", "ELT"]):
        sub = trend[trend["pipeline"] == pipeline]
        if sub.empty:
            ax.set_title(f"{pipeline}: no data")
            continue
        for check_name, group in sub.groupby("check_name"):
            ax.plot(group["checked_at"], group["rows_failed"],
                    marker="o", label=check_name)
        ax.set_ylabel("Rows failed")
        ax.set_title(f"{pipeline} pipeline")
        ax.legend(fontsize=7, loc="upper left")
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Run time (UTC)")
    fig.autofmt_xdate()
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.savefig(OUTPUT_PNG, dpi=150)
    print(f"[DQ Trend Chart] wrote {OUTPUT_PNG}")


if __name__ == "__main__":
    main()
